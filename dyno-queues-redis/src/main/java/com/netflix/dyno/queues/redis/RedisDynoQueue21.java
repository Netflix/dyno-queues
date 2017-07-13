/**
 * 
 */
package com.netflix.dyno.queues.redis;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;
import com.netflix.dyno.queues.DynoQueue;
import com.netflix.dyno.queues.Message;

/**
 * @author Viren
 *
 */
public class RedisDynoQueue21 implements DynoQueue {

	private List<String> shards;

	private String name;

	private Map<String, RedisDynoQueue2> queues = new HashMap<>();
	
	private RedisDynoQueue2 me;

	public RedisDynoQueue21(String queueName, String shardName, Map<String, RedisDynoQueue2> queues) {
		this.name = queueName;
		this.queues = queues;
		this.me = queues.get(shardName);
		if(me == null) {
			throw new IllegalArgumentException("List of shards supplied (" + queues.keySet() + ") does not contain current shard name: " + shardName);
		}
		this.shards = queues.keySet().stream().collect(Collectors.toList());
	}

	@Override
	public String getName() {
		return name;
	}

	@Override
	public int getUnackTime() {
		return me.getUnackTime();
	}

	@Override
	public List<String> push(List<Message> messages) {
		List<List<Message>> p = Lists.partition(messages, shards.size());
		List<String> ids = new LinkedList<>();
		for(List<Message> pm : p) {
			ids.addAll(queues.get(getNextShard()).push(pm));
		}
		return ids;
	}

	@Override
	public List<Message> pop(int messageCount, int wait, TimeUnit unit) {
		return me.pop(messageCount, wait, unit);
	}

	@Override
	public List<Message> peek(int messageCount) {
		return me.peek(messageCount);
	}

	@Override
	public boolean ack(String messageId) {
		for(DynoQueue q : queues.values()) {
			if(q.ack(messageId)) {
				return true;
			}
		}
		return false;
	}

	@Override
	public void ack(List<Message> messages) {
		Map<String, List<Message>> byShard = messages.stream().collect(Collectors.groupingBy(Message::getShard));
		for(Entry<String, List<Message>> e: byShard.entrySet()) {
			queues.get(e.getKey()).ack(e.getValue());
		}
	}

	@Override
	public boolean setUnackTimeout(String messageId, long timeout) {
		for(DynoQueue q : queues.values()) {
			if(q.setUnackTimeout(messageId, timeout)) {
				return true;
			}
		}
		return false;
	}

	@Override
	public boolean setTimeout(String messageId, long timeout) {
		for(DynoQueue q : queues.values()) {
			if(q.setTimeout(messageId, timeout)) {
				return true;
			}
		}
		return false;
	}

	@Override
	public boolean remove(String messageId) {
		for(DynoQueue q : queues.values()) {
			if(q.remove(messageId)) {
				return true;
			}
		}
		return false;
	}

	@Override
	public Message get(String messageId) {
		for(DynoQueue q : queues.values()) {
			Message msg = q.get(messageId);
			if(msg != null) {
				return msg;
			}
		}
		return null;
	}

	@Override
	public long size() {
		long size = 0;
		for(DynoQueue q : queues.values()) {
			size += q.size();
		}
		return size;
	}

	@Override
	public Map<String, Map<String, Long>> shardSizes() {
		Map<String, Map<String, Long>> sizes = new HashMap<>();
		for(Entry<String, RedisDynoQueue2> e : queues.entrySet()) {
			sizes.put(e.getKey(), e.getValue().shardSizes().get(e.getKey()));
		}
		return sizes;
	}

	@Override
	public void clear() {
		for(DynoQueue q : queues.values()) {
			q.clear();
		}
		
	}

	@Override
	public void close() throws IOException {
		for(RedisDynoQueue2 queue : queues.values()) {
			queue.close();
		}
	}
	
	public void processUnacks() {
		for(RedisDynoQueue2 queue : queues.values()) {
			queue.processUnacks();
		}
	}
	
	private AtomicInteger nextShardIndex = new AtomicInteger(0);

	private String getNextShard() {
		int indx = nextShardIndex.incrementAndGet();
		if (indx >= shards.size()) {
			nextShardIndex.set(0);
			indx = 0;
		}
		String s = shards.get(indx);
		return s;
	}

	
}
