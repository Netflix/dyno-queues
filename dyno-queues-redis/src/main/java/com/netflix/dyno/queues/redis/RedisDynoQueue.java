/**
 * Copyright 2016 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.dyno.queues.redis;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.JedisCommands;
import redis.clients.jedis.Tuple;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.annotations.VisibleForTesting;
import com.netflix.dyno.connectionpool.exception.DynoException;
import com.netflix.dyno.queues.DynoQueue;
import com.netflix.dyno.queues.Message;
import com.netflix.servo.monitor.Stopwatch;

/**
 * 
 * @author Viren
 *
 */
public class RedisDynoQueue implements DynoQueue {

	private final Logger logger = LoggerFactory.getLogger(RedisDynoQueue.class);

	private String queueName;

	private List<String> allShards;

	private String shardName;

	private String redisKeyPrefix;

	private String messageStoreKey;
	
	private String myQueueShard;

	private int unackTime = 60;
	
	private int unackScheduleInMS = 60_000;

	private QueueMonitor monitor;

	private ObjectMapper om;
	
	private ExecutorService executorService;

	private JedisCommands quorumConn;
	
	private JedisCommands nonQuorumConn;
	
	private LinkedBlockingQueue<String> prefetchedIds;
	
	private int prefetchCount = 10000;
	
	private int retryCount = 2;

	public RedisDynoQueue(String redisKeyPrefix, String queueName, Set<String> allShards, String shardName, ExecutorService dynoCallExecutor){
		this.redisKeyPrefix = redisKeyPrefix;
		this.queueName = queueName;
		this.allShards = allShards.stream().collect(Collectors.toList());
		this.shardName = shardName;
		this.messageStoreKey = redisKeyPrefix + ".MESSAGE." + queueName;
		this.myQueueShard = getQueueShardKey(queueName, shardName);
		
		ObjectMapper om = new ObjectMapper();
		om.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		om.configure(DeserializationFeature.FAIL_ON_IGNORED_PROPERTIES, false);
		om.configure(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, false);
		om.setSerializationInclusion(Include.NON_NULL);
		om.setSerializationInclusion(Include.NON_EMPTY);
		om.disable(SerializationFeature.INDENT_OUTPUT);

		this.om = om;
		this.monitor = new QueueMonitor(queueName, shardName);
		this.prefetchedIds = new LinkedBlockingQueue<>();
		this.executorService = dynoCallExecutor;
		
		Executors.newScheduledThreadPool(1).scheduleAtFixedRate(() -> processUnacks(), unackScheduleInMS, unackScheduleInMS, TimeUnit.MILLISECONDS);
		Executors.newScheduledThreadPool(1).scheduleAtFixedRate(() -> prefetchIds(), 0, 20, TimeUnit.MILLISECONDS);
		
		logger.info(RedisDynoQueue.class.getName() + " is ready to serve " + queueName);
		
	}
	
	public RedisDynoQueue withQuorumConn(JedisCommands quorumConn){
		this.quorumConn = quorumConn;
		return this;
	}
	
	public RedisDynoQueue withNonQuorumConn(JedisCommands nonQuorumConn){
		this.nonQuorumConn = nonQuorumConn;
		return this;
	}
	
	public RedisDynoQueue withUnackTime(int unackTime){
		this.unackTime = unackTime;
		return this;
	}
	
	public RedisDynoQueue withUnackSchedulerTime(int unackScheduleInMS){
		this.unackScheduleInMS = unackScheduleInMS;
		return this;
	}

	@Override
	public String getName() {
		return queueName;
	}

	@Override
	public int getUnackTime() {
		return unackTime;
	}

	@Override
	public List<String> push(final List<Message> messages) {
		
		Stopwatch sw = monitor.start(monitor.push, messages.size());

		try {

			execute(() -> {
				for (Message message : messages) {
					String json = om.writeValueAsString(message);
					quorumConn.hset(messageStoreKey, message.getId(), json);
					double priority = message.getPriority() / 100;
					double score = Long.valueOf(System.currentTimeMillis() + message.getTimeout()).doubleValue() + priority;
					String shard = getNextShard();
					String queueShard = getQueueShardKey(queueName, shard);
					quorumConn.zadd(queueShard, score, message.getId());
				}
				return messages;
			});
			
			return messages.stream().map(msg -> msg.getId()).collect(Collectors.toList());

		} finally {
			sw.stop();
		}
	}

	@Override
	public List<Message> peek(final int messageCount) {

		Stopwatch sw = monitor.peek.start();

		try {
			
			Set<String> ids = peekIds(0, messageCount);
			if (ids == null) {
				return Collections.emptyList();
			}
			
			List<Message> msgs = execute(() -> {
				List<Message> messages = new LinkedList<Message>();
				for (String id : ids) {
					String json = nonQuorumConn.hget(messageStoreKey, id);
					Message message = om.readValue(json, Message.class);
					messages.add(message);
				}
				return messages;
			});
			
			return msgs;

		} finally {
			sw.stop();
		}
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public List<Message> pop(int messageCount, int wait, TimeUnit unit) {

		if (messageCount < 1) {
			return Collections.emptyList();
		}

		Stopwatch sw = monitor.start(monitor.pop, messageCount);

		try {

			return (List<Message>) execute(() -> {

				Set<String> ids = new HashSet<>();
				if (logger.isDebugEnabled()) {
					logger.debug("prefetchedIds.size=" + prefetchedIds.size());
				}
				if (prefetchedIds.isEmpty()) {
					prefetch.set(true);
					String id = prefetchedIds.poll(wait, unit);
					if (id != null) {
						ids.add(id);
					}
				}
				prefetchedIds.drainTo(ids, messageCount);
				if (ids.size() < messageCount) {
					prefetch.set(true);
				}

				if (ids.isEmpty()) {
					return Collections.emptyList();
				}

				return _pop(ids, messageCount);

			});

		} finally {
			sw.stop();
		}

	}
	
	private AtomicBoolean prefetch = new AtomicBoolean(false);
	
	private void prefetchIds() {

		if (!prefetch.get()) {
			return;
		}

		Stopwatch sw = monitor.start(monitor.prefetch, prefetchCount);
		try {
			
			execute(() -> {
				Set<String> ids = peekIds(0, prefetchCount);
				prefetchedIds.addAll(ids);
				prefetch.set(false);
				return null;
			});
			
		} finally {

			sw.stop();
		}

	}
	
	private List<Message> _pop(Set<String> ids, int messageCount) throws Exception {
		
		double unackScore = Long.valueOf(System.currentTimeMillis() + unackTime).doubleValue();
		String unackQueueName = getUnackKey(queueName, shardName);

		List<Message> popped = new LinkedList<>();
		
		for (String msgId : ids) {
			
			long added = quorumConn.zadd(unackQueueName, unackScore, msgId);
			if(added == 0){
				if (logger.isDebugEnabled()) {
					logger.debug("cannot add to the unack shard " + msgId);
				}
				monitor.misses.increment();
				continue;
			}
			
			long removed = quorumConn.zrem(myQueueShard, msgId);
			if (removed == 0) {
				if (logger.isDebugEnabled()) {
					logger.debug("cannot remove from the queue shard " + msgId);
				}
				monitor.misses.increment();
				continue;
			}

			String json = quorumConn.hget(messageStoreKey, msgId);
			if(json == null){
				if (logger.isDebugEnabled()) {
					logger.debug("Cannot get the message payload " + msgId);
				}
				monitor.misses.increment();
				continue;
			}
			
			Message msg = om.readValue(json, Message.class);
			popped.add(msg);
			
			if(popped.size() == messageCount){
				return popped;
			}
			
		
		}

		return popped;

	
	}

	@Override
	public boolean ack(String messageId) {

		Stopwatch sw = monitor.ack.start();

		try {
			
			return execute(() -> {

				for (String shard : allShards) {
					String unackShardKey = getUnackKey(queueName, shard);
					Long removed = quorumConn.zrem(unackShardKey, messageId);
					if (removed > 0) {
						quorumConn.hdel(messageStoreKey, messageId);
						return true;
					}
				}
				return false;
			});

		} finally {
			sw.stop();
		}
	}

	@Override
	public boolean remove(String messageId) {

		Stopwatch sw = monitor.remove.start();

		try {
			
			return execute(() -> {
				
				for (String shard : allShards) {

					String unackShardKey = getUnackKey(queueName, shard);
					quorumConn.zrem(unackShardKey, messageId);

					String queueShardKey = getQueueShardKey(queueName, shard);
					Long removed = quorumConn.zrem(queueShardKey, messageId);
					Long msgRemoved = quorumConn.hdel(messageStoreKey, messageId);

					if (removed > 0 && msgRemoved > 0) {
						return true;
					}
				}

				return false;
				
			});
			
		} finally {
			sw.stop();
		}
	}
	
	@Override
	public Message get(String messageId) {

		Stopwatch sw = monitor.get.start();

		try {
			
			return execute(() -> {
				String json = quorumConn.hget(messageStoreKey, messageId);
				if(json == null){
					if (logger.isDebugEnabled()) {
						logger.debug("Cannot get the message payload " + messageId);						
					}
					return null;
				}
				
				Message msg = om.readValue(json, Message.class);
				return msg;
			});
			
		} finally {
			sw.stop();
		}
	}

	@Override
	public long size() {

		Stopwatch sw = monitor.size.start();

		try {
			
			return execute(() -> {
				long size = 0;
				for (String shard : allShards) {
					size += nonQuorumConn.zcard(getQueueShardKey(queueName, shard));
				}
				return size;
			});

		} finally {
			sw.stop();
		}
	}
	
	@Override
	public Map<String, Map<String, Long>> shardSizes() {

		Stopwatch sw = monitor.size.start();
		Map<String, Map<String, Long>> shardSizes = new HashMap<>();
		try {

			return execute(() -> {
				for (String shard : allShards) {
					long size = nonQuorumConn.zcard(getQueueShardKey(queueName, shard));
					long uacked = nonQuorumConn.zcard(getUnackKey(queueName, shard));
					Map<String, Long> shardDetails = new HashMap<>();
					shardDetails.put("size", size);
					shardDetails.put("uacked", uacked);
					shardSizes.put(shard, shardDetails);
				}
				return shardSizes;
			});

		} finally {
			sw.stop();
		}
	}

	@Override
	public void clear() {
		execute(() -> {
			for (String shard : allShards) {
				String queueShard = getQueueShardKey(queueName, shard);
				String unackShard = getUnackKey(queueName, shard);
				quorumConn.del(queueShard);
				quorumConn.del(unackShard);				
			}
			quorumConn.del(messageStoreKey);
			return null;
		});

	}

	private Set<String> peekIds(int offset, int count) {
		
		return execute(() -> {
			double now = Long.valueOf(System.currentTimeMillis() + 1).doubleValue();
			Set<String> scanned = quorumConn.zrangeByScore(myQueueShard, 0, now, offset, count);
			return scanned;
		});
		
	}

	@VisibleForTesting
	void processUnacks() {

		Stopwatch sw = monitor.processUnack.start();
		try {

			long queueDepth = size();
			monitor.queueDepth.record(queueDepth);
			
			execute(() -> {

				int batchSize = 1_000;
				String unackQueueName = getUnackKey(queueName, shardName);

				double now = Long.valueOf(System.currentTimeMillis()).doubleValue();

				Set<Tuple> unacks = quorumConn.zrangeByScoreWithScores(unackQueueName, 0, now, 0, batchSize);

				if (unacks.size() > 0) {
					logger.debug("Adding " + unacks.size() + " messages back to the queue for " + queueName);
				}

				for (Tuple unack : unacks) {

					double score = unack.getScore();
					String member = unack.getElement();

					String payload = quorumConn.hget(messageStoreKey, member);
					if (payload == null) {
						quorumConn.zrem(unackQueueName, member);
						continue;
					}

					quorumConn.zadd(myQueueShard, score, member);
					quorumConn.zrem(unackQueueName, member);
				}

				prefetchIds();
				return null;
			});

		} finally {
			sw.stop();
		}

	}

	private AtomicInteger nextShardIndex = new AtomicInteger(0);

	private String getNextShard() {
		int indx = nextShardIndex.incrementAndGet();
		if (indx >= allShards.size()) {
			nextShardIndex.set(0);
			indx = 0;
		}
		String s = allShards.get(indx);
		return s;
	}

	private String getQueueShardKey(String queueName, String shard) {
		return redisKeyPrefix + ".QUEUE." + queueName + "." + shard;
	}

	private String getUnackKey(String queueName, String shard) {
		return redisKeyPrefix + ".UNACK." + queueName + "." + shard;
	}
	
	private <R> R execute(Callable<R> r) {
		return executeWithRetry(executorService, r, 0);
	}
	
	private <R> R executeWithRetry(ExecutorService es, Callable<R> r, int retryCount) {

		try {

			return es.submit(r).get(10, TimeUnit.SECONDS);

		} catch (ExecutionException e) {
			
			if (e.getCause() instanceof DynoException) {
				if (retryCount < this.retryCount) {
					//return executeWithRetry(es, r, ++retryCount);
				}
			}
			throw new RuntimeException(e.getCause());
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

}
