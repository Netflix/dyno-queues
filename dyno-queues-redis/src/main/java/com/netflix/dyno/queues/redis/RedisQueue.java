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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.Uninterruptibles;
import com.netflix.dyno.queues.DynoQueue;
import com.netflix.dyno.queues.Message;
import com.netflix.servo.monitor.Stopwatch;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.Tuple;
import redis.clients.jedis.params.sortedset.ZAddParams;

/**
 *
 * @author Viren
 *
 */
public class RedisQueue implements DynoQueue {

	private final Logger logger = LoggerFactory.getLogger(RedisQueue.class);

	private String queueName;

	private String shardName;

	private String messageStoreKey;

	private String myQueueShard;
	
	private String unackShardKey;

	private int unackTime = 60;

	private QueueMonitor monitor;

	private ObjectMapper om;

	private JedisPool connPool;
	
	private JedisPool nonQuorumPool;

	private ConcurrentLinkedQueue<String> prefetchedIds;

	private ScheduledExecutorService schedulerForUnacksProcessing;

	private ScheduledExecutorService schedulerForPrefetchProcessing;

	public RedisQueue(String redisKeyPrefix, String queueName, String shardName, int unackTime, JedisPool pool) {
		this(redisKeyPrefix, queueName, shardName, unackTime, unackTime, pool);
	}

	public RedisQueue(String redisKeyPrefix, String queueName, String shardName, int unackScheduleInMS, int unackTime, JedisPool pool) {
		this.queueName = queueName;
		this.shardName = shardName;
		this.messageStoreKey = redisKeyPrefix + ".MESSAGE." + queueName;
		this.myQueueShard = redisKeyPrefix + ".QUEUE." + queueName + "." + shardName;
		this.unackShardKey = redisKeyPrefix + ".UNACK." + queueName + "." + shardName;
		this.unackTime = unackTime;
		this.connPool = pool;
		this.nonQuorumPool = pool;
		
		ObjectMapper om = new ObjectMapper();
		om.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		om.configure(DeserializationFeature.FAIL_ON_IGNORED_PROPERTIES, false);
		om.configure(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, false);
		om.setSerializationInclusion(Include.NON_NULL);
		om.setSerializationInclusion(Include.NON_EMPTY);
		om.disable(SerializationFeature.INDENT_OUTPUT);

		this.om = om;
		this.monitor = new QueueMonitor(queueName, shardName);
		this.prefetchedIds = new ConcurrentLinkedQueue<>();

		schedulerForUnacksProcessing = Executors.newScheduledThreadPool(1);
		schedulerForPrefetchProcessing = Executors.newScheduledThreadPool(1);

		schedulerForUnacksProcessing.scheduleAtFixedRate(() -> processUnacks(), unackScheduleInMS, unackScheduleInMS, TimeUnit.MILLISECONDS);

		logger.info(RedisQueue.class.getName() + " is ready to serve " + queueName);

	}
	
	/**
	 * 
	 * @param nonQuorumPool When using a cluster like Dynomite, which relies on the quorum reads, supply a separate non-quorum read connection for ops like size etc.
	 */
	public void setNonQuorumPool(JedisPool nonQuorumPool) {
		this.nonQuorumPool = nonQuorumPool;
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
		Jedis conn = connPool.getResource();
		try {

			Pipeline pipe = conn.pipelined();

			for (Message message : messages) {
				String json = om.writeValueAsString(message);
				pipe.hset(messageStoreKey, message.getId(), json);
				double priority = message.getPriority() / 100.0;
				double score = Long.valueOf(System.currentTimeMillis() + message.getTimeout()).doubleValue() + priority;
				pipe.zadd(myQueueShard, score, message.getId());
			}
			pipe.sync();

			return messages.stream().map(msg -> msg.getId()).collect(Collectors.toList());

		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		} finally {
			conn.close();
			sw.stop();
		}
	}

	@Override
	public List<Message> peek(final int messageCount) {

		Stopwatch sw = monitor.peek.start();
		Jedis jedis = connPool.getResource();

		try {

			Set<String> ids = peekIds(0, messageCount);
			if (ids == null) {
				return Collections.emptyList();
			}

			List<Message> messages = new LinkedList<Message>();
			for (String id : ids) {
				String json = jedis.hget(messageStoreKey, id);
				Message message = om.readValue(json, Message.class);
				messages.add(message);
			}
			return messages;

		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			jedis.close();
			sw.stop();
		}
	}

	@Override
	public List<Message> pop(int messageCount, int wait, TimeUnit unit) {

		if (messageCount < 1) {
			return Collections.emptyList();
		}

		Stopwatch sw = monitor.start(monitor.pop, messageCount);

		try {
			long start = System.currentTimeMillis();
			long waitFor = unit.toMillis(wait);
			prefetch.addAndGet(messageCount);
			prefetchIds();
			while (prefetchedIds.size() < messageCount && ((System.currentTimeMillis() - start) < waitFor)) {
				Uninterruptibles.sleepUninterruptibly(200, TimeUnit.MILLISECONDS);
				prefetchIds();
			}

			List<Message> popped = _pop(messageCount);
			return popped;

		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			sw.stop();
		}

	}

	@VisibleForTesting
	AtomicInteger prefetch = new AtomicInteger(0);

	private void prefetchIds() {

		if (prefetch.get() < 1) {
			return;
		}

		int prefetchCount = prefetch.get();
		Stopwatch sw = monitor.start(monitor.prefetch, prefetchCount);
		try {

			Set<String> ids = peekIds(0, prefetchCount);
			prefetchedIds.addAll(ids);
			prefetch.addAndGet((-1 * ids.size()));
			if (prefetch.get() < 0 || ids.isEmpty()) {
				prefetch.set(0);
			}
		} finally {
			sw.stop();
		}

	}

	private List<Message> _pop(int messageCount) throws Exception {

		double unackScore = Long.valueOf(System.currentTimeMillis() + unackTime).doubleValue();

		List<Message> popped = new LinkedList<>();
		ZAddParams zParams = ZAddParams.zAddParams().nx();

		Jedis jedis = connPool.getResource();
		try {
			
			List<String> batch = new LinkedList<>();			
			Pipeline pipe = jedis.pipelined();
			List<Response<Long>> responses = new ArrayList<>(messageCount);
			for (int i = 0; i < messageCount; i++) {
				//String msgId = batch.get(i);
				String msgId = prefetchedIds.poll();
				if(msgId == null) {
					break;
				}
				batch.add(msgId);
				responses.add(pipe.zadd(unackShardKey, unackScore, msgId, zParams));
			}
			pipe.sync();
			int count = batch.size();

			List<Response<Long>> zremRes = new ArrayList<>(count);
			List<String> zremIds = new ArrayList<>(count);
			for (int i = 0; i < count; i++) {
				long added = responses.get(i).get();
				if (added == 0) {
					monitor.misses.increment();
					continue;
				}
				zremIds.add(batch.get(i));
				zremRes.add(pipe.zrem(myQueueShard, batch.get(i)));
			}
			pipe.sync();

			List<Response<String>> getRes = new ArrayList<>(count);
			for (int i = 0; i < zremRes.size(); i++) {
				long removed = zremRes.get(i).get();
				if (removed == 0) {
					monitor.misses.increment();
					continue;
				}
				getRes.add(pipe.hget(messageStoreKey, zremIds.get(i)));
			}
			pipe.sync();

			for (int i = 0; i < getRes.size(); i++) {
				String json = getRes.get(i).get();
				if (json == null) {
					monitor.misses.increment();
					continue;
				}
				Message msg = om.readValue(json, Message.class);
				msg.setShard(shardName);
				popped.add(msg);
			}
			return popped;
		} finally {
			jedis.close();
		}
	}

	@Override
	public boolean ack(String messageId) {

		Stopwatch sw = monitor.ack.start();
		Jedis jedis = connPool.getResource();

		try {

			Long removed = jedis.zrem(unackShardKey, messageId);
			if (removed > 0) {
				jedis.hdel(messageStoreKey, messageId);
				return true;
			}
		
			return false;

		} finally {
			jedis.close();
			sw.stop();
		}
	}
	
	@Override
	public void ack(List<Message> messages) {

		Stopwatch sw = monitor.ack.start();
		Jedis jedis = connPool.getResource();
		Pipeline pipe = jedis.pipelined();
		List<Response<Long>> responses = new LinkedList<>();
		try {
			for(Message msg : messages) {
				responses.add(pipe.zrem(unackShardKey, msg.getId()));
			}
			pipe.sync();
			
			List<Response<Long>> dels = new LinkedList<>();
			for(int i = 0; i < messages.size(); i++) {
				Long removed = responses.get(i).get();
				if (removed > 0) {
					dels.add(pipe.hdel(messageStoreKey, messages.get(i).getId()));	
				}
			}
			pipe.sync();

		} finally {
			jedis.close();
			sw.stop();
		}
	
		
	}

	@Override
	public boolean setUnackTimeout(String messageId, long timeout) {

		Stopwatch sw = monitor.ack.start();
		Jedis jedis = connPool.getResource();

		try {

			double unackScore = Long.valueOf(System.currentTimeMillis() + timeout).doubleValue();
			Double score = jedis.zscore(unackShardKey, messageId);
			if (score != null) {
				jedis.zadd(unackShardKey, unackScore, messageId);
				return true;
			}
		
			return false;

		} finally {
			jedis.close();
			sw.stop();
		}
	}

	@Override
	public boolean setTimeout(String messageId, long timeout) {

		Jedis jedis = connPool.getResource();

		try {
			String json = jedis.hget(messageStoreKey, messageId);
			if (json == null) {
				return false;
			}
			Message message = om.readValue(json, Message.class);
			message.setTimeout(timeout);

			Double score = jedis.zscore(myQueueShard, messageId);
			if (score != null) {
				double priorityd = message.getPriority() / 100.0;
				double newScore = Long.valueOf(System.currentTimeMillis() + timeout).doubleValue() + priorityd;
				jedis.zadd(myQueueShard, newScore, messageId);
				json = om.writeValueAsString(message);
				jedis.hset(messageStoreKey, message.getId(), json);
				return true;
			
			}
		
			return false;
		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			jedis.close();
		}

	}

	@Override
	public boolean remove(String messageId) {

		Stopwatch sw = monitor.remove.start();
		Jedis jedis = connPool.getResource();

		try {

			jedis.zrem(unackShardKey, messageId);

			Long removed = jedis.zrem(myQueueShard, messageId);
			Long msgRemoved = jedis.hdel(messageStoreKey, messageId);

			if (removed > 0 && msgRemoved > 0) {
				return true;
			}
		
			return false;

		} finally {
			jedis.close();
			sw.stop();
		}
	}

	@Override
	public Message get(String messageId) {

		Stopwatch sw = monitor.get.start();
		Jedis jedis = connPool.getResource();
		try {

			String json = jedis.hget(messageStoreKey, messageId);
			if (json == null) {
				if (logger.isDebugEnabled()) {
					logger.debug("Cannot get the message payload " + messageId);
				}
				return null;
			}

			Message msg = om.readValue(json, Message.class);
			return msg;

		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			jedis.close();
			sw.stop();
		}
	}

	@Override
	public long size() {

		Stopwatch sw = monitor.size.start();
		Jedis jedis = nonQuorumPool.getResource();

		try {
			long size = jedis.zcard(myQueueShard);
			return size;
		} finally {
			jedis.close();
			sw.stop();
		}
	}

	@Override
	public Map<String, Map<String, Long>> shardSizes() {

		Stopwatch sw = monitor.size.start();
		Map<String, Map<String, Long>> shardSizes = new HashMap<>();
		Jedis jedis = nonQuorumPool.getResource();
		try {

			long size = jedis.zcard(myQueueShard);
			long uacked = jedis.zcard(unackShardKey);
			Map<String, Long> shardDetails = new HashMap<>();
			shardDetails.put("size", size);
			shardDetails.put("uacked", uacked);
			shardSizes.put(shardName, shardDetails);
		
			return shardSizes;

		} finally {
			jedis.close();
			sw.stop();
		}
	}

	@Override
	public void clear() {
		Jedis jedis = connPool.getResource();
		try {

			jedis.del(myQueueShard);
			jedis.del(unackShardKey);
			jedis.del(messageStoreKey);
			
		} finally {
			jedis.close();
		}
	}

	private Set<String> peekIds(int offset, int count) {
		Jedis jedis = connPool.getResource();
		try {
			double now = Long.valueOf(System.currentTimeMillis() + 1).doubleValue();
			Set<String> scanned = jedis.zrangeByScore(myQueueShard, 0, now, offset, count);
			return scanned;
		} finally {
			jedis.close();
		}
	}

	public void processUnacks() {

		Stopwatch sw = monitor.processUnack.start();
		Jedis jedis = connPool.getResource();

		try {

			long queueDepth = size();
			monitor.queueDepth.record(queueDepth);

			int batchSize = 1_000;

			double now = Long.valueOf(System.currentTimeMillis()).doubleValue();

			Set<Tuple> unacks = jedis.zrangeByScoreWithScores(unackShardKey, 0, now, 0, batchSize);

			if (unacks.size() > 0) {
				logger.debug("Adding " + unacks.size() + " messages back to the queue for " + queueName);
			}

			for (Tuple unack : unacks) {

				double score = unack.getScore();
				String member = unack.getElement();

				String payload = jedis.hget(messageStoreKey, member);
				if (payload == null) {
					jedis.zrem(unackShardKey, member);
					continue;
				}

				jedis.zadd(myQueueShard, score, member);
				jedis.zrem(unackShardKey, member);
			}

		} finally {
			jedis.close();
			sw.stop();
		}

	}
	
	@Override
	public void close() throws IOException {
		schedulerForUnacksProcessing.shutdown();
		schedulerForPrefetchProcessing.shutdown();
		monitor.close();
	}
}
