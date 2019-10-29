/**
 * Copyright 2016 Netflix, Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.dyno.queues.redis.v2;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.dyno.connectionpool.HashPartitioner;
import com.netflix.dyno.connectionpool.impl.hash.Murmur3HashPartitioner;
import com.netflix.dyno.queues.DynoQueue;
import com.netflix.dyno.queues.Message;
import com.netflix.dyno.queues.redis.QueueMonitor;
import com.netflix.dyno.queues.redis.QueueUtils;
import com.netflix.dyno.queues.redis.conn.Pipe;
import com.netflix.dyno.queues.redis.conn.RedisConnection;
import com.netflix.servo.monitor.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Response;
import redis.clients.jedis.Tuple;
import redis.clients.jedis.params.ZAddParams;

import java.io.IOException;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * @author Viren
 * Queue implementation that uses Redis pipelines that improves the throughput under heavy load.
 */
public class RedisPipelineQueue implements DynoQueue {

    private final Logger logger = LoggerFactory.getLogger(RedisPipelineQueue.class);

    private final Clock clock;

    private final String queueName;

    private final String shardName;

    private final String messageStoreKeyPrefix;

    private final String myQueueShard;

    private final String unackShardKeyPrefix;

    private final int unackTime;

    private final QueueMonitor monitor;

    private final ObjectMapper om;

    private final RedisConnection connPool;

    private volatile RedisConnection nonQuorumPool;

    private final ScheduledExecutorService schedulerForUnacksProcessing;

    private final HashPartitioner partitioner = new Murmur3HashPartitioner();

    private final int maxHashBuckets = 32;

    private final int longPollWaitIntervalInMillis = 10;

    public RedisPipelineQueue(String redisKeyPrefix, String queueName, String shardName, int unackScheduleInMS, int unackTime, RedisConnection pool) {
        this(Clock.systemDefaultZone(), redisKeyPrefix, queueName, shardName, unackScheduleInMS, unackTime, pool);
    }

    public RedisPipelineQueue(Clock clock, String redisKeyPrefix, String queue, String shardName, int unackScheduleInMS, int unackTime, RedisConnection pool) {
        this.clock = clock;
        this.queueName = queue;
        String qName = "{" + queue + "." + shardName + "}";
        this.shardName = shardName;

        this.messageStoreKeyPrefix = redisKeyPrefix + ".MSG." + qName;
        this.myQueueShard = redisKeyPrefix + ".QUEUE." + qName;
        this.unackShardKeyPrefix = redisKeyPrefix + ".UNACK." + qName + ".";
        this.unackTime = unackTime;
        this.connPool = pool;
        this.nonQuorumPool = pool;

        this.om = QueueUtils.constructObjectMapper();
        this.monitor = new QueueMonitor(qName, shardName);

        schedulerForUnacksProcessing = Executors.newScheduledThreadPool(1);

        schedulerForUnacksProcessing.scheduleAtFixedRate(() -> processUnacks(), unackScheduleInMS, unackScheduleInMS, TimeUnit.MILLISECONDS);

        logger.info(RedisPipelineQueue.class.getName() + " is ready to serve " + qName + ", shard=" + shardName);

    }

    /**
     * @param nonQuorumPool When using a cluster like Dynomite, which relies on the quorum reads, supply a separate non-quorum read connection for ops like size etc.
     */
    public void setNonQuorumPool(RedisConnection nonQuorumPool) {
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
        RedisConnection conn = connPool.getResource();
        try {

            Pipe pipe = conn.pipelined();

            for (Message message : messages) {
                String json = om.writeValueAsString(message);
                pipe.hset(messageStoreKey(message.getId()), message.getId(), json);
                double priority = message.getPriority() / 100.0;
                double score = Long.valueOf(clock.millis() + message.getTimeout()).doubleValue() + priority;
                pipe.zadd(myQueueShard, score, message.getId());
            }
            pipe.sync();
            pipe.close();

            return messages.stream().map(msg -> msg.getId()).collect(Collectors.toList());

        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            conn.close();
            sw.stop();
        }
    }

    private String messageStoreKey(String msgId) {
        Long hash = partitioner.hash(msgId);
        long bucket = hash % maxHashBuckets;
        return messageStoreKeyPrefix + "." + bucket;
    }

    private String unackShardKey(String messageId) {
        Long hash = partitioner.hash(messageId);
        long bucket = hash % maxHashBuckets;
        return unackShardKeyPrefix + bucket;
    }

    @Override
    public List<Message> peek(final int messageCount) {

        Stopwatch sw = monitor.peek.start();
        RedisConnection jedis = connPool.getResource();

        try {

            Set<String> ids = peekIds(0, messageCount);
            if (ids == null) {
                return Collections.emptyList();
            }

            List<Message> messages = new LinkedList<Message>();
            for (String id : ids) {
                String json = jedis.hget(messageStoreKey(id), id);
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
    public synchronized List<Message> pop(int messageCount, int wait, TimeUnit unit) {

        if (messageCount < 1) {
            return Collections.emptyList();
        }

        Stopwatch sw = monitor.start(monitor.pop, messageCount);
        List<Message> messages = new LinkedList<>();
        int remaining = messageCount;
        long time = clock.millis() + unit.toMillis(wait);

        try {

            do {

                List<String> peeked = peekIds(0, remaining).stream().collect(Collectors.toList());
                List<Message> popped = _pop(peeked);
                int poppedCount = popped.size();
                if (poppedCount == messageCount) {
                    messages = popped;
                    break;
                }
                messages.addAll(popped);
                remaining -= poppedCount;
                if (clock.millis() > time) {
                    break;
                }

                try {
                    Thread.sleep(longPollWaitIntervalInMillis);
                } catch (InterruptedException ie) {
                    logger.error(ie.getMessage(), ie);
                }

            } while (remaining > 0);

            return messages;

        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            sw.stop();
        }

    }

    @Override
    public Message popWithMsgId(String messageId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Message unsafePopWithMsgIdAllShards(String messageId) {
        throw new UnsupportedOperationException();
    }

    private List<Message> _pop(List<String> batch) throws Exception {

        double unackScore = Long.valueOf(clock.millis() + unackTime).doubleValue();

        List<Message> popped = new LinkedList<>();
        ZAddParams zParams = ZAddParams.zAddParams().nx();

        RedisConnection jedis = connPool.getResource();
        try {

            Pipe pipe = jedis.pipelined();
            List<Response<Long>> zadds = new ArrayList<>(batch.size());

            for (int i = 0; i < batch.size(); i++) {
                String msgId = batch.get(i);
                if (msgId == null) {
                    break;
                }
                zadds.add(pipe.zadd(unackShardKey(msgId), unackScore, msgId, zParams));
            }
            pipe.sync();

            pipe = jedis.pipelined();
            int count = zadds.size();
            List<String> zremIds = new ArrayList<>(count);
            List<Response<Long>> zremRes = new LinkedList<>();

            for (int i = 0; i < count; i++) {
                long added = zadds.get(i).get();
                if (added == 0) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Cannot add {} to unack queue shard", batch.get(i));
                    }
                    monitor.misses.increment();
                    continue;
                }
                String id = batch.get(i);
                zremIds.add(id);
                zremRes.add(pipe.zrem(myQueueShard, id));
            }
            pipe.sync();

            pipe = jedis.pipelined();
            List<Response<String>> getRes = new ArrayList<>(count);
            for (int i = 0; i < zremRes.size(); i++) {
                long removed = zremRes.get(i).get();
                if (removed == 0) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Cannot remove {} from queue shard", zremIds.get(i));
                    }
                    monitor.misses.increment();
                    continue;
                }
                getRes.add(pipe.hget(messageStoreKey(zremIds.get(i)), zremIds.get(i)));
            }
            pipe.sync();

            for (int i = 0; i < getRes.size(); i++) {
                String json = getRes.get(i).get();
                if (json == null) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Cannot read payload for {}", zremIds.get(i));
                    }
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
        RedisConnection jedis = connPool.getResource();

        try {

            Long removed = jedis.zrem(unackShardKey(messageId), messageId);
            if (removed > 0) {
                jedis.hdel(messageStoreKey(messageId), messageId);
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
        RedisConnection jedis = connPool.getResource();
        Pipe pipe = jedis.pipelined();
        List<Response<Long>> responses = new LinkedList<>();
        try {
            for (Message msg : messages) {
                responses.add(pipe.zrem(unackShardKey(msg.getId()), msg.getId()));
            }
            pipe.sync();
            pipe = jedis.pipelined();

            List<Response<Long>> dels = new LinkedList<>();
            for (int i = 0; i < messages.size(); i++) {
                Long removed = responses.get(i).get();
                if (removed > 0) {
                    dels.add(pipe.hdel(messageStoreKey(messages.get(i).getId()), messages.get(i).getId()));
                }
            }
            pipe.sync();

        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            jedis.close();
            sw.stop();
        }


    }

    @Override
    public boolean setUnackTimeout(String messageId, long timeout) {

        Stopwatch sw = monitor.ack.start();
        RedisConnection jedis = connPool.getResource();

        try {

            double unackScore = Long.valueOf(clock.millis() + timeout).doubleValue();
            Double score = jedis.zscore(unackShardKey(messageId), messageId);
            if (score != null) {
                jedis.zadd(unackShardKey(messageId), unackScore, messageId);
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

        RedisConnection jedis = connPool.getResource();

        try {
            String json = jedis.hget(messageStoreKey(messageId), messageId);
            if (json == null) {
                return false;
            }
            Message message = om.readValue(json, Message.class);
            message.setTimeout(timeout);

            Double score = jedis.zscore(myQueueShard, messageId);
            if (score != null) {
                double priorityd = message.getPriority() / 100.0;
                double newScore = Long.valueOf(clock.millis() + timeout).doubleValue() + priorityd;
                jedis.zadd(myQueueShard, newScore, messageId);
                json = om.writeValueAsString(message);
                jedis.hset(messageStoreKey(message.getId()), message.getId(), json);
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
        RedisConnection jedis = connPool.getResource();

        try {

            jedis.zrem(unackShardKey(messageId), messageId);

            Long removed = jedis.zrem(myQueueShard, messageId);
            Long msgRemoved = jedis.hdel(messageStoreKey(messageId), messageId);

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
    public boolean ensure(Message message) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean containsPredicate(String predicate) {
        return containsPredicate(predicate, false);
    }

    @Override
    public String getMsgWithPredicate(String predicate) {
        return getMsgWithPredicate(predicate, false);
    }

    @Override
    public boolean containsPredicate(String predicate, boolean localShardOnly) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getMsgWithPredicate(String predicate, boolean localShardOnly) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Message popMsgWithPredicate(String predicate, boolean localShardOnly) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Message get(String messageId) {

        Stopwatch sw = monitor.get.start();
        RedisConnection jedis = connPool.getResource();
        try {

            String json = jedis.hget(messageStoreKey(messageId), messageId);
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
        RedisConnection jedis = nonQuorumPool.getResource();

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
        RedisConnection jedis = nonQuorumPool.getResource();
        try {

            long size = jedis.zcard(myQueueShard);
            long uacked = 0;
            for (int i = 0; i < maxHashBuckets; i++) {
                String unackShardKey = unackShardKeyPrefix + i;
                uacked += jedis.zcard(unackShardKey);
            }

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
        RedisConnection jedis = connPool.getResource();
        try {

            jedis.del(myQueueShard);

            for (int bucket = 0; bucket < maxHashBuckets; bucket++) {
                String unackShardKey = unackShardKeyPrefix + bucket;
                jedis.del(unackShardKey);

                String messageStoreKey = messageStoreKeyPrefix + "." + bucket;
                jedis.del(messageStoreKey);

            }

        } finally {
            jedis.close();
        }
    }

    private Set<String> peekIds(int offset, int count) {
        RedisConnection jedis = connPool.getResource();
        try {
            double now = Long.valueOf(clock.millis() + 1).doubleValue();
            Set<String> scanned = jedis.zrangeByScore(myQueueShard, 0, now, offset, count);
            return scanned;
        } finally {
            jedis.close();
        }
    }

    public void processUnacks() {
        for (int i = 0; i < maxHashBuckets; i++) {
            String unackShardKey = unackShardKeyPrefix + i;
            processUnacks(unackShardKey);
        }
    }

    private void processUnacks(String unackShardKey) {

        Stopwatch sw = monitor.processUnack.start();
        RedisConnection jedis2 = connPool.getResource();

        try {

            do {

                long queueDepth = size();
                monitor.queueDepth.record(queueDepth);

                int batchSize = 1_000;

                double now = Long.valueOf(clock.millis()).doubleValue();

                Set<Tuple> unacks = jedis2.zrangeByScoreWithScores(unackShardKey, 0, now, 0, batchSize);

                if (unacks.size() > 0) {
                    logger.debug("Adding " + unacks.size() + " messages back to the queue for " + queueName);
                } else {
                    //Nothing more to be processed
                    return;
                }

                List<Tuple> requeue = new LinkedList<>();
                for (Tuple unack : unacks) {

                    double score = unack.getScore();
                    String member = unack.getElement();

                    String payload = jedis2.hget(messageStoreKey(member), member);
                    if (payload == null) {
                        jedis2.zrem(unackShardKey(member), member);
                        continue;
                    }
                    requeue.add(unack);
                }

                Pipe pipe = jedis2.pipelined();

                for (Tuple unack : requeue) {
                    double score = unack.getScore();
                    String member = unack.getElement();

                    pipe.zadd(myQueueShard, score, member);
                    pipe.zrem(unackShardKey(member), member);
                }
                pipe.sync();

            } while (true);

        } finally {
            jedis2.close();
            sw.stop();
        }

    }

    @Override
    public void close() throws IOException {
        schedulerForUnacksProcessing.shutdown();
        monitor.close();
    }

    @Override
    public List<Message> unsafePeekAllShards(final int messageCount) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<Message> unsafePopAllShards(int messageCount, int wait, TimeUnit unit) {
        throw new UnsupportedOperationException();
    }
}
