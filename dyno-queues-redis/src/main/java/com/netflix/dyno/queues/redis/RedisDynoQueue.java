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

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Uninterruptibles;
import com.netflix.dyno.connectionpool.exception.DynoException;
import com.netflix.dyno.queues.DynoQueue;
import com.netflix.dyno.queues.Message;
import com.netflix.dyno.queues.redis.sharding.ShardingStrategy;
import com.netflix.servo.monitor.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisCommands;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Tuple;
import redis.clients.jedis.params.sortedset.ZAddParams;

import java.io.IOException;
import java.time.Clock;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 *
 * @author Viren
 * Current Production (March 2018) recipe - well tested in production.
 * Note, this recipe does not use redis pipelines and hence the throughput offered is less compared to v2 recipes.
 */
public class RedisDynoQueue implements DynoQueue {

    private final Logger logger = LoggerFactory.getLogger(RedisDynoQueue.class);

    private final Clock clock;

    private final String queueName;

    private final List<String> allShards;

    private final String shardName;

    private final String redisKeyPrefix;

    private final String messageStoreKey;

    private final String myQueueShard;

    private volatile int unackTime = 60;

    private final QueueMonitor monitor;

    private final ObjectMapper om;

    private volatile JedisCommands quorumConn;

    private volatile JedisCommands nonQuorumConn;

    private final ConcurrentLinkedQueue<String> prefetchedIds;

    private final ScheduledExecutorService schedulerForUnacksProcessing;

    private final int retryCount = 2;

    private final ShardingStrategy shardingStrategy;

    public RedisDynoQueue(String redisKeyPrefix, String queueName, Set<String> allShards, String shardName, ShardingStrategy shardingStrategy) {
        this(redisKeyPrefix, queueName, allShards, shardName, 60_000, shardingStrategy);
    }

    public RedisDynoQueue(String redisKeyPrefix, String queueName, Set<String> allShards, String shardName, int unackScheduleInMS, ShardingStrategy shardingStrategy) {
        this(Clock.systemDefaultZone(), redisKeyPrefix, queueName, allShards, shardName, unackScheduleInMS, shardingStrategy);
    }

    public RedisDynoQueue(Clock clock, String redisKeyPrefix, String queueName, Set<String> allShards, String shardName, int unackScheduleInMS, ShardingStrategy shardingStrategy) {
        this.clock = clock;
        this.redisKeyPrefix = redisKeyPrefix;
        this.queueName = queueName;
        this.allShards = ImmutableList.copyOf(allShards.stream().collect(Collectors.toList()));
        this.shardName = shardName;
        this.messageStoreKey = redisKeyPrefix + ".MESSAGE." + queueName;
        this.myQueueShard = getQueueShardKey(queueName, shardName);
        this.shardingStrategy = shardingStrategy;

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

        schedulerForUnacksProcessing.scheduleAtFixedRate(() -> processUnacks(), unackScheduleInMS, unackScheduleInMS, TimeUnit.MILLISECONDS);

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
            execute("push", "(a shard in) " + queueName, () -> {
                for (Message message : messages) {
                    String json = om.writeValueAsString(message);
                    quorumConn.hset(messageStoreKey, message.getId(), json);
                    double priority = message.getPriority() / 100.0;
                    double score = Long.valueOf(clock.millis() + message.getTimeout()).doubleValue() + priority;
                    String shard = shardingStrategy.getNextShard(allShards, message);
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

            List<Message> msgs = execute("peek", messageStoreKey, () -> {
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

    @Override
    public List<Message> pop(int messageCount, int wait, TimeUnit unit) {

        if (messageCount < 1) {
            return Collections.emptyList();
        }

        Stopwatch sw = monitor.start(monitor.pop, messageCount);
        try {
            long start = clock.millis();
            long waitFor = unit.toMillis(wait);
            prefetch.addAndGet(messageCount);
            prefetchIds();
            while(prefetchedIds.size() < messageCount && ((clock.millis() - start) < waitFor)) {
                Uninterruptibles.sleepUninterruptibly(200, TimeUnit.MILLISECONDS);
                prefetchIds();
            }
            return _pop(messageCount);

        } catch(Exception e) {
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
            if(prefetch.get() < 0 || ids.isEmpty()) {
                prefetch.set(0);
            }
        } finally {
            sw.stop();
        }

    }

    private List<Message> _pop(int messageCount) throws Exception {

        double unackScore = Long.valueOf(clock.millis() + unackTime).doubleValue();
        String unackQueueName = getUnackKey(queueName, shardName);

        List<Message> popped = new LinkedList<>();
        ZAddParams zParams = ZAddParams.zAddParams().nx();

        for (;popped.size() != messageCount;) {
            String msgId = prefetchedIds.poll();
            if(msgId == null) {
                break;
            }

            long added = quorumConn.zadd(unackQueueName, unackScore, msgId, zParams);
            if(added == 0){
                if (logger.isDebugEnabled()) {
                    logger.debug("cannot add {} to the unack shard ", queueName, msgId);
                }
                monitor.misses.increment();
                continue;
            }

            long removed = quorumConn.zrem(myQueueShard, msgId);
            if (removed == 0) {
                if (logger.isDebugEnabled()) {
                    logger.debug("cannot remove {} from the queue shard ", queueName, msgId);
                }
                monitor.misses.increment();
                continue;
            }

            String json = quorumConn.hget(messageStoreKey, msgId);
            if(json == null){
                if (logger.isDebugEnabled()) {
                    logger.debug("Cannot get the message payload for {}", msgId);
                }
                monitor.misses.increment();
                continue;
            }
            Message msg = om.readValue(json, Message.class);
            popped.add(msg);

            if (popped.size() == messageCount) {
                return popped;
            }
        }
        return popped;
    }

    @Override
    public boolean ack(String messageId) {

        Stopwatch sw = monitor.ack.start();

        try {
            return execute("ack", "(a shard in) " + queueName, () -> {

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
    public void ack(List<Message> messages) {
        for(Message message : messages) {
            ack(message.getId());
        }
    }

    @Override
    public boolean setUnackTimeout(String messageId, long timeout) {

        Stopwatch sw = monitor.ack.start();

        try {
            return execute("setUnackTimeout", "(a shard in) " + queueName, () -> {
                double unackScore = Long.valueOf(clock.millis() + timeout).doubleValue();
                for (String shard : allShards) {

                    String unackShardKey = getUnackKey(queueName, shard);
                    Double score = quorumConn.zscore(unackShardKey, messageId);
                    if(score != null) {
                        quorumConn.zadd(unackShardKey, unackScore, messageId);
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
    public boolean setTimeout(String messageId, long timeout) {

        return execute("setTimeout", "(a shard in) " + queueName, () -> {

            String json = nonQuorumConn.hget(messageStoreKey, messageId);
            if(json == null) {
                return false;
            }
            Message message = om.readValue(json, Message.class);
            message.setTimeout(timeout);

            for (String shard : allShards) {

                String queueShard = getQueueShardKey(queueName, shard);
                Double score = quorumConn.zscore(queueShard, messageId);
                if(score != null) {
                    double priorityd = message.getPriority() / 100;
                    double newScore = Long.valueOf(clock.millis() + timeout).doubleValue() + priorityd;
                    ZAddParams params = ZAddParams.zAddParams().xx();
                    quorumConn.zadd(queueShard, newScore, messageId, params);
                    json = om.writeValueAsString(message);
                    quorumConn.hset(messageStoreKey, message.getId(), json);
                    return true;
                }
            }
            return false;
        });
    }

    @Override
    public boolean remove(String messageId) {

        Stopwatch sw = monitor.remove.start();

        try {

            return execute("remove", "(a shard in) " + queueName, () -> {

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
    public boolean ensure(Message message) {
      return execute("ensure", "(a shard in) " + queueName, () -> {

        String messageId = message.getId();
        for (String shard : allShards) {

          String queueShard = getQueueShardKey(queueName, shard);
          Double score = quorumConn.zscore(queueShard, messageId);
          if(score != null) {
            return false;
          }
          String unackShardKey = getUnackKey(queueName, shard);
          score = quorumConn.zscore(unackShardKey, messageId);
          if(score != null) {
            return false;
          }
        }
        push(Collections.singletonList(message));
        return true;
      });
    }

    @Override
    public boolean containsPredicate(String predicate) {
        return execute("containsPredicate", messageStoreKey, () -> {

            // We use a Lua script here to do predicate matching since we only want to find whether the predicate
            // exists in any of the message bodies or not, and the only way to do that is to check for the predicate
            // match on the server side.
            // The alternative is to have the server return all the hash values back to us and we filter it here on
            // the client side. This is not desirable since we would potentially be sending large amounts of data
            // over the network only to return a 'true' or 'false' value back to the calling application.
            String predicateCheckLuaScript = "local hkey=KEYS[1]\n" +
                    "local predicate=ARGV[1]\n" +
                    "local cursor=0\n" +
                    "local begin=false\n" +
                    "while (cursor ~= 0 or begin==false) do\n" +
                    "  local ret = redis.call('hscan', hkey, cursor)\n" +
                    "  for i, content in ipairs(ret[2]) do\n" +
                    "    if (string.find(content, predicate)) then\n" +
                    "      return 1\n" +
                    "    end\n" +
                    "  end\n" +
                    "  cursor=tonumber(ret[1])\n" +
                    "  begin=true\n" +
                    "end\n" +
                    "return 0";

            // Cast from 'JedisCommands' to 'Jedis' here since the former does not expose 'eval()'.
            int retval = (int) ((Jedis)quorumConn).eval(predicateCheckLuaScript,
                    Collections.singletonList(messageStoreKey), Collections.singletonList(predicate));

            return (retval == 1);
        });
    }

    @Override
    public Message get(String messageId) {

        Stopwatch sw = monitor.get.start();

        try {

            return execute("get", messageStoreKey, () -> {
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

            return execute("size", "(a shard in) " + queueName, () -> {
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

            return execute("shardSizes", "(a shard in) " + queueName, () -> {
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
        execute("clear", "(a shard in) " + queueName, () -> {
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

        return execute("peekIds", myQueueShard, () -> {
            double now = Long.valueOf(clock.millis() + 1).doubleValue();
            Set<String> scanned = quorumConn.zrangeByScore(myQueueShard, 0, now, offset, count);
            return scanned;
        });

    }

    @Override
    public void processUnacks() {

        Stopwatch sw = monitor.processUnack.start();
        try {

            long queueDepth = size();
            monitor.queueDepth.record(queueDepth);

            String keyName = getUnackKey(queueName, shardName);
            execute("processUnacks", keyName, () -> {

                int batchSize = 1_000;
                String unackQueueName = getUnackKey(queueName, shardName);

                double now = Long.valueOf(clock.millis()).doubleValue();

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
                return null;
            });

        } finally {
            sw.stop();
        }

    }

    private String getQueueShardKey(String queueName, String shard) {
        return redisKeyPrefix + ".QUEUE." + queueName + "." + shard;
    }

    private String getUnackKey(String queueName, String shard) {
        return redisKeyPrefix + ".UNACK." + queueName + "." + shard;
    }

    private <R> R execute(String opName, String keyName, Callable<R> r) {
        return executeWithRetry(opName, keyName, r, 0);
    }

    private <R> R executeWithRetry(String opName, String keyName, Callable<R> r, int retryCount) {

        try {

            return r.call();

        } catch (ExecutionException e) {

            if (e.getCause() instanceof DynoException) {
                if (retryCount < this.retryCount) {
                    return executeWithRetry(opName, keyName, r, ++retryCount);
                }
            }
            throw new RuntimeException(e.getCause());
        } catch (Exception e) {
            throw new RuntimeException(
                    "Operation: ( " + opName + " ) failed on key: [" + keyName + " ].", e);
        }
    }

    @Override
    public void close() throws IOException {
        schedulerForUnacksProcessing.shutdown();
        monitor.close();
    }
}
