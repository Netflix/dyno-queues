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
package com.netflix.dyno.queues.redis;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Uninterruptibles;
import com.netflix.dyno.jedis.DynoJedisClient;
import com.netflix.dyno.queues.DynoQueue;
import com.netflix.dyno.queues.Message;
import com.netflix.dyno.queues.redis.sharding.ShardingStrategy;
import com.netflix.servo.monitor.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Tuple;
import redis.clients.jedis.commands.JedisCommands;
import redis.clients.jedis.params.ZAddParams;

import java.io.IOException;
import java.time.Clock;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.netflix.dyno.queues.redis.QueueUtils.execute;

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

    private final String localQueueShard;

    private volatile int unackTime = 60;

    private final QueueMonitor monitor;

    private final ObjectMapper om;

    private volatile JedisCommands quorumConn;

    private volatile JedisCommands nonQuorumConn;

    private final ConcurrentLinkedQueue<String> prefetchedIds;

    private final Map<String, ConcurrentLinkedQueue<String>> unsafePrefetchedIdsAllShardsMap;

    private final ScheduledExecutorService schedulerForUnacksProcessing;

    private final int retryCount = 2;

    private final ShardingStrategy shardingStrategy;

    // Tracks the number of message IDs to prefetch based on the message counts requested by the caller via pop().
    @VisibleForTesting
    AtomicInteger numIdsToPrefetch;

    // Tracks the number of message IDs to prefetch based on the message counts requested by the caller via
    // unsafePopAllShards().
    @VisibleForTesting
    AtomicInteger unsafeNumIdsToPrefetchAllShards;

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
        this.localQueueShard = getQueueShardKey(queueName, shardName);
        this.shardingStrategy = shardingStrategy;

        this.numIdsToPrefetch = new AtomicInteger(0);
        this.unsafeNumIdsToPrefetchAllShards = new AtomicInteger(0);

        this.om = QueueUtils.constructObjectMapper();
        this.monitor = new QueueMonitor(queueName, shardName);
        this.prefetchedIds = new ConcurrentLinkedQueue<>();
        this.unsafePrefetchedIdsAllShardsMap = new HashMap<>();
        for (String shard : allShards) {
            unsafePrefetchedIdsAllShardsMap.put(getQueueShardKey(queueName, shard), new ConcurrentLinkedQueue<>());
        }

        schedulerForUnacksProcessing = Executors.newScheduledThreadPool(1);

        schedulerForUnacksProcessing.scheduleAtFixedRate(() -> processUnacks(), unackScheduleInMS, unackScheduleInMS, TimeUnit.MILLISECONDS);

        logger.info(RedisDynoQueue.class.getName() + " is ready to serve " + queueName);
    }

    public RedisDynoQueue withQuorumConn(JedisCommands quorumConn) {
        this.quorumConn = quorumConn;
        return this;
    }

    public RedisDynoQueue withNonQuorumConn(JedisCommands nonQuorumConn) {
        this.nonQuorumConn = nonQuorumConn;
        return this;
    }

    public RedisDynoQueue withUnackTime(int unackTime) {
        this.unackTime = unackTime;
        return this;
    }

    /**
     * @return Number of items in each ConcurrentLinkedQueue from 'unsafePrefetchedIdsAllShardsMap'.
     */
    private int unsafeGetNumPrefetchedIds() {
        // Note: We use an AtomicInteger due to Java's limitation of not allowing the modification of local native
        // data types in lambdas (Java 8).
        AtomicInteger totalSize = new AtomicInteger(0);
        unsafePrefetchedIdsAllShardsMap.forEach((k,v)->totalSize.addAndGet(v.size()));
        return totalSize.get();
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
            return doPeekBodyHelper(ids);

        } finally {
            sw.stop();
        }
    }

    @Override
    public List<Message> unsafePeekAllShards(final int messageCount) {

        Stopwatch sw = monitor.peek.start();

        try {

            Set<String> ids = peekIdsAllShards(0, messageCount);
            if (ids == null) {
                return Collections.emptyList();
            }
            return doPeekBodyHelper(ids);
        } finally {
            sw.stop();
        }
    }

    /**
     *
     * Peeks into 'this.localQueueShard' and returns up to 'count' items starting at position 'offset' in the shard.
     *
     *
     * @param offset Number of items to skip over in 'this.localQueueShard'
     * @param count Number of items to return.
     * @return Up to 'count' number of message IDs in a set.
     */
    private Set<String> peekIds(final int offset, final int count, final double peekTillTs) {

        return execute("peekIds", localQueueShard, () -> {
            double peekTillTsOrNow = (peekTillTs == 0.0) ? Long.valueOf(clock.millis() + 1).doubleValue() : peekTillTs;
            return doPeekIdsFromShardHelper(localQueueShard, peekTillTsOrNow, offset, count);
        });

    }

    private Set<String> peekIds(final int offset, final int count) {
        return peekIds(offset, count, 0.0);
    }

    /**
     *
     * Same as 'peekIds()' but looks into all shards of the queue ('this.allShards').
     *
     * @param count Number of items to return.
     * @return Up to 'count' number of message IDs in a set.
     */
    private Set<String> peekIdsAllShards(final int offset, final int count) {
        return execute("peekIdsAllShards", localQueueShard, () -> {
            Set<String> scanned = new HashSet<>();
            double now = Long.valueOf(clock.millis() + 1).doubleValue();
            int remaining_count = count;

            // Try to get as many items from 'this.localQueueShard' first to reduce chances of returning duplicate items.
            // (See unsafe* functions disclaimer in DynoQueue.java)
            scanned.addAll(peekIds(offset, count, now));
            remaining_count -= scanned.size();

            for (String shard : allShards) {
                String queueShardName = getQueueShardKey(queueName, shard);
                // Skip 'localQueueShard'.
                if (queueShardName.equals(localQueueShard)) continue;

                Set<String> elems = doPeekIdsFromShardHelper(queueShardName, now, offset, count);
                scanned.addAll(elems);
                remaining_count -= elems.size();
                if (remaining_count <= 0) break;
            }

            return scanned;

        });
    }

    private Set<String> doPeekIdsFromShardHelper(final String queueShardName, final double peekTillTs, final int offset,
                                          final int count) {
        return nonQuorumConn.zrangeByScore(queueShardName, 0, peekTillTs, offset, count);
    }

    /**
     * Takes a set of message IDs, 'message_ids', and returns a list of Message objects
     * corresponding to 'message_ids'. Read only, does not make any updates.
     *
     * @param message_ids Set of message IDs to peek.
     * @return a list of Message objects corresponding to 'message_ids'
     *
     */
    private List<Message> doPeekBodyHelper(Set<String> message_ids) {
        List<Message> msgs = execute("peek", messageStoreKey, () -> {
            List<Message> messages = new LinkedList<Message>();
            for (String id : message_ids) {
                String json = nonQuorumConn.hget(messageStoreKey, id);
                Message message = om.readValue(json, Message.class);
                messages.add(message);
            }
            return messages;
        });

        return msgs;
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
            numIdsToPrefetch.addAndGet(messageCount);

            // We prefetch message IDs here first before attempting to pop them off the sorted set.
            // The reason we do this (as opposed to just popping from the head of the sorted set),
            // is that due to the eventually consistent nature of Dynomite, the different replicas of the same
            // sorted set _may_ not look exactly the same at any given time, i.e. they may have a different number of
            // items due to replication lag.
            // So, we first peek into the sorted set to find the list of message IDs that we know for sure are
            // replicated across all replicas and then attempt to pop them based on those message IDs.
            prefetchIds();
            while (prefetchedIds.size() < messageCount && ((clock.millis() - start) < waitFor)) {
                Uninterruptibles.sleepUninterruptibly(200, TimeUnit.MILLISECONDS);
                prefetchIds();
            }
            return _pop(shardName, messageCount, prefetchedIds);

        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            sw.stop();
        }

    }

    @Override
    public Message popWithMsgId(String messageId) {
        return popWithMsgIdHelper(messageId, shardName);
    }

    @Override
    public Message unsafePopWithMsgIdAllShards(String messageId) {
        for (String shard : allShards) {
            Message msg = popWithMsgIdHelper(messageId, shard);
            if (msg != null) return msg;
        }
        return null;
    }

    public Message popWithMsgIdHelper(String messageId, String targetShard) {

        Stopwatch sw = monitor.start(monitor.pop, 1);

        try {
            return execute("popWithMsgId", targetShard, () -> {

                String queueShardName = getQueueShardKey(queueName, targetShard);
                double unackScore = Long.valueOf(clock.millis() + unackTime).doubleValue();
                String unackShardName = getUnackKey(queueName, targetShard);

                ZAddParams zParams = ZAddParams.zAddParams().nx();

                Long exists = nonQuorumConn.zrank(queueShardName, messageId);
                // If we get back a null type, then the element doesn't exist.
                if (exists == null) {
                    logger.warn("Cannot find the message with ID {}", messageId);
                    monitor.misses.increment();
                    return null;
                }

                String json = quorumConn.hget(messageStoreKey, messageId);
                if (json == null) {
                    logger.warn("Cannot get the message payload for {}", messageId);
                    monitor.misses.increment();
                    return null;
                }

                long added = quorumConn.zadd(unackShardName, unackScore, messageId, zParams);
                if (added == 0) {
                    logger.warn("cannot add {} to the unack shard {}", messageId, unackShardName);
                    monitor.misses.increment();
                    return null;
                }

                long removed = quorumConn.zrem(queueShardName, messageId);
                if (removed == 0) {
                    logger.warn("cannot remove {} from the queue shard ", queueName, messageId);
                    monitor.misses.increment();
                    return null;
                }

                Message msg = om.readValue(json, Message.class);
                return msg;
            });
        } finally {
            sw.stop();
        }

    }

    public List<Message> unsafePopAllShards(int messageCount, int wait, TimeUnit unit) {
        if (messageCount < 1) {
            return Collections.emptyList();
        }

        Stopwatch sw = monitor.start(monitor.pop, messageCount);
        try {
            long start = clock.millis();
            long waitFor = unit.toMillis(wait);
            unsafeNumIdsToPrefetchAllShards.addAndGet(messageCount);

            prefetchIdsAllShards();
            while(unsafeGetNumPrefetchedIds() < messageCount && ((clock.millis() - start) < waitFor)) {
                Uninterruptibles.sleepUninterruptibly(200, TimeUnit.MILLISECONDS);
                prefetchIdsAllShards();
            }

            int remainingCount = messageCount;
            // Pop as much as possible from the local shard first to reduce chances of returning duplicate items.
            // (See unsafe* functions disclaimer in DynoQueue.java)
            List<Message> popped = _pop(shardName, remainingCount, unsafePrefetchedIdsAllShardsMap.get(localQueueShard));
            remainingCount -= popped.size();

            for (String shard : allShards) {
                String queueShardName = getQueueShardKey(queueName, shard);
                List<Message> elems = _pop(shard, remainingCount, unsafePrefetchedIdsAllShardsMap.get(queueShardName));
                popped.addAll(elems);
                remainingCount -= elems.size();
            }
            return popped;
        } catch(Exception e) {
            throw new RuntimeException(e);
        } finally {
            sw.stop();
        }
    }

    /**
     * Prefetch message IDs from the local shard.
     */
    private void prefetchIds() {
        double now = Long.valueOf(clock.millis() + 1).doubleValue();
        int numPrefetched = doPrefetchIdsHelper(localQueueShard, numIdsToPrefetch, prefetchedIds, now);
        if (numPrefetched == 0) {
            numIdsToPrefetch.set(0);
        }
    }


    /**
     * Prefetch message IDs from all shards.
     */
    private void prefetchIdsAllShards() {
        double now = Long.valueOf(clock.millis() + 1).doubleValue();

        // Try to prefetch as many items from 'this.localQueueShard' first to reduce chances of returning duplicate items.
        // (See unsafe* functions disclaimer in DynoQueue.java)
        doPrefetchIdsHelper(localQueueShard, unsafeNumIdsToPrefetchAllShards,
                unsafePrefetchedIdsAllShardsMap.get(localQueueShard), now);

        if (unsafeNumIdsToPrefetchAllShards.get() < 1) return;

        for (String shard : allShards) {
            String queueShardName = getQueueShardKey(queueName, shard);
            if (queueShardName.equals(localQueueShard)) continue; // Skip since we've already serviced the local shard.

            doPrefetchIdsHelper(queueShardName, unsafeNumIdsToPrefetchAllShards,
                    unsafePrefetchedIdsAllShardsMap.get(queueShardName), now);
        }
    }

    /**
     * Attempts to prefetch up to 'prefetchCounter' message IDs, by peeking into a queue based on 'peekFunction',
     * and store it in a concurrent linked queue.
     *
     * @param prefetchCounter Number of message IDs to attempt prefetch.
     * @param prefetchedIdQueue Concurrent Linked Queue where message IDs are stored.
     * @param peekFunction Function to call to peek into the queue.
     */
    private int doPrefetchIdsHelper(String queueShardName, AtomicInteger prefetchCounter,
                                     ConcurrentLinkedQueue<String> prefetchedIdQueue, double prefetchFromTs) {

        if (prefetchCounter.get() < 1) {
            return 0;
        }

        int numSuccessfullyPrefetched = 0;
        int numToPrefetch = prefetchCounter.get();
        Stopwatch sw = monitor.start(monitor.prefetch, numToPrefetch);
        try {
            // Attempt to peek up to 'numToPrefetch' message Ids.
            Set<String> ids = doPeekIdsFromShardHelper(queueShardName, prefetchFromTs, 0, numToPrefetch);

            // Store prefetched IDs in a queue.
            prefetchedIdQueue.addAll(ids);

            numSuccessfullyPrefetched = ids.size();

            // Account for number of IDs successfully prefetched.
            prefetchCounter.addAndGet((-1 * ids.size()));
            if(prefetchCounter.get() < 0) {
                prefetchCounter.set(0);
            }
        } finally {
            sw.stop();
        }
        return numSuccessfullyPrefetched;
    }

    private List<Message> _pop(String shard, int messageCount,
                               ConcurrentLinkedQueue<String> prefetchedIdQueue) throws Exception {

        String queueShardName = getQueueShardKey(queueName, shard);
        String unackShardName = getUnackKey(queueName, shard);
        double unackScore = Long.valueOf(clock.millis() + unackTime).doubleValue();

        // NX option indicates add only if it doesn't exist.
        // https://redis.io/commands/zadd#zadd-options-redis-302-or-greater
        ZAddParams zParams = ZAddParams.zAddParams().nx();

        List<Message> popped = new LinkedList<>();
        for (;popped.size() != messageCount;) {
            String msgId = prefetchedIdQueue.poll();
            if(msgId == null) {
                break;
            }

            long added = quorumConn.zadd(unackShardName, unackScore, msgId, zParams);
            if(added == 0){
                logger.warn("cannot add {} to the unack shard {}", msgId, unackShardName);
                monitor.misses.increment();
                continue;
            }

            long removed = quorumConn.zrem(queueShardName, msgId);
            if (removed == 0) {
                logger.warn("cannot remove {} from the queue shard {}", msgId, queueShardName);
                monitor.misses.increment();
                continue;
            }

            String json = quorumConn.hget(messageStoreKey, msgId);
            if (json == null) {
                logger.warn("Cannot get the message payload for {}", msgId);
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
        for (Message message : messages) {
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
                    if (score != null) {
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
            if (json == null) {
                return false;
            }
            Message message = om.readValue(json, Message.class);
            message.setTimeout(timeout);

            for (String shard : allShards) {

                String queueShard = getQueueShardKey(queueName, shard);
                Double score = quorumConn.zscore(queueShard, messageId);
                if (score != null) {
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

                    if (removed > 0) {
                        // Ignoring return value since we just want to get rid of it.
                        Long msgRemoved = quorumConn.hdel(messageStoreKey, messageId);
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
                if (score != null) {
                    return false;
                }
                String unackShardKey = getUnackKey(queueName, shard);
                score = quorumConn.zscore(unackShardKey, messageId);
                if (score != null) {
                    return false;
                }
            }
            push(Collections.singletonList(message));
            return true;
        });
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
        return execute("containsPredicate", messageStoreKey, () -> getMsgWithPredicate(predicate, localShardOnly) != null);
    }

    @Override
    public String getMsgWithPredicate(String predicate, boolean localShardOnly) {
        return execute("getMsgWithPredicate", messageStoreKey, () -> {

            // We use a Lua script here to do predicate matching since we only want to find whether the predicate
            // exists in any of the message bodies or not, and the only way to do that is to check for the predicate
            // match on the server side.
            // The alternative is to have the server return all the hash values back to us and we filter it here on
            // the client side. This is not desirable since we would potentially be sending large amounts of data
            // over the network only to return a single string value back to the calling application.
            String predicateCheckAllLuaScript = "local hkey=KEYS[1]\n" +
                    "local predicate=ARGV[1]\n" +
                    "local cursor=0\n" +
                    "local begin=false\n" +
                    "while (cursor ~= 0 or begin==false) do\n" +
                    "  local ret = redis.call('hscan', hkey, cursor)\n" +
                    "  local curmsgid\n" +
                    "  for i, content in ipairs(ret[2]) do\n" +
                    "    if (i % 2 ~= 0) then\n" +
                    "      curmsgid = content\n" +
                    "    elseif (string.match(content, predicate)) then\n" +
                    "      return curmsgid\n" +
                    "    end\n" +
                    "  end\n" +
                    "  cursor=tonumber(ret[1])\n" +
                    "  begin=true\n" +
                    "end\n" +
                    "return nil";

            String predicateCheckLocalOnlyLuaScript = "local hkey=KEYS[1]\n" +
                    "local predicate=ARGV[1]\n" +
                    "local shard_name=ARGV[2]\n" +
                    "local cursor=0\n" +
                    "local begin=false\n" +
                    "while (cursor ~= 0 or begin==false) do\n" +
                    "    local ret = redis.call('hscan', hkey, cursor)\n" +
                    "local curmsgid\n" +
                    "for i, content in ipairs(ret[2]) do\n" +
                    "    if (i % 2 ~= 0) then\n" +
                    "        curmsgid = content\n" +
                    "elseif (string.match(content, predicate)) then\n" +
                    "local in_local_shard = redis.call('zrank', shard_name, curmsgid)\n" +
                    "if (type(in_local_shard) ~= 'boolean' and in_local_shard >= 0) then\n" +
                    "return curmsgid\n" +
                    "end\n" +
                    "        end\n" +
                    "end\n" +
                    "        cursor=tonumber(ret[1])\n" +
                    "begin=true\n" +
                    "end\n" +
                    "return nil";

            String retval;
            if (localShardOnly) {
                // Cast from 'JedisCommands' to 'DynoJedisClient' here since the former does not expose 'eval()'.
                retval = (String) ((DynoJedisClient) nonQuorumConn).eval(predicateCheckLocalOnlyLuaScript,
                        Collections.singletonList(messageStoreKey), ImmutableList.of(predicate, localQueueShard));
            } else {
                // Cast from 'JedisCommands' to 'DynoJedisClient' here since the former does not expose 'eval()'.
                retval = (String) ((DynoJedisClient) nonQuorumConn).eval(predicateCheckAllLuaScript,
                        Collections.singletonList(messageStoreKey), Collections.singletonList(predicate));
            }

            return retval;
        });
    }

    @Override
    public Message get(String messageId) {

        Stopwatch sw = monitor.get.start();

        try {

            return execute("get", messageStoreKey, () -> {
                String json = quorumConn.hget(messageStoreKey, messageId);
                if (json == null) {
                    logger.warn("Cannot get the message payload " + messageId);
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

    @Override
    public void processUnacks() {

        Stopwatch sw = monitor.processUnack.start();
        try {

            long queueDepth = size();
            monitor.queueDepth.record(queueDepth);

            String keyName = getUnackKey(queueName, shardName);
            execute("processUnacks", keyName, () -> {

                int batchSize = 1_000;
                String unackShardName = getUnackKey(queueName, shardName);

                double now = Long.valueOf(clock.millis()).doubleValue();
                int num_moved_back = 0;
                int num_stale = 0;

                Set<Tuple> unacks = nonQuorumConn.zrangeByScoreWithScores(unackShardName, 0, now, 0, batchSize);

                if (unacks.size() > 0) {
                    logger.info("processUnacks: Attempting to add " + unacks.size() + " messages back to shard of queue: " + unackShardName);
                }

                for (Tuple unack : unacks) {

                    double score = unack.getScore();
                    String member = unack.getElement();

                    String payload = quorumConn.hget(messageStoreKey, member);
                    if (payload == null) {
                        quorumConn.zrem(unackShardName, member);
                        ++num_stale;
                        continue;
                    }

                    long added_back = quorumConn.zadd(localQueueShard, score, member);
                    long removed_from_unack = quorumConn.zrem(unackShardName, member);
                    if (added_back > 0 && removed_from_unack > 0) ++num_moved_back;
                }

                if (num_moved_back > 0 || num_stale > 0) {
                    logger.info("processUnacks: Moved back " + num_moved_back + " items. Got rid of " + num_stale + " stale items.");
                }
                return null;
            });

        } catch (Exception e) {
            logger.error("Error while processing unacks. " + e.getMessage());
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

    @Override
    public void close() throws IOException {
        schedulerForUnacksProcessing.shutdown();
        monitor.close();
    }
}
