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
import java.text.NumberFormat;
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

    private final boolean singleRingTopology;

    // Tracks the number of message IDs to prefetch based on the message counts requested by the caller via pop().
    @VisibleForTesting
    AtomicInteger numIdsToPrefetch;

    // Tracks the number of message IDs to prefetch based on the message counts requested by the caller via
    // unsafePopAllShards().
    @VisibleForTesting
    AtomicInteger unsafeNumIdsToPrefetchAllShards;

    public RedisDynoQueue(String redisKeyPrefix, String queueName, Set<String> allShards, String shardName, ShardingStrategy shardingStrategy, boolean singleRingTopology) {
        this(redisKeyPrefix, queueName, allShards, shardName, 60_000, shardingStrategy, singleRingTopology);
    }

    public RedisDynoQueue(String redisKeyPrefix, String queueName, Set<String> allShards, String shardName, int unackScheduleInMS, ShardingStrategy shardingStrategy, boolean singleRingTopology) {
        this(Clock.systemDefaultZone(), redisKeyPrefix, queueName, allShards, shardName, unackScheduleInMS, shardingStrategy, singleRingTopology);
    }

    public RedisDynoQueue(Clock clock, String redisKeyPrefix, String queueName, Set<String> allShards, String shardName, int unackScheduleInMS, ShardingStrategy shardingStrategy, boolean singleRingTopology) {
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
        this.singleRingTopology = singleRingTopology;

        this.om = QueueUtils.constructObjectMapper();
        this.monitor = new QueueMonitor(queueName, shardName);
        this.prefetchedIds = new ConcurrentLinkedQueue<>();
        this.unsafePrefetchedIdsAllShardsMap = new HashMap<>();
        for (String shard : allShards) {
            unsafePrefetchedIdsAllShardsMap.put(getQueueShardKey(queueName, shard), new ConcurrentLinkedQueue<>());
        }

        schedulerForUnacksProcessing = Executors.newScheduledThreadPool(1);

        if (this.singleRingTopology) {
            schedulerForUnacksProcessing.scheduleAtFixedRate(() -> atomicProcessUnacks(), unackScheduleInMS, unackScheduleInMS, TimeUnit.MILLISECONDS);
        } else {
            schedulerForUnacksProcessing.scheduleAtFixedRate(() -> processUnacks(), unackScheduleInMS, unackScheduleInMS, TimeUnit.MILLISECONDS);
        }

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
        return popWithMsgIdHelper(messageId, shardName, true);
    }

    @Override
    public Message unsafePopWithMsgIdAllShards(String messageId) {
        int numShards = allShards.size();
        for (String shard : allShards) {
            boolean warnIfNotExists = false;

            // Only one of the shards will have the message, so we don't want the check in the other 2 shards
            // to spam the logs. So make sure only the last shard emits a warning log which means that none of the
            // shards have 'messageId'.
            if (--numShards == 0) warnIfNotExists = true;

            Message msg = popWithMsgIdHelper(messageId, shard, warnIfNotExists);
            if (msg != null) return msg;
        }
        return null;
    }

    public Message popWithMsgIdHelper(String messageId, String targetShard, boolean warnIfNotExists) {

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
                    // We only have a 'warnIfNotExists' check for this call since not all messages are present in
                    // all shards. So we want to avoid a log spam. If any of the following calls return 'null' or '0',
                    // we may have hit an inconsistency (because it's in the queue, but other calls have failed),
                    // so make sure to log those.
                    if (warnIfNotExists) {
                        logger.warn("Cannot find the message with ID {}", messageId);
                    }
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

            // TODO: Check for duplicates.
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
    public boolean atomicRemove(String messageId) {

        Stopwatch sw = monitor.remove.start();

        try {

            return execute("remove", "(a shard in) " + queueName, () -> {


                String atomicRemoveScript = "local hkey=KEYS[1]\n" +
                        "local msg_id=ARGV[1]\n" +
                        "local num_shards=ARGV[2]\n" +
                        "\n" +
                        "local removed_shard=0\n" +
                        "local removed_unack=0\n" +
                        "local removed_hash=0\n" +
                        "for i=0,num_shards-1 do\n" +
                        "  local shard_name = ARGV[3+(i*2)]\n" +
                        "  local unack_name = ARGV[3+(i*2)+1]\n" +
                        "\n" +
                        "  removed_shard = removed_shard + redis.call('zrem', shard_name, msg_id)\n" +
                        "  removed_unack = removed_unack + redis.call('zrem', unack_name, msg_id)\n" +
                        "end\n" +
                        "\n" +
                        "removed_hash = redis.call('hdel', hkey, msg_id)\n" +
                        "if (removed_shard==1 or removed_unack==1 or removed_hash==1) then\n" +
                        "  return 1\n" +
                        "end\n" +
                        "return removed_unack\n";

                ImmutableList.Builder builder = ImmutableList.builder();
                builder.add(messageId);
                builder.add(Integer.toString(allShards.size()));

                for (String shard : allShards) {

                    String queueShardKey = getQueueShardKey(queueName, shard);
                    String unackShardKey = getUnackKey(queueName, shard);

                    builder.add(queueShardKey);
                    builder.add(unackShardKey);
                }

                Long removed = (Long) ((DynoJedisClient)quorumConn).eval(atomicRemoveScript, Collections.singletonList(messageStoreKey), builder.build());
                if (removed == 1) return true;

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

    private Message popMsgWithPredicateObeyPriority(String predicate, boolean localShardOnly) {
        String popPredicateObeyPriority = "local hkey=KEYS[1]\n" +
                "local predicate=ARGV[1]\n" +
                "local num_shards=ARGV[2]\n" +
                "local peek_until=tonumber(ARGV[3])\n" +
                "local unack_score=tonumber(ARGV[4])\n" +
                "\n" +
                "local shard_names={}\n" +
                "local unack_names={}\n" +
                "local shard_lengths={}\n" +
                "local largest_shard=-1\n" +
                "for i=0,num_shards-1 do\n" +
                "  shard_names[i+1]=ARGV[5+(i*2)]\n" +
                "  shard_lengths[i+1] = redis.call('zcard', shard_names[i+1])\n" +
                "  unack_names[i+1]=ARGV[5+(i*2)+1]\n" +
                "\n" +
                "  if (shard_lengths[i+1] > largest_shard) then\n" +
                "    largest_shard = shard_lengths[i+1]\n" +
                "  end\n" +
                "end\n" +
                "\n" +
                "local min_score=-1\n" +
                "local min_member\n" +
                "local matching_value\n" +
                "local owning_shard_idx=-1\n" +
                "\n" +
                "local num_complete_shards=0\n" +
                "for j=0,largest_shard-1 do\n" +
                "  for i=1,num_shards do\n" +
                "    local skiploop=false\n" +
                "    if (shard_lengths[i] < j+1) then\n" +
                "      skiploop=true\n" +
                "    end\n" +
                "\n" +
                "    if (skiploop == false) then\n" +
                "      local element = redis.call('zrange', shard_names[i], j, j, 'WITHSCORES')\n" +
                "      if ((min_score ~= -1 and min_score < tonumber(element[2])) or peek_until < tonumber(element[2])) then\n" +
                "        -- This is to make sure we don't process this shard again\n" +
                "        -- since all elements henceforth are of lower priority than min_member\n" +
                "        shard_lengths[i]=0\n" +
                "        num_complete_shards = num_complete_shards + 1\n" +
                "      else\n" +
                "        local value = redis.call('hget', hkey, tostring(element[1]))\n" +
                "        if (value) then\n" +
                "          if (string.match(value, predicate)) then\n" +
                "            if (min_score == -1 or tonumber(element[2]) < min_score) then\n" +
                "              min_score = tonumber(element[2])\n" +
                "              owning_shard_idx=i\n" +
                "              min_member = element[1]\n" +
                "              matching_value = value\n" +
                "            end\n" +
                "          end\n" +
                "        end\n" +
                "      end\n" +
                "    end\n" +
                "  end\n" +
                "  if (num_complete_shards == num_shards) then\n" +
                "    break\n" +
                "  end\n" +
                "end\n" +
                "\n" +
                "if (min_member) then\n" +
                "  local queue_shard_name=shard_names[owning_shard_idx]\n" +
                "  local unack_shard_name=unack_names[owning_shard_idx]\n" +
                "  local zadd_ret = redis.call('zadd', unack_shard_name, 'NX', unack_score, min_member)\n" +
                "  if (zadd_ret) then\n" +
                "    redis.call('zrem', queue_shard_name, min_member)\n" +
                "  end\n" +
                "end\n" +
                "return {min_member, matching_value}";

        double now = Long.valueOf(clock.millis() + 1).doubleValue();
        double unackScore = Long.valueOf(clock.millis() + unackTime).doubleValue();

        // The script requires the scores as whole numbers
        NumberFormat fmt = NumberFormat.getIntegerInstance();
        fmt.setGroupingUsed(false);
        String nowScoreString = fmt.format(now);
        String unackScoreString = fmt.format(unackScore);

        ArrayList<String> retval;
        if (localShardOnly) {
            String unackShardName = getUnackKey(queueName, shardName);

            ImmutableList.Builder builder = ImmutableList.builder();
            builder.add(predicate);
            builder.add(Integer.toString(1));
            builder.add(nowScoreString);
            builder.add(unackScoreString);
            builder.add(localQueueShard);
            builder.add(unackShardName);

            // Cast from 'JedisCommands' to 'DynoJedisClient' here since the former does not expose 'eval()'.
            retval = (ArrayList) ((DynoJedisClient) quorumConn).eval(popPredicateObeyPriority,
                    Collections.singletonList(messageStoreKey), builder.build());
        } else {

            ImmutableList.Builder builder = ImmutableList.builder();
            builder.add(predicate);
            builder.add(Integer.toString(allShards.size()));
            builder.add(nowScoreString);
            builder.add(unackScoreString);
            for (String shard : allShards) {
                String queueShard = getQueueShardKey(queueName, shard);
                String unackShardName = getUnackKey(queueName, shard);
                builder.add(queueShard);
                builder.add(unackShardName);
            }

            // Cast from 'JedisCommands' to 'DynoJedisClient' here since the former does not expose 'eval()'.
            retval = (ArrayList) ((DynoJedisClient) quorumConn).eval(popPredicateObeyPriority,
                    Collections.singletonList(messageStoreKey), builder.build());
        }

        if (retval.size() == 0) return null;
        return new Message(retval.get(0), retval.get(1));

    }

    @Override
    public Message popMsgWithPredicate(String predicate, boolean localShardOnly) {
        Stopwatch sw = monitor.start(monitor.pop, 1);

        try {
            Message payload = execute("popMsgWithPredicateObeyPriority", messageStoreKey, () -> popMsgWithPredicateObeyPriority(predicate, localShardOnly));
            return payload;

        } finally {
            sw.stop();
        }

    }

    @Override
    public List<Message> bulkPop(int messageCount, int wait, TimeUnit unit) {

        if (messageCount < 1) {
            return Collections.emptyList();
        }

        Stopwatch sw = monitor.start(monitor.pop, messageCount);
        try {
            long start = clock.millis();
            long waitFor = unit.toMillis(wait);
            numIdsToPrefetch.addAndGet(messageCount);

            prefetchIds();
            while (prefetchedIds.size() < messageCount && ((clock.millis() - start) < waitFor)) {
                Uninterruptibles.sleepUninterruptibly(200, TimeUnit.MILLISECONDS);
                prefetchIds();
            }
            int numToPop = (prefetchedIds.size() > messageCount) ? messageCount : prefetchedIds.size();
            return atomicBulkPopHelper(numToPop, prefetchedIds, true);

        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            sw.stop();
        }

    }

    @Override
    public List<Message> unsafeBulkPop(int messageCount, int wait, TimeUnit unit) {
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

            int numToPop = (unsafeGetNumPrefetchedIds() > messageCount) ? messageCount : unsafeGetNumPrefetchedIds();
            ConcurrentLinkedQueue<String> messageIds = new ConcurrentLinkedQueue<>();
            int numPrefetched = 0;
            for (String shard : allShards) {
                String queueShardName = getQueueShardKey(queueName, shard);
                int prefetchedIdsSize = unsafePrefetchedIdsAllShardsMap.get(queueShardName).size();
                for (int i = 0; i < prefetchedIdsSize; ++i) {
                    messageIds.add(unsafePrefetchedIdsAllShardsMap.get(queueShardName).poll());
                    if (++numPrefetched == numToPop) break;
                }
                if (numPrefetched == numToPop) break;
            }
            return atomicBulkPopHelper(numToPop, messageIds, false);
        } catch(Exception e) {
            throw new RuntimeException(e);
        } finally {
            sw.stop();
        }
    }

    // TODO: Do code cleanup/consolidation
    private List<Message> atomicBulkPopHelper(int messageCount,
                          ConcurrentLinkedQueue<String> prefetchedIdQueue, boolean localShardOnly) throws IOException {

        double now = Long.valueOf(clock.millis() + 1).doubleValue();
        double unackScore = Long.valueOf(clock.millis() + unackTime).doubleValue();

        // The script requires the scores as whole numbers
        NumberFormat fmt = NumberFormat.getIntegerInstance();
        fmt.setGroupingUsed(false);
        String nowScoreString = fmt.format(now);
        String unackScoreString = fmt.format(unackScore);

        List<String> messageIds = new ArrayList<>();
        for (int i = 0; i < messageCount; ++i) {
            messageIds.add(prefetchedIdQueue.poll());
        }

        String atomicBulkPopScriptLocalOnly="local hkey=KEYS[1]\n" +
                "local num_msgs=ARGV[1]\n" +
                "local peek_until=ARGV[2]\n" +
                "local unack_score=ARGV[3]\n" +
                "local queue_shard_name=ARGV[4]\n" +
                "local unack_shard_name=ARGV[5]\n" +
                "local msg_start_idx = 6\n" +
                "local idx = 1\n" +
                "local return_vals={}\n" +
                "for i=0,num_msgs-1 do\n" +
                "  local message_id=ARGV[msg_start_idx + i]\n" +
                "  local exists = redis.call('zscore', queue_shard_name, message_id)\n" +
                "  if (exists) then\n" +
                "    if (exists <=peek_until) then\n" +
                "      local value = redis.call('hget', hkey, message_id)\n" +
                "      if (value) then\n" +
                "        local zadd_ret = redis.call('zadd', unack_shard_name, 'NX', unack_score, message_id)\n" +
                "        if (zadd_ret) then\n" +
                "          redis.call('zrem', queue_shard_name, message_id)\n" +
                "          return_vals[idx]=value\n" +
                "          idx=idx+1\n" +
                "        end\n" +
                "      end\n" +
                "    end\n" +
                "  else\n" +
                "    return {}\n" +
                "  end\n" +
                "end\n" +
                "return return_vals";

        String atomicBulkPopScript="local hkey=KEYS[1]\n" +
                "local num_msgs=ARGV[1]\n" +
                "local num_shards=ARGV[2]\n" +
                "local peek_until=ARGV[3]\n" +
                "local unack_score=ARGV[4]\n" +
                "local shard_start_idx = 5\n" +
                "local msg_start_idx = 5 + (num_shards * 2)\n" +
                "local out_idx = 1\n" +
                "local return_vals={}\n" +
                "for i=0,num_msgs-1 do\n" +
                "  local found_msg=false\n" +
                "  local message_id=ARGV[msg_start_idx + i]\n" +
                "  for j=0,num_shards-1 do\n" +
                "    local queue_shard_name=ARGV[shard_start_idx + (j*2)]\n" +
                "    local unack_shard_name=ARGV[shard_start_idx + (j*2) + 1]\n" +
                "    local exists = redis.call('zscore', queue_shard_name, message_id)\n" +
                "    if (exists) then\n" +
                "      found_msg=true\n" +
                "      if (exists <=peek_until) then\n" +
                "        local value = redis.call('hget', hkey, message_id)\n" +
                "        if (value) then\n" +
                "          local zadd_ret = redis.call('zadd', unack_shard_name, 'NX', unack_score, message_id)\n" +
                "          if (zadd_ret) then\n" +
                "            redis.call('zrem', queue_shard_name, message_id)\n" +
                "            return_vals[out_idx]=value\n" +
                "            out_idx=out_idx+1\n" +
                "            break\n" +
                "          end\n" +
                "        end\n" +
                "      end\n" +
                "    end\n" +
                "  end\n" +
                "  if (found_msg == false) then\n" +
                "    return {}\n" +
                "  end\n" +
                "end\n" +
                "return return_vals";

        List<Message> payloads = new ArrayList<>();
        if (localShardOnly) {
            String unackShardName = getUnackKey(queueName, shardName);

            ImmutableList.Builder builder = ImmutableList.builder();
            builder.add(Integer.toString(messageCount));
            builder.add(nowScoreString);
            builder.add(unackScoreString);
            builder.add(localQueueShard);
            builder.add(unackShardName);
            for (int i = 0; i < messageCount; ++i) {
                builder.add(messageIds.get(i));
            }

            List<String> jsonPayloads;
            // Cast from 'JedisCommands' to 'DynoJedisClient' here since the former does not expose 'eval()'.
            jsonPayloads = (List) ((DynoJedisClient) quorumConn).eval(atomicBulkPopScriptLocalOnly,
                    Collections.singletonList(messageStoreKey), builder.build());

            for (String p : jsonPayloads) {
                Message msg = om.readValue(p, Message.class);
                payloads.add(msg);
            }
        } else {
            ImmutableList.Builder builder = ImmutableList.builder();
            builder.add(Integer.toString(messageCount));
            builder.add(Integer.toString(allShards.size()));
            builder.add(nowScoreString);
            builder.add(unackScoreString);
            for (String shard : allShards) {
                String queueShard = getQueueShardKey(queueName, shard);
                String unackShardName = getUnackKey(queueName, shard);
                builder.add(queueShard);
                builder.add(unackShardName);
            }
            for (int i = 0; i < messageCount; ++i) {
                builder.add(messageIds.get(i));
            }

            List<String> jsonPayloads;
            // Cast from 'JedisCommands' to 'DynoJedisClient' here since the former does not expose 'eval()'.
            jsonPayloads = (List) ((DynoJedisClient) quorumConn).eval(atomicBulkPopScript,
                    Collections.singletonList(messageStoreKey), builder.build());

            for (String p : jsonPayloads) {
                Message msg = om.readValue(p, Message.class);
                payloads.add(msg);
            }
        }

        return payloads;
    }

    /**
     *
     * Similar to popWithMsgId() but completes all the operations in one round trip.
     *
     * NOTE: This function assumes that the ring size in the cluster is 1. DO NOT use for APIs that support a ring
     * size larger than 1.
     *
     * @param messageId
     * @param localShardOnly
     * @return
     */
    private String atomicPopWithMsgIdHelper(String messageId, boolean localShardOnly) {

        double now = Long.valueOf(clock.millis() + 1).doubleValue();
        double unackScore = Long.valueOf(clock.millis() + unackTime).doubleValue();

        // The script requires the scores as whole numbers
        NumberFormat fmt = NumberFormat.getIntegerInstance();
        fmt.setGroupingUsed(false);
        String nowScoreString = fmt.format(now);
        String unackScoreString = fmt.format(unackScore);

        String atomicPopScript = "local hkey=KEYS[1]\n" +
                "local message_id=ARGV[1]\n" +
                "local num_shards=ARGV[2]\n" +
                "local peek_until=ARGV[3]\n" +
                "local unack_score=ARGV[4]\n" +
                "for i=0,num_shards-1 do\n" +
                "  local queue_shard_name=ARGV[(i*2)+5]\n" +
                "  local unack_shard_name=ARGV[(i*2)+5+1]\n" +
                "  local exists = redis.call('zscore', queue_shard_name, message_id)\n" +
                "  if (exists) then\n" +
                "    if (exists <= peek_until) then\n" +
                "      local value = redis.call('hget', hkey, message_id)\n" +
                "      if (value) then\n" +
                "        local zadd_ret = redis.call('zadd', unack_shard_name, 'NX', unack_score, message_id )\n" +
                "        if (zadd_ret) then\n" +
                "          redis.call('zrem', queue_shard_name, message_id)\n" +
                "          return value\n" +
                "        end\n" +
                "      end\n" +
                "    end\n" +
                "  end\n" +
                "end\n" +
                "return nil";

        String retval;
        if (localShardOnly) {
            String unackShardName = getUnackKey(queueName, shardName);

            retval = (String) ((DynoJedisClient) quorumConn).eval(atomicPopScript, Collections.singletonList(messageStoreKey),
                    ImmutableList.of(messageId, Integer.toString(1), nowScoreString,
                            unackScoreString, localQueueShard, unackShardName));
        } else {
            int numShards = allShards.size();
            ImmutableList.Builder builder = ImmutableList.builder();
            builder.add(messageId);
            builder.add(Integer.toString(numShards));
            builder.add(nowScoreString);
            builder.add(unackScoreString);

            List<String> arguments = Arrays.asList(messageId, Integer.toString(numShards), nowScoreString,
                    unackScoreString);
            for (String shard : allShards) {
                String queueShard = getQueueShardKey(queueName, shard);
                String unackShardName = getUnackKey(queueName, shard);
                builder.add(queueShard);
                builder.add(unackShardName);
            }
            retval = (String) ((DynoJedisClient) quorumConn).eval(atomicPopScript, Collections.singletonList(messageStoreKey), builder.build());
        }

        return retval;
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
    public List<Message> getAllMessages() {
        Map<String, String> allMsgs = nonQuorumConn.hgetAll(messageStoreKey);
        List<Message> retList = new ArrayList<>();
        for (Map.Entry<String,String> entry: allMsgs.entrySet()) {
            Message msg = new Message(entry.getKey(), entry.getValue());
            retList.add(msg);
        }

        return retList;
    }

    @Override
    public Message localGet(String messageId) {

        Stopwatch sw = monitor.get.start();

        try {

            return execute("localGet", messageStoreKey, () -> {
                String json = nonQuorumConn.hget(messageStoreKey, messageId);
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

        logger.info("processUnacks() will NOT be atomic.");
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

    @Override
    public List<Message> findStaleMessages() {
        return execute("findStaleMessages", localQueueShard, () -> {

            List<Message> stale_msgs = new ArrayList<>();

            int batchSize = 1_000;

            double now = Long.valueOf(clock.millis()).doubleValue();
            long num_stale = 0;

            Set<String> elems = nonQuorumConn.zrangeByScore(localQueueShard, 0, now, 0, batchSize);

            if (elems.size() == 0) {
                return stale_msgs;
            }

            String findStaleMsgsScript = "local hkey=KEYS[1]\n" +
                    "local queue_shard=ARGV[1]\n" +
                    "local unack_shard=ARGV[2]\n" +
                    "local num_msgs=ARGV[3]\n" +
                    "\n" +
                    "local stale_msgs={}\n" +
                    "local num_stale_idx = 1\n" +
                    "for i=0,num_msgs-1 do\n" +
                    "  local msg_id=ARGV[4+i]\n" +
                    "\n" +
                    "  local exists_hash = redis.call('hget', hkey, msg_id)\n" +
                    "  local exists_queue = redis.call('zscore', queue_shard, msg_id)\n" +
                    "  local exists_unack = redis.call('zscore', unack_shard, msg_id)\n" +
                    "\n" +
                    "  if (exists_hash and exists_queue) then\n" +
                    "  elseif (not (exists_unack)) then\n" +
                    "    stale_msgs[num_stale_idx] = msg_id\n" +
                    "    num_stale_idx = num_stale_idx + 1\n" +
                    "  end\n" +
                    "end\n" +
                "\n" +
                "return stale_msgs\n";

            String unackKey = getUnackKey(queueName, shardName);
            ImmutableList.Builder builder = ImmutableList.builder();
            builder.add(localQueueShard);
            builder.add(unackKey);
            builder.add(Integer.toString(elems.size()));
            for (String msg : elems) {
                builder.add(msg);
            }

            ArrayList<String> stale_msg_ids = (ArrayList) ((DynoJedisClient)quorumConn).eval(findStaleMsgsScript, Collections.singletonList(messageStoreKey), builder.build());
            num_stale = stale_msg_ids.size();
            if (num_stale > 0) {
                logger.info("findStaleMsgs(): Found " + num_stale + " messages present in queue but not in hashmap");
            }

            for (String m : stale_msg_ids) {
                Message msg = new Message();
                msg.setId(m);
                stale_msgs.add(msg);
            }
            return stale_msgs;
        });
    }

    @Override
    public void atomicProcessUnacks() {

        logger.info("processUnacks() will be atomic.");
        Stopwatch sw = monitor.processUnack.start();
        try {

            long queueDepth = size();
            monitor.queueDepth.record(queueDepth);

            String keyName = getUnackKey(queueName, shardName);
            execute("processUnacks", keyName, () -> {

                int batchSize = 1_000;
                String unackShardName = getUnackKey(queueName, shardName);

                double now = Long.valueOf(clock.millis()).doubleValue();
                long num_moved_back = 0;
                long num_stale = 0;

                Set<Tuple> unacks = nonQuorumConn.zrangeByScoreWithScores(unackShardName, 0, now, 0, batchSize);

                if (unacks.size() > 0) {
                    logger.info("processUnacks: Attempting to add " + unacks.size() + " messages back to shard of queue: " + unackShardName);
                } else {
                    return null;
                }

                String atomicProcessUnacksScript = "local hkey=KEYS[1]\n" +
                        "local unack_shard=ARGV[1]\n" +
                        "local queue_shard=ARGV[2]\n" +
                        "local num_unacks=ARGV[3]\n" +
                        "\n" +
                        "local unacks={}\n" +
                        "local unack_scores={}\n" +
                        "local unack_start_idx = 4\n" +
                        "for i=0,num_unacks-1 do\n" +
                        "  unacks[i]=ARGV[4 + (i*2)]\n" +
                        "  unack_scores[i]=ARGV[4+(i*2)+1]\n" +
                        "end\n" +
                        "\n" +
                        "local num_moved=0\n" +
                        "local num_stale=0\n" +
                        "for i=0,num_unacks-1 do\n" +
                        "  local mem_val = redis.call('hget', hkey, unacks[i])\n" +
                        "  if (mem_val) then\n" +
                        "    redis.call('zadd', queue_shard, unack_scores[i], unacks[i])\n" +
                        "    redis.call('zrem', unack_shard, unacks[i])\n" +
                        "    num_moved=num_moved+1\n" +
                        "  else\n" +
                        "    redis.call('zrem', unack_shard, unacks[i])\n" +
                        "    num_stale=num_stale+1\n" +
                        "  end\n" +
                        "end\n" +
                        "\n" +
                        "return {num_moved, num_stale}\n";

                ImmutableList.Builder builder = ImmutableList.builder();
                builder.add(unackShardName);
                builder.add(localQueueShard);
                builder.add(Integer.toString(unacks.size()));
                for (Tuple unack : unacks) {
                    builder.add(unack.getElement());

                    // The script requires the scores as whole numbers
                    NumberFormat fmt = NumberFormat.getIntegerInstance();
                    fmt.setGroupingUsed(false);
                    String unackScoreString = fmt.format(unack.getScore());
                    builder.add(unackScoreString);
                }

                ArrayList<Long> retval = (ArrayList) ((DynoJedisClient)quorumConn).eval(atomicProcessUnacksScript, Collections.singletonList(messageStoreKey), builder.build());
                num_moved_back = retval.get(0).longValue();
                num_stale = retval.get(1).longValue();
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
