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

import com.netflix.dyno.jedis.DynoJedisClient;
import com.netflix.dyno.queues.DynoQueue;
import com.netflix.dyno.queues.ShardSupplier;
import com.netflix.dyno.queues.redis.sharding.RoundRobinStrategy;
import com.netflix.dyno.queues.redis.sharding.ShardingStrategy;
import redis.clients.jedis.commands.JedisCommands;

import java.io.Closeable;
import java.io.IOException;
import java.time.Clock;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Viren
 *
 * Please note that you should take care for disposing resource related to RedisQueue instances - that means you
 * should call close() on RedisQueue instance.
 */
public class RedisQueues implements Closeable {

    private final Clock clock;

    private final JedisCommands quorumConn;

    private final JedisCommands nonQuorumConn;

    private final Set<String> allShards;

    private final String shardName;

    private final String redisKeyPrefix;

    private final int unackTime;

    private final int unackHandlerIntervalInMS;

    private final ConcurrentHashMap<String, DynoQueue> queues;

    private final ShardingStrategy shardingStrategy;

    private final boolean singleRingTopology;

    /**
     * @param quorumConn Dyno connection with dc_quorum enabled
     * @param nonQuorumConn    Dyno connection to local Redis
     * @param redisKeyPrefix    prefix applied to the Redis keys
     * @param shardSupplier    Provider for the shards for the queues created
     * @param unackTime    Time in millisecond within which a message needs to be acknowledged by the client, after which the message is re-queued.
     * @param unackHandlerIntervalInMS    Time in millisecond at which the un-acknowledgement processor runs
     */
    public RedisQueues(JedisCommands quorumConn, JedisCommands nonQuorumConn, String redisKeyPrefix, ShardSupplier shardSupplier, int unackTime, int unackHandlerIntervalInMS) {
        this(Clock.systemDefaultZone(), quorumConn, nonQuorumConn, redisKeyPrefix, shardSupplier, unackTime, unackHandlerIntervalInMS, new RoundRobinStrategy());
    }

    /**
     * @param quorumConn Dyno connection with dc_quorum enabled
     * @param nonQuorumConn    Dyno connection to local Redis
     * @param redisKeyPrefix    prefix applied to the Redis keys
     * @param shardSupplier    Provider for the shards for the queues created
     * @param unackTime    Time in millisecond within which a message needs to be acknowledged by the client, after which the message is re-queued.
     * @param unackHandlerIntervalInMS    Time in millisecond at which the un-acknowledgement processor runs
     * @param shardingStrategy sharding strategy responsible for calculating message's destination shard
     */
    public RedisQueues(JedisCommands quorumConn, JedisCommands nonQuorumConn, String redisKeyPrefix, ShardSupplier shardSupplier, int unackTime, int unackHandlerIntervalInMS, ShardingStrategy shardingStrategy) {
        this(Clock.systemDefaultZone(), quorumConn, nonQuorumConn, redisKeyPrefix, shardSupplier, unackTime, unackHandlerIntervalInMS, shardingStrategy);
    }


    /**
     * @param clock Time provider
     * @param quorumConn Dyno connection with dc_quorum enabled
     * @param nonQuorumConn    Dyno connection to local Redis
     * @param redisKeyPrefix    prefix applied to the Redis keys
     * @param shardSupplier    Provider for the shards for the queues created
     * @param unackTime    Time in millisecond within which a message needs to be acknowledged by the client, after which the message is re-queued.
     * @param unackHandlerIntervalInMS    Time in millisecond at which the un-acknowledgement processor runs
     * @param shardingStrategy sharding strategy responsible for calculating message's destination shard
     */
    public RedisQueues(Clock clock, JedisCommands quorumConn, JedisCommands nonQuorumConn, String redisKeyPrefix, ShardSupplier shardSupplier, int unackTime, int unackHandlerIntervalInMS, ShardingStrategy shardingStrategy) {
        this.clock = clock;
        this.quorumConn = quorumConn;
        this.nonQuorumConn = nonQuorumConn;
        this.redisKeyPrefix = redisKeyPrefix;
        this.allShards = shardSupplier.getQueueShards();
        this.shardName = shardSupplier.getCurrentShard();
        this.unackTime = unackTime;
        this.unackHandlerIntervalInMS = unackHandlerIntervalInMS;
        this.queues = new ConcurrentHashMap<>();
        this.shardingStrategy = shardingStrategy;

        if (quorumConn instanceof DynoJedisClient) {
            this.singleRingTopology = ((DynoJedisClient) quorumConn).getConnPool().getPools().size() == 3;
        } else {
            this.singleRingTopology = false;
        }
    }

    /**
     *
     * @param queueName Name of the queue
     * @return Returns the DynoQueue hosting the given queue by name
     * @see DynoQueue
     * @see RedisDynoQueue
     */
    public DynoQueue get(String queueName) {

        String key = queueName.intern();

        return queues.computeIfAbsent(key, (keyToCompute) -> new RedisDynoQueue(clock, redisKeyPrefix, queueName, allShards, shardName, unackHandlerIntervalInMS, shardingStrategy, singleRingTopology)
                .withUnackTime(unackTime)
                .withNonQuorumConn(nonQuorumConn)
                .withQuorumConn(quorumConn));
    }

    /**
     *
     * @return Collection of all the registered queues
     */
    public Collection<DynoQueue> queues() {
        return this.queues.values();
    }

    @Override
    public void close() throws IOException {
        queues.values().forEach(queue -> {
            try {
                queue.close();
            } catch (final IOException e) {
                throw new RuntimeException(e.getCause());
            }
        });
    }
}
