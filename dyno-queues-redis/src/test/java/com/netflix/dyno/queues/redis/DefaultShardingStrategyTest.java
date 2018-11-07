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

import com.netflix.dyno.connectionpool.Host;
import com.netflix.dyno.connectionpool.HostSupplier;
import com.netflix.dyno.queues.Message;
import com.netflix.dyno.queues.ShardSupplier;
import com.netflix.dyno.queues.jedis.JedisMock;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

public class DefaultShardingStrategyTest {

    private static JedisMock dynoClient;

    private static final String queueName = "test_queue";

    private static final String redisKeyPrefix = "testdynoqueues";

    private static RedisDynoQueue shard1DynoQueue;
    private static RedisDynoQueue shard2DynoQueue;
    private static RedisDynoQueue shard3DynoQueue;

    private static RedisQueues shard1Queue;
    private static RedisQueues shard2Queue;
    private static RedisQueues shard3Queue;

    private static String messageKey;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {

        HostSupplier hs = new HostSupplier() {
            @Override
            public List<Host> getHosts() {
                List<Host> hosts = new LinkedList<>();
                hosts.add(new Host("localhost", 8102, "us-east-1d", Host.Status.Up));
                hosts.add(new Host("localhost", 8102, "us-east-2d", Host.Status.Up));
                hosts.add(new Host("localhost", 8102, "us-east-3d", Host.Status.Up));
                return hosts;
            }
        };

        dynoClient = new JedisMock();

        Set<String> allShards = hs.getHosts().stream().map(host -> host.getRack().substring(host.getRack().length() - 2)).collect(Collectors.toSet());
        Iterator<String> iterator = allShards.iterator();
        String shard1Name = iterator.next();
        String shard2Name = iterator.next();
        String shard3Name = iterator.next();

        ShardSupplier shard1Supplier = new ShardSupplier() {

            @Override
            public Set<String> getQueueShards() {
                return allShards;
            }

            @Override
            public String getCurrentShard() {
                return shard1Name;
            }
        };

        ShardSupplier shard2Supplier = new ShardSupplier() {

            @Override
            public Set<String> getQueueShards() {
                return allShards;
            }

            @Override
            public String getCurrentShard() {
                return shard2Name;
            }
        };


        ShardSupplier shard3Supplier = new ShardSupplier() {

            @Override
            public Set<String> getQueueShards() {
                return allShards;
            }

            @Override
            public String getCurrentShard() {
                return shard3Name;
            }
        };

        messageKey = redisKeyPrefix + ".MESSAGE." + queueName;

        shard1Queue = new RedisQueues(dynoClient, dynoClient, redisKeyPrefix, shard1Supplier, 1_000, 1_000_000);
        shard2Queue = new RedisQueues(dynoClient, dynoClient, redisKeyPrefix, shard2Supplier, 1_000, 1_000_000);
        shard3Queue = new RedisQueues(dynoClient, dynoClient, redisKeyPrefix, shard3Supplier, 1_000, 1_000_000);


        shard1DynoQueue = (RedisDynoQueue)shard1Queue.get(queueName);
        shard2DynoQueue = (RedisDynoQueue)shard2Queue.get(queueName);
        shard3DynoQueue = (RedisDynoQueue)shard3Queue.get(queueName);
    }

    @Before
    public void clearAll()
    {
        shard1DynoQueue.clear();
        shard2DynoQueue.clear();
        shard3DynoQueue.clear();
    }

    @Test
    public void testAll() {

        List<Message> messages = new LinkedList<>();

        Message msg = new Message("1", "Hello World");
        msg.setPriority(1);
        messages.add(msg);

        /**
         * Because of sharding strategy works in round-robin manner, single client, for shard1, should
         * push message(even the same) to three different shards.
         */
        shard1DynoQueue.push(messages);
        shard1DynoQueue.push(messages);
        shard1DynoQueue.push(messages);

        List<Message> popedFromShard1 = shard1DynoQueue.pop(1, 1, TimeUnit.SECONDS);

        List<Message> popedFromShard2 = shard2DynoQueue.pop(1, 1, TimeUnit.SECONDS);

        List<Message> popedFromShard3 = shard3DynoQueue.pop(1, 1, TimeUnit.SECONDS);


        assertEquals(1, popedFromShard1.size());
        assertEquals(1, popedFromShard2.size());
        assertEquals(1, popedFromShard3.size());

        assertEquals(msg, popedFromShard1.get(0));
        assertEquals(msg, popedFromShard2.get(0));
        assertEquals(msg, popedFromShard3.get(0));

    }

}
