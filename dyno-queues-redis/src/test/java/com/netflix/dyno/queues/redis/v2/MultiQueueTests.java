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

import com.netflix.dyno.connectionpool.Host;
import com.netflix.dyno.connectionpool.HostBuilder;
import com.netflix.dyno.queues.DynoQueue;
import com.netflix.dyno.queues.Message;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 *
 */
public class MultiQueueTests {

    private static Jedis dynoClient;


    private static RedisPipelineQueue rdq;

    private static String messageKeyPrefix;

    private static int maxHashBuckets = 32;

    public DynoQueue getQueue(String redisKeyPrefix, String queueName) {
        JedisPoolConfig config = new JedisPoolConfig();
        config.setTestOnBorrow(true);
        config.setTestOnCreate(true);
        config.setMaxTotal(10);
        config.setMaxIdle(5);
        config.setMaxWaitMillis(60_000);
        JedisPool pool = new JedisPool(config, "localhost", 6379);
        dynoClient = new Jedis("localhost", 6379, 0, 0);
        dynoClient.flushAll();

        List<Host> hosts = new LinkedList<>();
        hosts.add(
                new HostBuilder()
                        .setHostname("localhost")
                        .setPort(6379)
                        .setRack("us-east-1a")
                        .createHost()
        );
        hosts.add(
                new HostBuilder()
                        .setHostname("localhost")
                        .setPort(6379)
                        .setRack("us-east-2b")
                        .createHost()
        );

        QueueBuilder qb = new QueueBuilder();
        DynoQueue queue = qb
                .setCurrentShard("a")
                .setHostToShardMap((Host h) -> h.getRack().substring(h.getRack().length() - 1))
                .setQueueName(queueName)
                .setRedisKeyPrefix(redisKeyPrefix)
                .setUnackTime(50_000)
                .useNonDynomiteRedis(config, hosts)
                .build();

        queue.clear();      //clear the queue

        return queue;

    }

    @Test
    public void testAll() {
        DynoQueue queue = getQueue("test", "multi_queue");
        assertEquals(MultiRedisQueue.class, queue.getClass());

        long start = System.currentTimeMillis();
        List<Message> popped = queue.pop(1, 1, TimeUnit.SECONDS);
        assertTrue(popped.isEmpty());       //we have not pushed anything!!!!
        long elapsedTime = System.currentTimeMillis() - start;
        System.out.println("elapsed Time " + elapsedTime);
        assertTrue(elapsedTime > 1000);

        List<Message> messages = new LinkedList<>();
        for (int i = 0; i < 10; i++) {
            Message msg = new Message();
            msg.setId("" + i);
            msg.setPayload("" + i);
            messages.add(msg);
        }
        queue.push(messages);

        assertEquals(10, queue.size());
        Map<String, Map<String, Long>> shards = queue.shardSizes();
        assertEquals(2, shards.keySet().size());        //a and b

        Map<String, Long> shardA = shards.get("a");
        Map<String, Long> shardB = shards.get("b");

        assertNotNull(shardA);
        assertNotNull(shardB);

        Long sizeA = shardA.get("size");
        Long sizeB = shardB.get("size");

        assertNotNull(sizeA);
        assertNotNull(sizeB);

        assertEquals(5L, sizeA.longValue());
        assertEquals(5L, sizeB.longValue());

        start = System.currentTimeMillis();
        popped = queue.pop(2, 1, TimeUnit.SECONDS);
        elapsedTime = System.currentTimeMillis() - start;
        assertEquals(2, popped.size());
        System.out.println("elapsed Time " + elapsedTime);
        assertTrue(elapsedTime < 1000);


        start = System.currentTimeMillis();
        popped = queue.pop(5, 5, TimeUnit.SECONDS);
        elapsedTime = System.currentTimeMillis() - start;
        assertEquals(3, popped.size());     //3 remaining in the current shard
        System.out.println("elapsed Time " + elapsedTime);
        assertTrue(elapsedTime > 5000);     //we would have waited for at least 5 second for the last 2 elements!

    }


}
