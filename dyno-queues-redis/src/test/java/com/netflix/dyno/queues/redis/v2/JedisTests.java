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
import com.netflix.dyno.queues.redis.BaseQueueTests;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.LinkedList;
import java.util.List;

/**
 *
 */
public class JedisTests extends BaseQueueTests {

    private static Jedis dynoClient;


    private static RedisPipelineQueue rdq;

    private static String messageKeyPrefix;

    private static int maxHashBuckets = 32;

    public JedisTests() {
        super("jedis_queue_tests");
    }

    @Override
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

        QueueBuilder qb = new QueueBuilder();
        DynoQueue queue = qb
                .setCurrentShard("a")
                .setQueueName(queueName)
                .setRedisKeyPrefix(redisKeyPrefix)
                .setUnackTime(1_000)
                .useNonDynomiteRedis(config, hosts)
                .build();

        queue.clear();

        return queue;

    }


}
