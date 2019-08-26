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
import com.netflix.dyno.connectionpool.HostSupplier;
import com.netflix.dyno.connectionpool.TokenMapSupplier;
import com.netflix.dyno.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.dyno.connectionpool.impl.lb.HostToken;
import com.netflix.dyno.jedis.DynoJedisClient;
import com.netflix.dyno.queues.DynoQueue;
import com.netflix.dyno.queues.redis.BaseQueueTests;
import redis.clients.jedis.Jedis;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

public class DynoJedisTests extends BaseQueueTests {

    private static Jedis dynoClient;

    private static RedisPipelineQueue rdq;

    private static String messageKeyPrefix;

    private static int maxHashBuckets = 32;

    public DynoJedisTests() {
        super("dyno_queue_tests");
    }

    @Override
    public DynoQueue getQueue(String redisKeyPrefix, String queueName) {

        List<Host> hosts = new ArrayList<>(1);
        hosts.add(
                new HostBuilder()
                        .setHostname("localhost")
                        .setIpAddress("127.0.0.1")
                        .setPort(6379)
                        .setRack("us-east-1a")
                        .setDatacenter("us-east-1")
                        .setStatus(Host.Status.Up)
                        .createHost()
        );


        QueueBuilder qb = new QueueBuilder();

        DynoJedisClient.Builder builder = new DynoJedisClient.Builder();
        HostSupplier hs = new HostSupplier() {
            @Override
            public List<Host> getHosts() {
                return hosts;
            }
        };

        ConnectionPoolConfigurationImpl cp = new ConnectionPoolConfigurationImpl("test").withTokenSupplier(new TokenMapSupplier() {

            HostToken token = new HostToken(1L, hosts.get(0));

            @Override
            public List<HostToken> getTokens(Set<Host> activeHosts) {
                return Arrays.asList(token);
            }

            @Override
            public HostToken getTokenForHost(Host host, Set<Host> activeHosts) {
                return token;
            }


        }).setLocalRack("us-east-1a").setLocalDataCenter("us-east-1");
        cp.setSocketTimeout(0);
        cp.setConnectTimeout(0);
        cp.setMaxConnsPerHost(10);
        cp.withHashtag("{}");

        DynoJedisClient client = builder.withApplicationName("test")
                .withDynomiteClusterName("test")
                .withCPConfig(cp)
                .withHostSupplier(hs)
                .build();

        return qb
                .setCurrentShard("a")
                .setQueueName(queueName)
                .setRedisKeyPrefix(redisKeyPrefix)
                .setUnackTime(1_000)
                .useDynomite(client, client)
                .build();
    }


}
