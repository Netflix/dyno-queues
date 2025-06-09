/**
 * Copyright 2018 Netflix, Inc.
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
/**
 *
 */
package com.netflix.dyno.queues.redis.v2;

import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import com.netflix.appinfo.AmazonInfo;
import com.netflix.appinfo.AmazonInfo.MetaDataKey;
import com.netflix.appinfo.InstanceInfo;
import com.netflix.appinfo.InstanceInfo.InstanceStatus;
import com.netflix.discovery.EurekaClient;
import com.netflix.discovery.shared.Application;
import com.netflix.dyno.connectionpool.Host;
import com.netflix.dyno.connectionpool.HostBuilder;
import com.netflix.dyno.connectionpool.HostSupplier;
import com.netflix.dyno.connectionpool.impl.utils.ConfigUtils;
import com.netflix.dyno.jedis.DynoJedisClient;
import com.netflix.dyno.queues.DynoQueue;
import com.netflix.dyno.queues.ShardSupplier;
import com.netflix.dyno.queues.redis.conn.DynoClientProxy;
import com.netflix.dyno.queues.redis.conn.JedisProxy;
import com.netflix.dyno.queues.redis.conn.RedisConnection;
import com.netflix.dyno.queues.shard.DynoShardSupplier;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Viren
 * Builder for the queues.
 *
 */
public class QueueBuilder {

    private Clock clock;

    private String queueName;

    private String redisKeyPrefix;

    private int unackTime;

    private String currentShard;

    private ShardSupplier shardSupplier;

    private HostSupplier hs;

    private EurekaClient eurekaClient;

    private String applicationName;

    private Collection<Host> hosts;

    private JedisPoolConfig redisPoolConfig;

    private DynoJedisClient dynoQuorumClient;

    private DynoJedisClient dynoNonQuorumClient;

    /**
     * @param clock the Clock instance to set
     * @return instance of QueueBuilder
     */
    public QueueBuilder setClock(Clock clock) {
        this.clock = clock;
        return this;
    }

    public QueueBuilder setApplicationName(String appName) {
        this.applicationName = appName;
        return this;
    }

    public QueueBuilder setEurekaClient(EurekaClient eurekaClient) {
        this.eurekaClient = eurekaClient;
        return this;
    }

    /**
     * @param queueName the queueName to set
     * @return instance of QueueBuilder
     */
    public QueueBuilder setQueueName(String queueName) {
        this.queueName = queueName;
        return this;
    }

    /**
     * @param redisKeyPrefix Prefix used for all the keys in Redis
     * @return instance of QueueBuilder
     */
    public QueueBuilder setRedisKeyPrefix(String redisKeyPrefix) {
        this.redisKeyPrefix = redisKeyPrefix;
        return this;
    }

    /**
     * @param redisPoolConfig
     * @return instance of QueueBuilder
     */
    public QueueBuilder useNonDynomiteRedis(JedisPoolConfig redisPoolConfig, List<Host> redisHosts) {
        this.redisPoolConfig = redisPoolConfig;
        this.hosts = redisHosts;
        return this;
    }

    /**
     *
     * @param dynoQuorumClient
     * @param dynoNonQuorumClient
     * @return
     */
    public QueueBuilder useDynomite(DynoJedisClient dynoQuorumClient, DynoJedisClient dynoNonQuorumClient) {
        this.dynoQuorumClient = dynoQuorumClient;
        this.dynoNonQuorumClient = dynoNonQuorumClient;
        this.hs = dynoQuorumClient.getConnPool().getConfiguration().getHostSupplier();
        return this;
    }

    /**
     * @param unackTime Time in millisecond, after which the uncked messages will be re-queued for the delivery
     * @return instance of QueueBuilder
     */
    public QueueBuilder setUnackTime(int unackTime) {
        this.unackTime = unackTime;
        return this;
    }

    /**
     * @param currentShard Name of the current shard
     * @return instance of QueueBuilder
     */
    public QueueBuilder setCurrentShard(String currentShard) {
        this.currentShard = currentShard;
        return this;
    }

    /**
     * @param shardSupplier
     * @return
     */
    public QueueBuilder setShardSupplier(ShardSupplier shardSupplier) {
        this.shardSupplier = shardSupplier;
        return this;
    }

    /**
     *
     * @return Build an instance of the queue with supplied parameters.
     * @see MultiRedisQueue
     * @see RedisPipelineQueue
     */
    public DynoQueue build() {

        boolean useDynomiteCluster = dynoQuorumClient != null;
        if (useDynomiteCluster) {
            if(hs == null) {
                hs = dynoQuorumClient.getConnPool().getConfiguration().getHostSupplier();
            }
            this.hosts = hs.getHosts();
        }

        if (shardSupplier == null) {
            String region = ConfigUtils.getDataCenter();
            String az = ConfigUtils.getLocalZone();
            shardSupplier = new DynoShardSupplier(hs, region, az);
        }
        if(currentShard == null) {
            currentShard = shardSupplier.getCurrentShard();
        }

        if (clock == null) {
            clock = Clock.systemDefaultZone();
        }

        Map<String, Host> shardMap = new HashMap<>();
        for (Host host : hosts) {
            String shard = shardSupplier.getShardForHost(host);
            shardMap.put(shard, host);
        }


        Map<String, RedisPipelineQueue> queues = new HashMap<>();

        for (String queueShard : shardMap.keySet()) {

            Host host = shardMap.get(queueShard);
            String hostAddress = host.getIpAddress();
            if (hostAddress == null || "".equals(hostAddress)) {
                hostAddress = host.getHostName();
            }
            RedisConnection redisConn = null;
            RedisConnection redisConnRead = null;

            if (useDynomiteCluster) {
                redisConn = new DynoClientProxy(dynoQuorumClient);
                if(dynoNonQuorumClient == null) {
                    dynoNonQuorumClient = dynoQuorumClient;
                }
                redisConnRead = new DynoClientProxy(dynoNonQuorumClient);
            } else {
                JedisPool pool = new JedisPool(redisPoolConfig, hostAddress, host.getPort(), 0);
                redisConn = new JedisProxy(pool);
                redisConnRead = new JedisProxy(pool);
            }

            RedisPipelineQueue q = new RedisPipelineQueue(clock, redisKeyPrefix, queueName, queueShard, unackTime, unackTime, redisConn);
            q.setNonQuorumPool(redisConnRead);

            queues.put(queueShard, q);
        }

        if (queues.size() == 1) {
            //This is a queue with a single shard
            return queues.values().iterator().next();
        }

        MultiRedisQueue queue = new MultiRedisQueue(queueName, currentShard, queues);
        return queue;
    }


    private HostSupplier getHostSupplierFromEureka(String applicationName) {
        return () -> {
            Application app = eurekaClient.getApplication(applicationName);
            List<Host> hosts = new ArrayList<>();

            if (app == null) {
                return hosts;
            }

            List<InstanceInfo> ins = app.getInstances();

            if (ins == null || ins.isEmpty()) {
                return hosts;
            }

            hosts = Lists.newArrayList(Collections2.transform(ins,

                    info -> {

                        Host.Status status = info.getStatus() == InstanceStatus.UP ? Host.Status.Up : Host.Status.Down;
                        String rack = null;
                        if (info.getDataCenterInfo() instanceof AmazonInfo) {
                            AmazonInfo amazonInfo = (AmazonInfo) info.getDataCenterInfo();
                            rack = amazonInfo.get(MetaDataKey.availabilityZone);
                        }
                        //Host host = new Host(info.getHostName(), info.getIPAddr(), rack, status);
                        Host host = new HostBuilder()
                                .setHostname(info.getHostName())
                                .setIpAddress(info.getIPAddr())
                                .setRack(rack).setStatus(status)
                                .createHost();
                        return host;
                    }));
            return hosts;
        };
    }
}
