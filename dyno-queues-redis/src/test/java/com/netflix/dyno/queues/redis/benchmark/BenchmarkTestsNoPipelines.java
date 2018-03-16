/**
 *
 */
package com.netflix.dyno.queues.redis.benchmark;

import com.netflix.dyno.connectionpool.Host;
import com.netflix.dyno.connectionpool.HostSupplier;
import com.netflix.dyno.connectionpool.TokenMapSupplier;
import com.netflix.dyno.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.dyno.connectionpool.impl.lb.HostToken;
import com.netflix.dyno.jedis.DynoJedisClient;
import com.netflix.dyno.queues.ShardSupplier;
import com.netflix.dyno.queues.redis.RedisQueues;

import java.util.*;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * @author Viren
 */
public class BenchmarkTestsNoPipelines extends QueueBenchmark {


    public BenchmarkTestsNoPipelines() {

        String redisKeyPrefix = "perftestnopipe";
        String queueName = "nopipequeue";

        List<Host> hosts = new ArrayList<>(1);
        hosts.add(new Host("localhost", "127.0.0.1", 6379, "us-east-1c", "us-east-1", Host.Status.Up));

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


        }).setLocalRack("us-east-1c").setLocalDataCenter("us-east-1");
        cp.setSocketTimeout(0);
        cp.setConnectTimeout(0);
        cp.setMaxConnsPerHost(10);


        DynoJedisClient client = builder.withApplicationName("test")
                .withDynomiteClusterName("test")
                .withCPConfig(cp)
                .withHostSupplier(hs)
                .build();

        Set<String> allShards = hs.getHosts().stream().map(host -> host.getRack().substring(host.getRack().length() - 2)).collect(Collectors.toSet());
        String shardName = allShards.iterator().next();
        ShardSupplier ss = new ShardSupplier() {

            @Override
            public Set<String> getQueueShards() {
                return allShards;
            }

            @Override
            public String getCurrentShard() {
                return shardName;
            }
        };

        RedisQueues rq = new RedisQueues(client, client, redisKeyPrefix, ss, 60_000, 1_000_000);
        queue = rq.get(queueName);
    }


    public static void main(String[] args) throws Exception {
        try {

            BenchmarkTestsNoPipelines tests = new BenchmarkTestsNoPipelines();
            tests.run();

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            System.exit(0);
        }
    }

}
