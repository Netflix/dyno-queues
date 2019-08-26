/**
 *
 */
package com.netflix.dyno.queues.redis.benchmark;

import com.netflix.dyno.connectionpool.Host;
import com.netflix.dyno.connectionpool.HostBuilder;
import com.netflix.dyno.connectionpool.HostSupplier;
import com.netflix.dyno.connectionpool.TokenMapSupplier;
import com.netflix.dyno.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.dyno.connectionpool.impl.lb.HostToken;
import com.netflix.dyno.jedis.DynoJedisClient;
import com.netflix.dyno.queues.redis.v2.QueueBuilder;

import java.util.*;

/**
 * @author Viren
 */
public class BenchmarkTestsDynoJedis extends QueueBenchmark {

    public BenchmarkTestsDynoJedis() {

        List<Host> hosts = new ArrayList<>(1);
        hosts.add(
                new HostBuilder()
                        .setHostname("localhost")
                        .setIpAddress("127.0.0.1")
                        .setPort(6379)
                        .setRack("us-east-1c")
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


        }).setLocalRack("us-east-1c").setLocalDataCenter("us-east-1");
        cp.setSocketTimeout(0);
        cp.setConnectTimeout(0);
        cp.setMaxConnsPerHost(10);
        cp.withHashtag("{}");

        DynoJedisClient client = builder.withApplicationName("test")
                .withDynomiteClusterName("test")
                .withCPConfig(cp)
                .withHostSupplier(hs)
                .build();


        queue = qb
                .setCurrentShard("a")
                .setHostToShardMap((Host h) -> h.getRack().substring(h.getRack().length() - 1))
                .setQueueName("testq")
                .setRedisKeyPrefix("keyprefix")
                .setUnackTime(60_000)
                .useDynomite(client, client, hs)
                .build();
    }


    public static void main(String[] args) throws Exception {
        try {

            System.out.println("Start");
            BenchmarkTestsDynoJedis tests = new BenchmarkTestsDynoJedis();
            tests.run();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            System.exit(0);
        }
    }

}
