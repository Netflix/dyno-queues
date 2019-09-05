/**
 *
 */
package com.netflix.dyno.queues.redis.benchmark;

import com.netflix.dyno.connectionpool.Host;
import com.netflix.dyno.connectionpool.HostBuilder;

import com.netflix.dyno.queues.redis.v2.QueueBuilder;
import redis.clients.jedis.JedisPoolConfig;

import java.util.LinkedList;
import java.util.List;

/**
 * @author Viren
 *
 */
public class BenchmarkTestsJedis extends QueueBenchmark {

    public BenchmarkTestsJedis() {
        List<Host> hosts = new LinkedList<>();
		hosts.add(
				new HostBuilder()
						.setHostname("localhost")
						.setPort(6379)
						.setRack("us-east-1a")
						.createHost()
		);

        QueueBuilder qb = new QueueBuilder();

        JedisPoolConfig config = new JedisPoolConfig();
        config.setTestOnBorrow(true);
        config.setTestOnCreate(true);
        config.setMaxTotal(10);
        config.setMaxIdle(5);
        config.setMaxWaitMillis(60_000);


        queue = qb
                .setCurrentShard("a")
                .setQueueName("testq")
                .setRedisKeyPrefix("keyprefix")
                .setUnackTime(60_000_000)
                .useNonDynomiteRedis(config, hosts)
                .build();

        System.out.println("Instance: " + queue.getClass().getName());
    }

    public static void main(String[] args) throws Exception {
        try {

            BenchmarkTestsJedis tests = new BenchmarkTestsJedis();
            tests.run();

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            System.exit(0);
        }
    }
}
