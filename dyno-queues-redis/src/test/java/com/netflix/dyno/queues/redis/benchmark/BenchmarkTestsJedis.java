/**
 * 
 */
package com.netflix.dyno.queues.redis.benchmark;

import java.util.LinkedList;
import java.util.List;

import com.netflix.dyno.connectionpool.Host;

import com.netflix.dyno.queues.redis.v2.QueueBuilder;
import redis.clients.jedis.JedisPoolConfig;

/**
 * @author Viren
 *
 */
public class BenchmarkTestsJedis extends QueueBenchmark {

	public BenchmarkTestsJedis() {
		List<Host> hosts = new LinkedList<>();
		hosts.add(new Host("localhost", 6379, "us-east-1a"));
		QueueBuilder qb = new QueueBuilder();
		
		JedisPoolConfig config = new JedisPoolConfig();
		config.setTestOnBorrow(true);
		config.setTestOnCreate(true);
		config.setMaxTotal(10);
		config.setMaxIdle(5);
		config.setMaxWaitMillis(60_000);
		
		
		queue = qb
				.setCurrentShard("a")			
				.setHostToShardMap((Host h) -> h.getRack().substring(h.getRack().length()-1))
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

		} catch(Exception e) {
			e.printStackTrace();
		}finally {
			System.exit(0);
		}
	}

}
