/**
 * 
 */
package com.netflix.dyno.queues.redis;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.netflix.dyno.connectionpool.Host;
import com.netflix.dyno.queues.DynoQueue;
import com.netflix.dyno.queues.Message;

import redis.clients.jedis.JedisPoolConfig;

/**
 * @author Viren
 *
 */
public class BenchmarkTests {

	private DynoQueue queue;
	
	public BenchmarkTests() {
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
	
	public void publish() {
		
		long s = System.currentTimeMillis();
		int loopCount = 100;
		int batchSize = 3000;
		for(int i = 0; i < loopCount; i++) {
			List<Message> messages = new ArrayList<>(batchSize);
			for(int k = 0; k < batchSize; k++) {
				String id = UUID.randomUUID().toString();
				Message message = new Message(id, getPayload());
				messages.add(message);
			}			
			queue.push(messages);
		}
		long e = System.currentTimeMillis();
		long diff = e-s;
		long throughput = 1000 * ((loopCount * batchSize)/diff); 
		System.out.println("Publish time: " + diff + ", throughput: " + throughput + " msg/sec");
	}
	
	public void consume() {
		try {

			long s = System.currentTimeMillis();
			int loopCount = 100;
			int batchSize = 3500;
			int count = 0;
			for(int i = 0; i < loopCount; i++) {
				List<Message> popped = queue.pop(batchSize, 1, TimeUnit.MILLISECONDS);
				queue.ack(popped);
				count += popped.size();
			}
			long e = System.currentTimeMillis();
			long diff = e-s;
			long throughput = 1000 * ((count)/diff); 
			System.out.println("Consume time: " + diff + ", read throughput: " + throughput + " msg/sec, messages read: " + count);
		}catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	private String getPayload() {
		StringBuilder sb = new StringBuilder();
		for(int i = 0; i < 1; i++) {
			sb.append(UUID.randomUUID().toString());
			sb.append(",");
		}
		return sb.toString();
	}
	
	public static void main(String[] args) throws Exception {
		try {
			
			BenchmarkTests tests = new BenchmarkTests();
			tests.publish();
			tests.consume();			
		} catch(Exception e) {
			e.printStackTrace();
		}finally {
			System.exit(0);
		}
	}

}
