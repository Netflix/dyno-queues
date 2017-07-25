/**
 * 
 */
package com.netflix.dyno.queues.redis;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.netflix.dyno.queues.Message;

import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * @author Viren
 *
 */
public class BenchmarkTests {

	private RedisQueue queue;
	
	public BenchmarkTests() {
		JedisPoolConfig config = new JedisPoolConfig();
		config.setTestOnBorrow(true);
		config.setTestOnCreate(true);
		config.setMaxTotal(10);
		config.setMaxIdle(5);
		config.setMaxWaitMillis(60_000);
		JedisPool pool = new JedisPool(config, "localhost", 6379);
		queue = new RedisQueue("perf", "TEST_QUEUE", "x", 60000_000, pool);
		
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
			int batchSize = 2000;
			int count = 0;
			for(int i = 0; i < loopCount; i++) {
				List<Message> popped = queue.pop(batchSize, 1, TimeUnit.MILLISECONDS);
				queue.ack(popped);
				count += popped.size();
			}
			long e = System.currentTimeMillis();
			long diff = e-s;
			long throughput = 1000 * ((count)/diff); 
			System.out.println("Consume time: " + diff + ", read throughput: " + throughput + " msg/sec, read: " + count);
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
			
			for(int i = 0; i < 20; i++) {
				tests.publish();
				tests.consume();
			}
			
		} finally {
			System.exit(0);
		}
	}

}
