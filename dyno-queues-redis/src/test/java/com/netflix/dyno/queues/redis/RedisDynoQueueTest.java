/**
 * Copyright 2016 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.dyno.queues.redis;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.util.concurrent.Uninterruptibles;
import com.netflix.dyno.connectionpool.Host;
import com.netflix.dyno.connectionpool.Host.Status;
import com.netflix.dyno.connectionpool.HostSupplier;
import com.netflix.dyno.queues.DynoQueue;
import com.netflix.dyno.queues.Message;
import com.netflix.dyno.queues.ShardSupplier;
import com.netflix.dyno.queues.jedis.JedisMock;

public class RedisDynoQueueTest {

	private static JedisMock dynoClient;

	private static final String queueName = "test_queue";

	private static final String redisKeyPrefix = "testdynoqueues";

	private static RedisDynoQueue rdq;

	private static RedisQueues rq;
	
	private static String messageKey;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {

		HostSupplier hs = new HostSupplier() {
			@Override
			public List<Host> getHosts() {
				List<Host> hosts = new LinkedList<>();
				hosts.add(new Host("ec2-11-22-33-444.compute-0.amazonaws.com", 8102, "us-east-1d", Status.Up));
				return hosts;
			}
		};
		
		dynoClient = new JedisMock();
		
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
		messageKey = redisKeyPrefix + ".MESSAGE." + queueName;
		
		rq = new RedisQueues(dynoClient, dynoClient, redisKeyPrefix, ss, 1_000, 1_000_000);
		DynoQueue rdq1 = rq.get(queueName);
		assertNotNull(rdq1);

		rdq = (RedisDynoQueue)rq.get(queueName);
		assertNotNull(rdq);

		assertEquals(rdq1, rdq); // should be the same instance.		
		
	}

	@Test
	public void testGetName() {
		assertEquals(queueName, rdq.getName());
	}

	@Test
	public void testGetUnackTime() {
		assertEquals(1_000, rdq.getUnackTime());
	}

	@Test
	public void testTimeoutUpdate() {
		
		rdq.clear();
		
		String id = UUID.randomUUID().toString();
		Message msg = new Message(id, "Hello World-" + id);
		msg.setTimeout(100, TimeUnit.MILLISECONDS);
		rdq.push(Arrays.asList(msg));
		
		List<Message> popped = rdq.pop(1, 10, TimeUnit.MILLISECONDS);
		assertNotNull(popped);
		assertEquals(0, popped.size());

		Uninterruptibles.sleepUninterruptibly(500, TimeUnit.MILLISECONDS);
		
		popped = rdq.pop(1, 1, TimeUnit.SECONDS);
		assertNotNull(popped);
		assertEquals(1, popped.size());
		
		boolean updated = rdq.setUnackTimeout(id, 500);
		assertTrue(updated);
		popped = rdq.pop(1, 1, TimeUnit.SECONDS);
		assertNotNull(popped);
		assertEquals(0, popped.size());
		
		Uninterruptibles.sleepUninterruptibly(1000, TimeUnit.MILLISECONDS);
		rdq.processUnacks();
		popped = rdq.pop(1, 1, TimeUnit.SECONDS);
		assertNotNull(popped);
		assertEquals(1, popped.size());
		
		updated = rdq.setUnackTimeout(id, 10_000);	//10 seconds!
		assertTrue(updated);
		rdq.processUnacks();
		popped = rdq.pop(1, 1, TimeUnit.SECONDS);
		assertNotNull(popped);
		assertEquals(0, popped.size());
		
		updated = rdq.setUnackTimeout(id, 0);
		assertTrue(updated);
		rdq.processUnacks();
		popped = rdq.pop(1, 1, TimeUnit.SECONDS);
		assertNotNull(popped);
		assertEquals(1, popped.size());
		
		rdq.ack(id);
		Map<String, Map<String, Long>> size = rdq.shardSizes();
		Map<String, Long> values = size.get("1d");
		long total = values.values().stream().mapToLong(v -> v).sum();
		assertEquals(0, total);
		
		popped = rdq.pop(1, 1, TimeUnit.SECONDS);
		assertNotNull(popped);
		assertEquals(0, popped.size());
	}
	
	@Test
	public void testConcurrency() throws InterruptedException, ExecutionException {
		
		rdq.clear();
		
		final int count = 10_000;
		final AtomicInteger published = new AtomicInteger(0);
		
		ScheduledExecutorService ses = Executors.newScheduledThreadPool(6);
		CountDownLatch publishLatch = new CountDownLatch(1);
		Runnable publisher = new Runnable() {

			@Override
			public void run() {
				List<Message> messages = new LinkedList<>();
				for (int i = 0; i < 10; i++) {
					Message msg = new Message(UUID.randomUUID().toString(), "Hello World-" + i);
					msg.setPriority(new Random().nextInt(98));
					messages.add(msg);
				}
				if(published.get() >= count) {
					publishLatch.countDown();
					return;
				}
				
				published.addAndGet(messages.size());
				rdq.push(messages);
				
			}
		};
		
		for(int p = 0; p < 3; p++) {
			ses.scheduleWithFixedDelay(publisher, 1, 1, TimeUnit.MILLISECONDS);
		}
		publishLatch.await();
		CountDownLatch latch = new CountDownLatch(count);
		List<Message> allMsgs = new CopyOnWriteArrayList<>();
		AtomicInteger consumed = new AtomicInteger(0);
		AtomicInteger counter = new AtomicInteger(0);
		Runnable consumer = new Runnable() {

			@Override
			public void run() {
				if(consumed.get() >= count) {
					return;
				}
				List<Message> popped = rdq.pop(100, 1, TimeUnit.MILLISECONDS);
				allMsgs.addAll(popped);
				consumed.addAndGet(popped.size());			
				popped.stream().forEach(p -> latch.countDown());
				counter.incrementAndGet();
			}
		};
		
		for(int c = 0; c < 2; c++) {
			ses.scheduleWithFixedDelay(consumer, 1, 10, TimeUnit.MILLISECONDS);
		}
		Uninterruptibles.awaitUninterruptibly(latch);
		System.out.println("Consumed: " + consumed.get() + ", all: " + allMsgs.size() + " counter: " + counter.get());
		Set<Message> uniqueMessages = allMsgs.stream().collect(Collectors.toSet());

		assertEquals(count, allMsgs.size());
		assertEquals(count, uniqueMessages.size());
		long start = System.currentTimeMillis();
		List<Message> more = rdq.pop(1, 1, TimeUnit.SECONDS);
		long elapsedTime = System.currentTimeMillis() - start;
		assertTrue(elapsedTime > 1000);
		assertEquals(0, more.size());
		assertEquals(0, rdq.prefetch.get());
		
		ses.shutdownNow();
	}
	
	@Test
	public void testSetTimeout() {
		
		rdq.clear();
		
		Message msg = new Message("x001", "Hello World");
		msg.setPriority(3);
		msg.setTimeout(20_000);
		rdq.push(Arrays.asList(msg));
		
		List<Message> popped = rdq.pop(1, 1, TimeUnit.SECONDS);
		assertTrue(popped.isEmpty());
		
		boolean updated = rdq.setTimeout(msg.getId(), 1);
		assertTrue(updated);
		popped = rdq.pop(1, 1, TimeUnit.SECONDS);
		assertEquals(1, popped.size());
		assertEquals(1, popped.get(0).getTimeout());
		updated = rdq.setTimeout(msg.getId(), 1);
		assertTrue(!updated);
	}
	
	@Test
	public void testAll() {
		
		rdq.clear();
		
		int count = 10;
		List<Message> messages = new LinkedList<>();
		for (int i = 0; i < count; i++) {
			Message msg = new Message("" + i, "Hello World-" + i);
			msg.setPriority(count - i);
			messages.add(msg);
		}
		rdq.push(messages);
		
		messages = rdq.peek(count);

		assertNotNull(messages);
		assertEquals(count, messages.size());
		long size = rdq.size();
		assertEquals(count, size);

		// We did a peek - let's ensure the messages are still around!
		List<Message> messages2 = rdq.peek(count);
		assertNotNull(messages2);
		assertEquals(messages, messages2);

		List<Message> poped = rdq.pop(count, 1, TimeUnit.SECONDS);
		assertNotNull(poped);
		assertEquals(count, poped.size());
		assertEquals(messages, poped);

		Uninterruptibles.sleepUninterruptibly(2, TimeUnit.SECONDS);
		((RedisDynoQueue)rdq).processUnacks();
		
		for (Message msg : messages) {
			Message found = rdq.get(msg.getId());
			assertNotNull(found);
			assertEquals(msg.getId(), found.getId());
			assertEquals(msg.getTimeout(), found.getTimeout());
		}
		assertNull(rdq.get("some fake id"));
		
		List<Message> messages3 = rdq.pop(count, 1, TimeUnit.SECONDS);
		if(messages3.size() < count){
			List<Message> messages4 = rdq.pop(count, 1, TimeUnit.SECONDS);
			messages3.addAll(messages4);
		}
		
		assertNotNull(messages3);
		assertEquals(10, messages3.size());
		assertEquals(messages, messages3);
		assertEquals(10, messages3.stream().map(msg -> msg.getId()).collect(Collectors.toSet()).size());
		messages3.stream().forEach(System.out::println);
		assertTrue(dynoClient.hlen(messageKey) == 10);
		
		for (Message msg : messages3) {
			assertTrue(rdq.ack(msg.getId()));
			assertFalse(rdq.ack(msg.getId()));
		}
		Uninterruptibles.sleepUninterruptibly(2, TimeUnit.SECONDS);
		messages3 = rdq.pop(count, 1, TimeUnit.SECONDS);
		assertNotNull(messages3);
		assertEquals(0, messages3.size());
		
		int max = 10;
		for (Message msg : messages) {
			assertEquals(max, msg.getPriority());
			rdq.remove(msg.getId());
			max--;
		}

		size = rdq.size();
		assertEquals(0, size);
		
		assertTrue(dynoClient.hlen(messageKey) == 0);

	}

	@Before
	public void clear(){
		rdq.clear();
		assertTrue(dynoClient.hlen(messageKey) == 0);
	}
	
	@Test
	public void testClearQueues() {
		rdq.clear();
		int count = 10;
		List<Message> messages = new LinkedList<>();
		for (int i = 0; i < count; i++) {
			Message msg = new Message("x" + i, "Hello World-" + i);
			msg.setPriority(count - i);
			messages.add(msg);
		}
		
		rdq.push(messages);
		assertEquals(count, rdq.size());
		rdq.clear();
		assertEquals(0, rdq.size());

	}

}
