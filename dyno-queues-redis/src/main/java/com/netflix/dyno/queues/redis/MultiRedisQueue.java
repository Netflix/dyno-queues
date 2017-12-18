/**
 * Copyright 2017 Netflix, Inc.
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

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import com.netflix.appinfo.AmazonInfo;
import com.netflix.appinfo.AmazonInfo.MetaDataKey;
import com.netflix.appinfo.InstanceInfo;
import com.netflix.appinfo.InstanceInfo.InstanceStatus;
import com.netflix.discovery.EurekaClient;
import com.netflix.discovery.shared.Application;
import com.netflix.dyno.connectionpool.Host;
import com.netflix.dyno.queues.DynoQueue;
import com.netflix.dyno.queues.Message;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.io.IOException;
import java.time.Clock;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * @author Viren
 *
 */
public class MultiRedisQueue implements DynoQueue {

	private List<String> shards;

	private String name;

	private Map<String, RedisQueue> queues = new HashMap<>();
	
	private RedisQueue me;

	public MultiRedisQueue(String queueName, String shardName, Map<String, RedisQueue> queues) {
		this.name = queueName;
		this.queues = queues;
		this.me = queues.get(shardName);
		if(me == null) {
			throw new IllegalArgumentException("List of shards supplied (" + queues.keySet() + ") does not contain current shard name: " + shardName);
		}
		this.shards = queues.keySet().stream().collect(Collectors.toList());
	}

	@Override
	public String getName() {
		return name;
	}

	@Override
	public int getUnackTime() {
		return me.getUnackTime();
	}

	@Override
	public List<String> push(List<Message> messages) {
		int size = queues.size();
		int partitionSize = messages.size()/size;
		List<String> ids = new LinkedList<>();
		
		for(int i = 0; i < size-1; i++) {
			RedisQueue queue = queues.get(getNextShard());
			int start = i * partitionSize;
			int end = start + partitionSize;
			ids.addAll(queue.push(messages.subList(start, end)));
		}
		RedisQueue queue = queues.get(getNextShard());
		int start = (size-1) * partitionSize;
		
		ids.addAll(queue.push(messages.subList(start, messages.size())));
		return ids;
	}

	@Override
	public List<Message> pop(int messageCount, int wait, TimeUnit unit) {
		return me.pop(messageCount, wait, unit);
	}

	@Override
	public List<Message> peek(int messageCount) {
		return me.peek(messageCount);
	}

	@Override
	public boolean ack(String messageId) {
		for(DynoQueue q : queues.values()) {
			if(q.ack(messageId)) {
				return true;
			}
		}
		return false;
	}

	@Override
	public void ack(List<Message> messages) {
		Map<String, List<Message>> byShard = messages.stream().collect(Collectors.groupingBy(Message::getShard));
		for(Entry<String, List<Message>> e: byShard.entrySet()) {
			queues.get(e.getKey()).ack(e.getValue());
		}
	}

	@Override
	public boolean setUnackTimeout(String messageId, long timeout) {
		for(DynoQueue q : queues.values()) {
			if(q.setUnackTimeout(messageId, timeout)) {
				return true;
			}
		}
		return false;
	}

	@Override
	public boolean setTimeout(String messageId, long timeout) {
		for(DynoQueue q : queues.values()) {
			if(q.setTimeout(messageId, timeout)) {
				return true;
			}
		}
		return false;
	}

	@Override
	public boolean remove(String messageId) {
		for(DynoQueue q : queues.values()) {
			if(q.remove(messageId)) {
				return true;
			}
		}
		return false;
	}

	@Override
	public Message get(String messageId) {
		for(DynoQueue q : queues.values()) {
			Message msg = q.get(messageId);
			if(msg != null) {
				return msg;
			}
		}
		return null;
	}

	@Override
	public long size() {
		long size = 0;
		for(DynoQueue q : queues.values()) {
			size += q.size();
		}
		return size;
	}

	@Override
	public Map<String, Map<String, Long>> shardSizes() {
		Map<String, Map<String, Long>> sizes = new HashMap<>();
		for(Entry<String, RedisQueue> e : queues.entrySet()) {
			sizes.put(e.getKey(), e.getValue().shardSizes().get(e.getKey()));
		}
		return sizes;
	}

	@Override
	public void clear() {
		for(DynoQueue q : queues.values()) {
			q.clear();
		}
		
	}

	@Override
	public void close() throws IOException {
		for(RedisQueue queue : queues.values()) {
			queue.close();
		}
	}
	
	public void processUnacks() {
		for(RedisQueue queue : queues.values()) {
			queue.processUnacks();
		}
	}
	
	private AtomicInteger nextShardIndex = new AtomicInteger(0);

	private String getNextShard() {
		int indx = nextShardIndex.incrementAndGet();
		if (indx >= shards.size()) {
			nextShardIndex.set(0);
			indx = 0;
		}
		String s = shards.get(indx);
		return s;
	}


	public static class Builder {

		private Clock clock;
		
		private String queueName;
		
		private EurekaClient ec;
		
		private String dynomiteClusterName;
		
		private String redisKeyPrefix;
		
		private int unackTime;
		
		private String currentShard;
		
		private Function<Host, String> hostToShardMap;
		
		private int redisPoolSize;
		
		private int quorumPort;
		
		private int nonQuorumPort;	
		
		private List<Host> hosts;

		/**
		 * @param clock the Clock instance to set
		 * @return instance of builder
		 */
		public Builder setClock(Clock clock) {
			this.clock = clock;
			return this;
		}
		
		/**
		 * @param queueName the queueName to set
		 * @return instance of builder
		 */
		public Builder setQueueName(String queueName) {
			this.queueName = queueName;
			return this;
		}

		/**
		 * @param ec the ec to set
		 * @return instance of builder
		 */
		public Builder setEc(EurekaClient ec) {
			this.ec = ec;
			return this;
		}

		/**
		 * @param dynomiteClusterName the dynomiteClusterName to set
		 * @return instance of builder
		 */
		public Builder setDynomiteClusterName(String dynomiteClusterName) {
			this.dynomiteClusterName = dynomiteClusterName;
			return this;
		}

		/**
		 * @param redisKeyPrefix the redisKeyPrefix to set
		 * @return instance of builder
		 */
		public Builder setRedisKeyPrefix(String redisKeyPrefix) {
			this.redisKeyPrefix = redisKeyPrefix;
			return this;
		}

		/**
		 * @param unackTime the unackTime to set
		 * @return instance of builder
		 */
		public Builder setUnackTime(int unackTime) {
			this.unackTime = unackTime;
			return this;
		}

		/**
		 * @param currentShard the currentShard to set
		 * @return instance of builder
		 */
		public Builder setCurrentShard(String currentShard) {
			this.currentShard = currentShard;
			return this;
		}

		/**
		 * @param hostToShardMap the hostToShardMap to set
		 * @return instance of builder
		 */
		public Builder setHostToShardMap(Function<Host, String> hostToShardMap) {
			this.hostToShardMap = hostToShardMap;
			return this;
		}

		/**
		 * @param redisPoolSize the redisPoolSize to set
		 * @return instance of builder
		 */
		public Builder setRedisPoolSize(int redisPoolSize) {
			this.redisPoolSize = redisPoolSize;
			return this;
		}

		/**
		 * @param quorumPort the quorumPort to set
		 * @return instance of builder
		 */
		public Builder setQuorumPort(int quorumPort) {
			this.quorumPort = quorumPort;
			return this;
		}

		/**
		 * @param nonQuorumPort the nonQuorumPort to set
		 * @return instance of builder
		 */
		public Builder setNonQuorumPort(int nonQuorumPort) {
			this.nonQuorumPort = nonQuorumPort;
			return this;
		}
		
		public Builder setHosts(List<Host> hosts) {
			this.hosts = hosts;
			return this;
		}

		public MultiRedisQueue build() {
			if (clock == null) {
				clock = Clock.systemDefaultZone();
			}
			if(hosts == null) {
				hosts = getHostsFromEureka(ec, dynomiteClusterName);
			}
			Map<String, Host> shardMap = new HashMap<>();
			for(Host host : hosts) {
				String shard = hostToShardMap.apply(host);
				shardMap.put(shard, host);
			}
			
			JedisPoolConfig config = new JedisPoolConfig();
			config.setTestOnBorrow(true);
			config.setTestOnCreate(true);
			config.setMaxTotal(redisPoolSize);
			config.setMaxIdle(5);
			config.setMaxWaitMillis(60_000);

			Map<String, RedisQueue> queues = new HashMap<>();
			for(String queueShard : shardMap.keySet()) {
				String host = shardMap.get(queueShard).getIpAddress();
				
				JedisPool pool = new JedisPool(config, host, quorumPort, 0);
				JedisPool readPool = new JedisPool(config, host, nonQuorumPort, 0);

				RedisQueue q = new RedisQueue(clock, redisKeyPrefix, queueName, queueShard, unackTime, unackTime, pool);
				q.setNonQuorumPool(readPool);
				queues.put(queueShard, q);
			}
			MultiRedisQueue queue = new MultiRedisQueue(queueName, currentShard, queues);
			return queue;
		}

		private static List<Host> getHostsFromEureka(EurekaClient ec, String applicationName) {
			
			Application app = ec.getApplication(applicationName);
			List<Host> hosts = new ArrayList<Host>();

			if (app == null) {
				return hosts;
			}

			List<InstanceInfo> ins = app.getInstances();

			if (ins == null || ins.isEmpty()) {
				return hosts;
			}

			hosts = Lists.newArrayList(Collections2.transform(ins,
					
					new Function<InstanceInfo, Host>() {
						@Override
						public Host apply(InstanceInfo info) {
							
							Host.Status status = info.getStatus() == InstanceStatus.UP ? Host.Status.Up : Host.Status.Down;
							String rack = null;
							if (info.getDataCenterInfo() instanceof AmazonInfo) {
								AmazonInfo amazonInfo = (AmazonInfo)info.getDataCenterInfo();
								rack = amazonInfo.get(MetaDataKey.availabilityZone);
							}
							Host host = new Host(info.getHostName(), info.getIPAddr(), rack, status);
							return host;
						}
					}));
			return hosts;
		}
	}
	
	
}
