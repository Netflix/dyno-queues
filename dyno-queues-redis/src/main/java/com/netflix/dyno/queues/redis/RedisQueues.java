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

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.netflix.dyno.queues.DynoQueue;
import com.netflix.dyno.queues.ShardSupplier;

import redis.clients.jedis.JedisCommands;

/**
 * @author Viren
 *
 * Please note that you should take care for disposing resource related to RedisQueue insatance - that menas you
 * should call close() on RedisQueue instance.
 */
public class RedisQueues implements Closeable {

	private JedisCommands quorumConn;

	private JedisCommands nonQuorumConn;

	private Set<String> allShards;

	private String shardName;

	private String redisKeyPrefix;

	private int unackTime;

	private int unackHandlerIntervalInMS;

	private ConcurrentHashMap<String, DynoQueue> queues;

	/**
	 *
	 * @param quorumConn Dyno connection with dc_quorum enabled
	 * @param nonQuorumConn	Dyno connection to local Redis
	 * @param redisKeyPrefix	prefix applied to the Redis keys
	 * @param shardSupplier	Provider for the shards for the queues created
	 * @param unackTime	Time in millisecond within which a message needs to be acknowledged by the client, after which the message is re-queued.
	 * @param unackHandlerIntervalInMS	Time in millisecond at which the un-acknowledgement processor runs
	 */
	public RedisQueues(JedisCommands quorumConn, JedisCommands nonQuorumConn, String redisKeyPrefix, ShardSupplier shardSupplier, int unackTime,
			int unackHandlerIntervalInMS) {

		this.quorumConn = quorumConn;
		this.nonQuorumConn = nonQuorumConn;
		this.redisKeyPrefix = redisKeyPrefix;
		this.allShards = shardSupplier.getQueueShards();
		this.shardName = shardSupplier.getCurrentShard();
		this.unackTime = unackTime;
		this.unackHandlerIntervalInMS = unackHandlerIntervalInMS;
		this.queues = new ConcurrentHashMap<>();
	}

	/**
	 *
	 * @param queueName Name of the queue
	 * @return Returns the DynoQueue hosting the given queue by name
	 * @see DynoQueue
	 * @see RedisDynoQueue
	 */
	public DynoQueue get(String queueName) {

		String key = queueName.intern();
		DynoQueue queue = this.queues.get(key);
		if (queue != null) {
			return queue;
		}

		synchronized (this) {
			queue = new RedisDynoQueue(redisKeyPrefix, queueName, allShards, shardName, unackHandlerIntervalInMS)
							.withUnackTime(unackTime)
							.withNonQuorumConn(nonQuorumConn)
							.withQuorumConn(quorumConn);
			this.queues.put(key, queue);
		}

		return queue;
	}

	/**
	 *
	 * @return Collection of all the registered queues
	 */
	public Collection<DynoQueue> queues(){
		return this.queues.values();
	}

	@Override
	public void close() throws IOException {
		queues.values().forEach(queue -> {
			try {
				queue.close();
			}
			catch (final IOException e) {
				throw new RuntimeException(e.getCause());
			}
		});
	}
}
