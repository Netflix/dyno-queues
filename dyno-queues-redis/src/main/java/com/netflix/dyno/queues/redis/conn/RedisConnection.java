/**
 * Copyright 2018 Netflix, Inc.
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
package com.netflix.dyno.queues.redis.conn;

import java.util.Set;

import com.netflix.dyno.jedis.DynoJedisClient;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Tuple;

/**
 * Abstraction of Redis connection.
 *  
 * @author viren
 * <p>
 * The methods are 1-1 proxies from Jedis. See Jedis documentation for the details.
 * </p>
 * @see Jedis
 * @see DynoJedisClient
 */
public interface RedisConnection {

	/**
	 * 
	 * @return Returns the underlying connection resource.  For connection pool, returns the actual connection
	 */
	public RedisConnection getResource();

	public String hget(String messkeyageStoreKey, String member);

	public Long zrem(String key, String member);
	
	public Long hdel(String key, String member);

	public Double zscore(String key, String member);

	public void zadd(String key, double score, String member);

	public void hset(String key, String id, String json);

	public long zcard(String key);

	public void del(String key);

	public Set<String> zrangeByScore(String key, int min, double max, int offset, int count);

	public Set<Tuple> zrangeByScoreWithScores(String key, int min, double max, int offset, int count);
	
	public void close();

	public Pipe pipelined();

}