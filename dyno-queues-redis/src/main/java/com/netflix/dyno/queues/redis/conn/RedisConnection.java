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