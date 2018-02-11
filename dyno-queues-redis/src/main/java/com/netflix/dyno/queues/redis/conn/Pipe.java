package com.netflix.dyno.queues.redis.conn;

import com.netflix.dyno.jedis.DynoJedisPipeline;

import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.params.sortedset.ZAddParams;

/**
 * 
 * @author Viren
 * <p>
 * Abstraction of Redis Pipeline.  
 * The abstraction is required as there is no common interface between DynoJedisPipeline and Jedis' Pipeline classes.
 * </p>
 * @see DynoJedisPipeline
 * @see Pipeline
 *
 */
public interface Pipe {

	/**
	 * 
	 * @param key
	 * @param field
	 * @param value
	 */
	public void hset(String key, String field, String value);

	/**
	 * 
	 * @param key
	 * @param score
	 * @param member
	 * @return
	 */
	public Response<Long> zadd(String key, double score, String member);

	/**
	 * 
	 * @param key
	 * @param score
	 * @param member
	 * @param zParams
	 * @return
	 */
	public Response<Long> zadd(String key, double score, String member, ZAddParams zParams);

	/**
	 * 
	 * @param key
	 * @param member
	 * @return
	 */
	public Response<Long> zrem(String key, String member);

	/**
	 * 
	 * @param key
	 * @param member
	 * @return
	 */
	public Response<String> hget(String key, String member);

	/**
	 * 
	 * @param key
	 * @param member
	 * @return
	 */
	public Response<Long> hdel(String key, String member);

	/**
	 * 
	 */
	public void sync();

	/**
	 * 
	 * @throws Exception
	 */
	public void close() throws Exception;
}