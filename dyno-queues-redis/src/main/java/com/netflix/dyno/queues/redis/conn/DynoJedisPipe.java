/**
 * 
 */
package com.netflix.dyno.queues.redis.conn;

import com.netflix.dyno.jedis.DynoJedisPipeline;

import redis.clients.jedis.Response;
import redis.clients.jedis.params.sortedset.ZAddParams;

/**
 * @author Viren
 *
 */
public class DynoJedisPipe implements Pipe {

	private DynoJedisPipeline pipe;
	
	public DynoJedisPipe(DynoJedisPipeline pipe) {
		this.pipe = pipe;
	}

	@Override
	public void hset(String key, String field, String value) {
		pipe.hset(key, field, value);
		
	}

	@Override
	public Response<Long> zadd(String key, double score, String member) {
		return pipe.zadd(key, score, member);		
	}

	@Override
	public Response<Long> zadd(String key, double score, String member, ZAddParams zParams) {
		return pipe.zadd(key, score, member, zParams);
	}

	@Override
	public Response<Long> zrem(String key, String member) {
		return pipe.zrem(key, member);
	}

	@Override
	public Response<String> hget(String key, String member) {
		return pipe.hget(key, member);
	}

	@Override
	public Response<Long> hdel(String key, String member) {
		return pipe.hdel(key, member);
	}

	@Override
	public void sync() {
		pipe.sync();		
	}

	@Override
	public void close() throws Exception {
		pipe.close();		
	}
	

}
