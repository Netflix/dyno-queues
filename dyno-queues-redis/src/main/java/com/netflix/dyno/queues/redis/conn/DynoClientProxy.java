/**
 * 
 */
package com.netflix.dyno.queues.redis.conn;

import java.util.Set;

import com.netflix.dyno.jedis.DynoJedisClient;

import redis.clients.jedis.Tuple;

/**
 * @author Viren
 *
 */
public class DynoClientProxy implements RedisConnection {

	private DynoJedisClient jedis;
	
	
	public DynoClientProxy(DynoJedisClient jedis) {
		this.jedis = jedis;
	}
	
	@Override
	public RedisConnection getResource() {
		return this;
	}
	
	@Override
	public void close() {
		//nothing!		
	}
	
	@Override
	public Pipe pipelined() {
		return new DynoJedisPipe(jedis.pipelined());
	}

	@Override
	public String hget(String key, String member) {
		return jedis.hget(key, member);
	}

	@Override
	public Long zrem(String key, String member) {
		return jedis.zrem(key, member);
	}

	@Override
	public Long hdel(String key, String member) {
		return jedis.hdel(key, member);
		
	}

	@Override
	public Double zscore(String key, String member) {
		return jedis.zscore(key, member);
	}

	@Override
	public void zadd(String key, double score, String member) {
		jedis.zadd(key, score, member);		
	}

	@Override
	public void hset(String key, String member, String json) {
		jedis.hset(key, member, json);		
	}

	@Override
	public long zcard(String key) {
		return jedis.zcard(key);
	}

	@Override
	public void del(String key) {
		jedis.del(key);		
	}

	@Override
	public Set<String> zrangeByScore(String key, int min, double max, int offset, int count) {		
		return jedis.zrangeByScore(key, min, max, offset, count);
	}

	@Override
	public Set<Tuple> zrangeByScoreWithScores(String key, int min, double max, int offset, int count) {
		return jedis.zrangeByScoreWithScores(key, min, max, offset, count);
	}

}
