/**
 * 
 */
package com.netflix.dyno.queues.redis.conn;

import java.util.Set;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Tuple;

/**
 * @author Viren
 *
 */
public class JedisProxy implements RedisConnection {

	private JedisPool pool;
	
	private Jedis jedis;
	
	public JedisProxy(JedisPool pool) {
		this.pool = pool;
	}
	
	public JedisProxy(Jedis jedis) {
		this.jedis = jedis;
	}
	
	@Override
	public RedisConnection getResource() {
		Jedis jedis = pool.getResource();
		return new JedisProxy(jedis);
	}
	
	@Override
	public void close() {
		jedis.close();		
	}
	
	@Override
	public Pipe pipelined() {
		return new RedisPipe(jedis.pipelined());
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
	public void zadd(String key, double unackScore, String member) {
		jedis.zadd(key, unackScore, member);		
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
