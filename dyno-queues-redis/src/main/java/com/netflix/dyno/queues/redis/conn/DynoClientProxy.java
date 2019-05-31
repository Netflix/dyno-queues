/**
 * Copyright 2018 Netflix, Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
 * Dynomite connection
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
