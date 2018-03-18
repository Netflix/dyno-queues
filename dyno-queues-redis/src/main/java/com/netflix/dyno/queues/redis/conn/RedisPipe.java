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
/**
 * 
 */
package com.netflix.dyno.queues.redis.conn;

import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.params.sortedset.ZAddParams;

/**
 * @author Viren
 *
 * Pipeline abstraction for direct redis connection - when not using Dynomite.
 */
public class RedisPipe implements Pipe {

	private Pipeline pipe;
	
	public RedisPipe(Pipeline pipe) {
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
