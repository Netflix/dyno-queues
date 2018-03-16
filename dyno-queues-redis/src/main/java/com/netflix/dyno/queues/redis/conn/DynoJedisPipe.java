/**
 *
 */
package com.netflix.dyno.queues.redis.conn;


import com.netflix.dyno.jedis.DynoJedisPipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.params.sortedset.ZAddParams;

/**
 * @author Viren
 */
public class DynoJedisPipe implements Pipe {

    private DynoJedisPipeline pipe;

    private boolean modified;

    public DynoJedisPipe(DynoJedisPipeline pipe) {
        this.pipe = pipe;
        this.modified = false;
    }

    @Override
    public void hset(String key, String field, String value) {
        pipe.hset(key, field, value);
        this.modified = true;

    }

    @Override
    public Response<Long> zadd(String key, double score, String member) {
        this.modified = true;
        return pipe.zadd(key, score, member);
    }

    @Override
    public Response<Long> zadd(String key, double score, String member, ZAddParams zParams) {
        this.modified = true;
        return pipe.zadd(key, score, member, zParams);
    }

    @Override
    public Response<Long> zrem(String key, String member) {
        this.modified = true;
        return pipe.zrem(key, member);
    }

    @Override
    public Response<String> hget(String key, String member) {
        this.modified = true;
        return pipe.hget(key, member);
    }

    @Override
    public Response<Long> hdel(String key, String member) {
        this.modified = true;
        return pipe.hdel(key, member);
    }

    @Override
    public void sync() {
        if (modified) {
            pipe.sync();
            modified = false;
        }
    }

    @Override
    public void close() throws Exception {
        pipe.close();
    }


}
