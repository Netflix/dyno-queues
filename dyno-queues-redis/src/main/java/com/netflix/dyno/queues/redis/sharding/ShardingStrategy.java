package com.netflix.dyno.queues.redis.sharding;

import com.netflix.dyno.queues.Message;

import java.util.List;

/**
 * Expose common interface that allow to apply custom sharding strategy.
 */
public interface ShardingStrategy {
    String getNextShard(List<String> allShards, Message message);
}
