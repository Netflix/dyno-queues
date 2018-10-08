package com.netflix.dyno.queues.redis.sharding;

import com.netflix.dyno.queues.Message;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class RoundRobinStrategy implements ShardingStrategy {

    private final AtomicInteger nextShardIndex = new AtomicInteger(0);

    /**
     * Get shard based on round robin strategy.
     * @param allShards
     * @param message is ignored in round robin strategy
     * @return
     */
    @Override
    public String getNextShard(List<String> allShards, Message message) {
        int index = nextShardIndex.incrementAndGet();
        if (index >= allShards.size()) {
            nextShardIndex.set(0);
            index = 0;
        }
        String shard = allShards.get(index);
        return shard;
    }
}
