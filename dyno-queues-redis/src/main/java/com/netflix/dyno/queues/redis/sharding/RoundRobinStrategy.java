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
