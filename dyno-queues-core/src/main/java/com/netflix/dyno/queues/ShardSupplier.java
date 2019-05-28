/**
 * Copyright 2016 Netflix, Inc.
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
package com.netflix.dyno.queues;

import java.util.Set;


/**
 * @author Viren
 *
 */
public interface ShardSupplier {

    /**
     *
     * @return Provides the set of all the available queue shards.  The elements are evenly distributed amongst these shards
     */
    public Set<String> getQueueShards();

    /**
     *
     * @return Name of the current shard.  Used when popping elements out of the queue
     */
    public String getCurrentShard();

}
