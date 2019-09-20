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
package com.netflix.dyno.queues.shard;

import com.netflix.dyno.connectionpool.Host;
import com.netflix.dyno.connectionpool.HostSupplier;
import com.netflix.dyno.queues.ShardSupplier;

import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @author Viren
 *
 * NOTE: This class is deprecated and should not be used. It still remains for backwards compatibility for legacy applications
 * New applications must use 'ConsistentAWSDynoShardSupplier' or extend 'ConsistentDynoShardSupplier' for non-AWS environments.
 *
 */
@Deprecated
public class DynoShardSupplier implements ShardSupplier {

    private HostSupplier hs;

    private String region;

    private String localRack;

    private Function<String, String> rackToShardMap = rack -> rack.substring(rack.length()-1);

    /**
     * Dynomite based shard supplier.  Keeps the number of shards in parity with the hosts and regions
     * @param hs Host supplier
     * @param region current region
     * @param localRack local rack identifier
     */
    public DynoShardSupplier(HostSupplier hs, String region, String localRack) {
        this.hs = hs;
        this.region = region;
        this.localRack = localRack;
    }

    @Override
    public String getCurrentShard() {
        return rackToShardMap.apply(localRack);
    }

    @Override
    public Set<String> getQueueShards() {
        return hs.getHosts().stream().map(host -> host.getRack()).map(rackToShardMap).collect(Collectors.toSet());
    }

    @Override
    public String getShardForHost(Host host) {
        return rackToShardMap.apply(host.getRack());
    }
}
