package com.netflix.dyno.queues.shard;

import com.netflix.dyno.connectionpool.HostSupplier;

import java.util.HashMap;
import java.util.Map;

public class ConsistentAWSDynoShardSupplier extends ConsistentDynoShardSupplier {

    /**
     * Dynomite based shard supplier. Keeps the number of shards in parity with the hosts and regions
     *
     * Note: This ensures that all racks use the same shard names. This fixes issues with the now deprecated DynoShardSupplier
     * that would write to the wrong shard if there are cross-region writers/readers.
     *
     * @param hs Host supplier
     * @param region current region
     * @param localRack local rack identifier
     */
    public ConsistentAWSDynoShardSupplier(HostSupplier hs, String region, String localRack) {
        super(hs, region, localRack);
        Map<String, String> rackToHashMapEntries = new HashMap<String, String>() {{
            this.put("us-east-1c", "c");
            this.put("us-east-1d", "d");
            this.put("us-east-1e", "e");

            this.put("eu-west-1a", "c");
            this.put("eu-west-1b", "d");
            this.put("eu-west-1c", "e");

            this.put("us-west-2a", "c");
            this.put("us-west-2b", "d");
            this.put("us-west-2c", "e");
        }};
        setRackToShardMap(rackToHashMapEntries);
    }
}