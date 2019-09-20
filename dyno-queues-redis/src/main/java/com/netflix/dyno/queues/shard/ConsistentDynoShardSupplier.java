package com.netflix.dyno.queues.shard;

import com.netflix.dyno.connectionpool.Host;
import com.netflix.dyno.connectionpool.HostSupplier;
import com.netflix.dyno.queues.ShardSupplier;

import java.util.*;

abstract class ConsistentDynoShardSupplier implements ShardSupplier {

    protected HostSupplier hs;

    protected  String region;

    protected String localRack;

    protected Map<String, String> rackToShardMap;

    /**
     * Dynomite based shard supplier.  Keeps the number of shards in parity with the hosts and regions
     * @param hs Host supplier
     * @param region current region
     * @param localRack local rack identifier
     */
    public ConsistentDynoShardSupplier(HostSupplier hs, String region, String localRack) {
        this.hs = hs;
        this.region = region;
        this.localRack = localRack;
    }

    public void setRackToShardMap(Map<String, String> rackToShardMapEntries) {
        rackToShardMap = new HashMap<>(rackToShardMapEntries);
    }

    @Override
    public String getCurrentShard() {
        return rackToShardMap.get(localRack);
    }

    @Override
    public Set<String> getQueueShards() {
        Set<String> queueShards = new HashSet<>();
        List<Host> hosts = hs.getHosts();
        for (Host host : hosts) {
            queueShards.add(rackToShardMap.get(host.getRack()));
        }
        return queueShards;
    }

    @Override
    public String getShardForHost(Host host) {
        return rackToShardMap.get(host.getRack());
    }
}