/**
 * 
 */
package com.netflix.dyno.queues.redis;

import java.time.Clock;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import com.netflix.appinfo.AmazonInfo;
import com.netflix.appinfo.AmazonInfo.MetaDataKey;
import com.netflix.appinfo.InstanceInfo;
import com.netflix.appinfo.InstanceInfo.InstanceStatus;
import com.netflix.discovery.EurekaClient;
import com.netflix.discovery.shared.Application;
import com.netflix.dyno.connectionpool.Host;
import com.netflix.dyno.connectionpool.Host.Status;
import com.netflix.dyno.contrib.EurekaHostsSupplier;
import com.netflix.dyno.jedis.DynoJedisClient;
import com.netflix.dyno.queues.DynoQueue;
import com.netflix.dyno.queues.redis.conn.DynoClientProxy;
import com.netflix.dyno.queues.redis.conn.JedisProxy;
import com.netflix.dyno.queues.redis.conn.RedisConnection;

import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * @author Viren
 *
 */
public class QueueBuilder {

	private Clock clock;
	
	private String queueName;
	
	private EurekaClient ec;
	
	private String dynomiteClusterName;
	
	private String redisKeyPrefix;
	
	private int unackTime;
	
	private String currentShard;
	
	private Function<Host, String> hostToShardMap;
	
	private int nonQuorumPort;	
	
	private List<Host> hosts;
	
	private JedisPoolConfig redisPoolConfig;

	/**
	 * @param clock the Clock instance to set
	 * @return instance of QueueBuilder
	 */
	public QueueBuilder setClock(Clock clock) {
		this.clock = clock;
		return this;
	}
	
	/**
	 * @param queueName the queueName to set
	 * @return instance of QueueBuilder
	 */
	public QueueBuilder setQueueName(String queueName) {
		this.queueName = queueName;
		return this;
	}

	/**
	 * @param redisKeyPrefix Prefix used for all the keys in Redis
	 * @return instance of QueueBuilder
	 */
	public QueueBuilder setRedisKeyPrefix(String redisKeyPrefix) {
		this.redisKeyPrefix = redisKeyPrefix;
		return this;
	}
	
	/**
	 * @param ec the ec to set
	 * @return instance of QueueBuilder
	 */
	
	/**
	 * 
	 * @param ec EurekaClient instance used to discover the hosts in the dynomite cluster
	 * @param dynomiteClusterName Name of the Dynomite Cluster to be used
	 * @param nonQuorumPort Direct Redis port used to make non-quorumed queries - used when querying the message counts
	 * @return instance of QueueBuilder
	 */
	public QueueBuilder useDynomiteCluster(EurekaClient ec, String dynomiteClusterName, int nonQuorumPort) {
		this.ec = ec;
		this.dynomiteClusterName = dynomiteClusterName;
		this.nonQuorumPort = nonQuorumPort;
		return this;
	}

	/**
	 * 
	 * @param redisPoolConfig
	 * @return instance of QueueBuilder
	 */
	public QueueBuilder useNonDynomiteRedis(JedisPoolConfig redisPoolConfig, List<Host> redisHosts) {
		this.redisPoolConfig = redisPoolConfig;
		this.hosts = redisHosts;
		return this;
	}
	
	/**
	 * 
	 * @param hostToShardMap Mapping from a Host to a queue shard
	 * @return instance of QueueBuilder
	 */
	public QueueBuilder setHostToShardMap(Function<Host, String> hostToShardMap) {
		this.hostToShardMap = hostToShardMap;
		return this;
	}
	
	/**
	 * @param unackTime Time in millisecond, after which the uncked messages will be re-queued for the delivery 
	 * @return instance of QueueBuilder
	 */
	public QueueBuilder setUnackTime(int unackTime) {
		this.unackTime = unackTime;
		return this;
	}

	/**
	 * @param currentShard Name of the current shard
	 * @return instance of QueueBuilder
	 */
	public QueueBuilder setCurrentShard(String currentShard) {
		this.currentShard = currentShard;
		return this;
	}

	public DynoQueue build() {
		if (clock == null) {
			clock = Clock.systemDefaultZone();
		}
		
		boolean useDynomite = false;
		//When using Dynomite
		if(dynomiteClusterName != null) {
			hosts = getHostsFromEureka(ec, dynomiteClusterName);
			useDynomite = true;
		}
		Map<String, Host> shardMap = new HashMap<>();
		for(Host host : hosts) {
			String shard = hostToShardMap.apply(host);
			shardMap.put(shard, host);
		}
		
		DynoJedisClient dynoClientRead = null;
		DynoJedisClient dynoClient = null;
		if(useDynomite) {
			String appId = queueName;
			EurekaHostsSupplier hostSupplier = new EurekaHostsSupplier(dynomiteClusterName, ec) {
				@Override
				public List<Host> getHosts() {
					List<Host> hosts = super.getHosts();
					List<Host> updatedHosts = new ArrayList<>(hosts.size());
					hosts.forEach(host -> {
						updatedHosts.add(new Host(host.getHostName(), host.getIpAddress(), nonQuorumPort, host.getRack(), host.getDatacenter(), host.isUp() ? Status.Up : Status.Down));
					});
					return updatedHosts;
				}
			};
			
			dynoClientRead = new DynoJedisClient.Builder().withApplicationName(appId).withDynomiteClusterName(dynomiteClusterName).withHostSupplier(hostSupplier).build();
			dynoClient = new DynoJedisClient.Builder().withApplicationName(appId).withDynomiteClusterName(dynomiteClusterName).withDiscoveryClient(ec).build();
		}
		
		
		Map<String, RedisQueue> queues = new HashMap<>();
		for(String queueShard : shardMap.keySet()) {
			
			Host host = shardMap.get(queueShard);
			String hostAddress = host.getIpAddress();
			if(hostAddress == null || "".equals(hostAddress)) {
				hostAddress = host.getHostName();
			}
			RedisConnection redisConn = null;
			RedisConnection redisConnRead = null;
			
			if(useDynomite) {
				redisConn = new DynoClientProxy(dynoClient);
				redisConnRead = new DynoClientProxy(dynoClientRead);
			} else{
				JedisPool pool = new JedisPool(redisPoolConfig, hostAddress, host.getPort(), 0);
				redisConn = new JedisProxy(pool);
				redisConnRead = new JedisProxy(pool);
			}
			
			RedisQueue q = new RedisQueue(clock, redisKeyPrefix, queueName, queueShard, unackTime, unackTime, redisConn);
			q.setNonQuorumPool(redisConnRead);
			
			queues.put(queueShard, q);
		}
		
		if(queues.size() == 1) {
			//This is a queue with a single shard
			return queues.values().iterator().next();
		}
		
		MultiRedisQueue queue = new MultiRedisQueue(queueName, currentShard, queues);
		return queue;
	}

	
	private static List<Host> getHostsFromEureka(EurekaClient ec, String applicationName) {
		
		Application app = ec.getApplication(applicationName);
		List<Host> hosts = new ArrayList<Host>();

		if (app == null) {
			return hosts;
		}

		List<InstanceInfo> ins = app.getInstances();

		if (ins == null || ins.isEmpty()) {
			return hosts;
		}

		hosts = Lists.newArrayList(Collections2.transform(ins,
				
				new Function<InstanceInfo, Host>() {
					@Override
					public Host apply(InstanceInfo info) {
						
						Host.Status status = info.getStatus() == InstanceStatus.UP ? Host.Status.Up : Host.Status.Down;
						String rack = null;
						if (info.getDataCenterInfo() instanceof AmazonInfo) {
							AmazonInfo amazonInfo = (AmazonInfo)info.getDataCenterInfo();
							rack = amazonInfo.get(MetaDataKey.availabilityZone);
						}
						Host host = new Host(info.getHostName(), info.getIPAddr(), rack, status);
						return host;
					}
				}));
		return hosts;
	}
}
