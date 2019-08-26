/**
 * Copyright 2016 Netflix, Inc.
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
package com.netflix.dyno.queues.redis;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.netflix.dyno.connectionpool.HostBuilder;
import org.junit.Test;

import com.netflix.dyno.connectionpool.Host;
import com.netflix.dyno.connectionpool.Host.Status;
import com.netflix.dyno.queues.shard.DynoShardSupplier;
import com.netflix.dyno.connectionpool.HostSupplier;

/**
 * @author Viren
 *
 */
public class DynoShardSupplierTest {

	@Test
	public void test(){
		HostSupplier hs = new HostSupplier() {
			@Override
			public List<Host> getHosts() {
				List<Host> hosts = new LinkedList<>();
				hosts.add(
						new HostBuilder()
								.setHostname("host1")
								.setPort(8102)
								.setRack("us-east-1a")
								.setStatus(Host.Status.Up)
								.createHost()
				);
				hosts.add(
						new HostBuilder()
								.setHostname("host1")
								.setPort(8102)
								.setRack("us-east-1b")
								.setStatus(Host.Status.Up)
								.createHost()
				);
				hosts.add(
						new HostBuilder()
								.setHostname("host1")
								.setPort(8102)
								.setRack("us-east-1d")
								.setStatus(Host.Status.Up)
								.createHost()
				);
				
				return hosts;
			}
		};
		DynoShardSupplier supplier = new DynoShardSupplier(hs, "us-east-1", "a");
		String localShard = supplier.getCurrentShard();
		Set<String> allShards = supplier.getQueueShards();
		
		assertNotNull(localShard);
		assertEquals("a", localShard);
		assertNotNull(allShards);
		assertEquals(Arrays.asList("a","b","d").stream().collect(Collectors.toSet()), allShards);		
	}
}
