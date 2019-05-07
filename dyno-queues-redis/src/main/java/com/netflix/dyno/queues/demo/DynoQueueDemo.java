package com.netflix.dyno.queues.demo;

import com.google.common.collect.ImmutableList;
import com.netflix.dyno.demo.redis.DynoJedisDemo;
import com.netflix.dyno.jedis.DynoJedisClient;
import com.netflix.dyno.queues.redis.RedisQueues;
import com.netflix.dyno.queues.DynoQueue;
import com.netflix.dyno.queues.Message;
import com.netflix.dyno.queues.redis.v2.QueueBuilder;
import com.netflix.dyno.queues.shard.DynoShardSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Time;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class DynoQueueDemo extends DynoJedisDemo {

    private static final Logger logger = LoggerFactory.getLogger(DynoQueue.class);
    public DynoQueueDemo(String clusterName, String localRack) {
        super(clusterName, localRack);
    }

    public DynoQueueDemo(String primaryCluster, String shadowCluster, String localRack) {
        super(primaryCluster, shadowCluster, localRack);
    }

    /**
     * Provide the cluster name to connect to as an argument to the function.
     * throws java.lang.RuntimeException: java.net.ConnectException: Connection timed out (Connection timed out)
     * if the cluster is not reachable.
     *
     * @param args:
     * <cluster-name> <version>
     *
     * cluster-name: Name of cluster to run demo against
     * version: Possible values = 1 or 2; (for V1 or V2)
     *
     */
    public static void main(String[] args) throws IOException {
        final String clusterName = args[0];

        if (args.length < 2) {
            throw new IllegalArgumentException("Need to pass in cluster-name and version of dyno-queues to run as arguments");
        }

        int version = Integer.parseInt(args[1]);
        final DynoQueueDemo demo = new DynoQueueDemo(clusterName, "us-east-1e");
        Properties props = new Properties();
        props.load(DynoQueueDemo.class.getResourceAsStream("/demo.properties"));
        for (String name : props.stringPropertyNames()) {
            System.setProperty(name, props.getProperty(name));
        }

        try {
            demo.initWithRemoteClusterFromEurekaUrl(args[0], 8102);

            if (version == 1) {
                demo.runSimpleV1Demo(demo.client);
            } else if (version == 2) {
                demo.runSimpleV2QueueDemo(demo.client);
            }
            Thread.sleep(10000);

        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            demo.stop();
            logger.info("Done");
        }
    }

    private void runSimpleV1Demo(DynoJedisClient dyno) throws IOException {
        String region = System.getProperty("LOCAL_DATACENTER");
        String localRack = System.getProperty("LOCAL_RACK");

        String prefix = "dynoQueue_";

        DynoShardSupplier ss = new DynoShardSupplier(dyno.getConnPool().getConfiguration().getHostSupplier(), region, localRack);

        RedisQueues queues = new RedisQueues(dyno, dyno, prefix, ss, 50_000, 50_000);

        Message msg1 = new Message("id1", "searchable payload");
        Message msg2 = new Message("id2", "payload 2");
        Message msg3 = new Message("id2", "payload 3");
        DynoQueue V1Queue = queues.get("simpleQueue");

        // Test push() API
        //List pushed_msgs = V1Queue.push(Arrays.asList(msg1));
        List pushed_msgs = V1Queue.push(ImmutableList.of(msg1, msg2, msg3));

        // Test ensure() API
        System.out.println("Does Message with ID '" + msg1.getId() + "' already exist? " + !V1Queue.ensure(msg1));

        // Test containsPredicate() API
        System.out.println("Does the predicate 'searchable' exist in  the queue ? " + V1Queue.containsPredicate("searchable"));

        // Test pop()
        List<Message> popped_msgs = V1Queue.pop(3, 1000, TimeUnit.MILLISECONDS);

        // Test ack()
        assert(V1Queue.ack(popped_msgs.get(0).getId()));

        V1Queue.close();
    }

    private void runSimpleV2QueueDemo(DynoJedisClient dyno) throws IOException {
        String region = System.getProperty("LOCAL_DATACENTER");
        String localRack = System.getProperty("LOCAL_RACK");

        String prefix = "dynoQueue_";


        DynoShardSupplier ss = new DynoShardSupplier(dyno.getConnPool().getConfiguration().getHostSupplier(), region, localRack);

        DynoQueue queue = new QueueBuilder()
                .setQueueName("test")
                .setCurrentShard(ss.getCurrentShard())
                .setRedisKeyPrefix(prefix)
                .useDynomite(dyno, dyno, dyno.getConnPool().getConfiguration().getHostSupplier())
                .setUnackTime(50_000)
                .build();

        Message msg = new Message("id1", "message payload");
        queue.push(Arrays.asList(msg));

        int count = 10;
        List<Message> polled = queue.pop(count, 1, TimeUnit.SECONDS);
        logger.info(polled.toString());

        queue.ack("id1");
        queue.close();
    }
}
