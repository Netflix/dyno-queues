package com.netflix.dyno.queues.demo;

import com.netflix.dyno.demo.redis.DynoJedisDemo;
import com.netflix.dyno.jedis.DynoJedisClient;
import com.netflix.dyno.queues.DynoQueue;
import com.netflix.dyno.queues.Message;
import com.netflix.dyno.queues.redis.v2.QueueBuilder;
import com.netflix.dyno.queues.shard.DynoShardSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
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
     */
    public static void main(String[] args) throws IOException {
        final String clusterName = args[0];

        final DynoQueueDemo demo = new DynoQueueDemo(clusterName, "us-east-1e");
        int numCounters = (args.length == 2) ? Integer.valueOf(args[1]) : 1;
        Properties props = new Properties();
        props.load(DynoQueueDemo.class.getResourceAsStream("/demo.properties"));
        for (String name : props.stringPropertyNames()) {
            System.setProperty(name, props.getProperty(name));
        }

        try {
            demo.initWithRemoteClusterFromEurekaUrl(args[0], 8102);

            demo.runSimpleV2QueueDemo(demo.client);
            Thread.sleep(10000);

        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            demo.stop();
            logger.info("Done");
        }
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
