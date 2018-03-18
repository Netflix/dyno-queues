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
package com.netflix.dyno.queues.redis;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.netflix.servo.DefaultMonitorRegistry;
import com.netflix.servo.MonitorRegistry;
import com.netflix.servo.monitor.BasicCounter;
import com.netflix.servo.monitor.BasicStopwatch;
import com.netflix.servo.monitor.BasicTimer;
import com.netflix.servo.monitor.MonitorConfig;
import com.netflix.servo.monitor.StatsMonitor;
import com.netflix.servo.monitor.Stopwatch;
import com.netflix.servo.stats.StatsConfig;

/**
 * @author Viren
 * Monitoring for the queue, publishes the metrics using  servo
 * https://github.com/Netflix/servo
 */
public class QueueMonitor implements Closeable {

    public BasicTimer peek;

    public BasicTimer ack;

    public BasicTimer size;

    public BasicTimer processUnack;

    public BasicTimer remove;

    public BasicTimer get;

    public StatsMonitor queueDepth;

    public StatsMonitor batchSize;

    public StatsMonitor pop;

    public StatsMonitor push;

    public BasicCounter misses;

    public StatsMonitor prefetch;

    private String queueName;

    private String shardName;

    private ScheduledExecutorService executor;

    private static final String className = QueueMonitor.class.getSimpleName();

    public QueueMonitor(String queueName, String shardName) {

        String totalTagName = "total";
        executor = Executors.newScheduledThreadPool(1);

        this.queueName = queueName;
        this.shardName = shardName;

        peek = new BasicTimer(create("peek"), TimeUnit.MILLISECONDS);
        ack = new BasicTimer(create("ack"), TimeUnit.MILLISECONDS);
        size = new BasicTimer(create("size"), TimeUnit.MILLISECONDS);
        processUnack = new BasicTimer(create("processUnack"), TimeUnit.MILLISECONDS);
        remove = new BasicTimer(create("remove"), TimeUnit.MILLISECONDS);
        get = new BasicTimer(create("get"), TimeUnit.MILLISECONDS);
        misses = new BasicCounter(create("queue_miss"));


        StatsConfig statsConfig = new StatsConfig.Builder().withPublishCount(true).withPublishMax(true).withPublishMean(true).withPublishMin(true).withPublishTotal(true).build();

        queueDepth = new StatsMonitor(create("queueDepth"), statsConfig, executor, totalTagName, true);
        batchSize = new StatsMonitor(create("batchSize"), statsConfig, executor, totalTagName, true);
        pop = new StatsMonitor(create("pop"), statsConfig, executor, totalTagName, true);
        push = new StatsMonitor(create("push"), statsConfig, executor, totalTagName, true);
        prefetch = new StatsMonitor(create("prefetch"), statsConfig, executor, totalTagName, true);

        MonitorRegistry registry = DefaultMonitorRegistry.getInstance();

        registry.register(pop);
        registry.register(push);
        registry.register(peek);
        registry.register(ack);
        registry.register(size);
        registry.register(processUnack);
        registry.register(remove);
        registry.register(get);
        registry.register(queueDepth);
        registry.register(misses);
        registry.register(batchSize);
        registry.register(prefetch);
    }

    private MonitorConfig create(String name) {
        return MonitorConfig.builder(name).withTag("class", className).withTag("shard", shardName).withTag("queueName", queueName).build();
    }

    public Stopwatch start(StatsMonitor sm, int batchCount) {
        int count = (batchCount == 0) ? 1 : batchCount;
        Stopwatch sw = new BasicStopwatch() {

            @Override
            public void stop() {
                super.stop();
                long duration = getDuration(TimeUnit.MILLISECONDS) / count;
                sm.record(duration);
                batchSize.record(count);
            }

        };
        sw.start();
        return sw;
    }

    @Override
    public void close() throws IOException {
        executor.shutdown();
    }
}
