package com.netflix.dyno.queues.redis.benchmark;

import com.netflix.dyno.queues.DynoQueue;
import com.netflix.dyno.queues.Message;
import com.netflix.dyno.queues.redis.RedisQueue;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public abstract class QueueBenchmark {

    protected DynoQueue queue;

    public void publish() {

        long s = System.currentTimeMillis();
        int loopCount = 100;
        int batchSize = 3000;
        for (int i = 0; i < loopCount; i++) {
            List<Message> messages = new ArrayList<>(batchSize);
            for (int k = 0; k < batchSize; k++) {
                String id = UUID.randomUUID().toString();
                Message message = new Message(id, getPayload());
                messages.add(message);
            }
            queue.push(messages);
        }
        long e = System.currentTimeMillis();
        long diff = e - s;
        long throughput = 1000 * ((loopCount * batchSize) / diff);
        System.out.println("Publish time: " + diff + ", throughput: " + throughput + " msg/sec");
    }

    public void consume() {
        try {
            Set<String> ids = new HashSet<>();
            long s = System.currentTimeMillis();
            int loopCount = 100;
            int batchSize = 3500;
            int count = 0;
            for (int i = 0; i < loopCount; i++) {
                List<Message> popped = queue.pop(batchSize, 1, TimeUnit.MILLISECONDS);
                queue.ack(popped);
                Set<String> poppedIds = popped.stream().map(Message::getId).collect(Collectors.toSet());
                if (popped.size() != poppedIds.size()) {
                    //We consumed dups
                    throw new RuntimeException("Count does not match.  expected:  " + popped.size() + ", but actual was : " + poppedIds.size() + ", i: " + i);
                }
                ids.addAll(poppedIds);
                count += popped.size();
            }
            long e = System.currentTimeMillis();
            long diff = e - s;
            long throughput = 1000 * ((count) / diff);
            if (count != ids.size()) {
                //We consumed dups
                throw new RuntimeException("There were duplicate messages consumed... expected messages to be consumed " + count + ", but actual was : " + ids.size());
            }
            System.out.println("Consume time: " + diff + ", read throughput: " + throughput + " msg/sec, messages read: " + count);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private String getPayload() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 1; i++) {
            sb.append(UUID.randomUUID().toString());
            sb.append(",");
        }
        return sb.toString();
    }

    public void run() throws Exception {

        ExecutorService es = Executors.newFixedThreadPool(2);
        List<Future<Void>> futures = new LinkedList<>();
        for (int i = 0; i < 2; i++) {
            Future<Void> future = es.submit(() -> {
                publish();
                consume();
                return null;
            });
            futures.add(future);
        }
        for (Future<Void> future : futures) {
            future.get();
        }
    }
}
