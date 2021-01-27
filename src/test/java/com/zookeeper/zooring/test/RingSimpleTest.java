package com.zookeeper.zooring.test;

import com.zookeeper.zooring.NodeService;
import com.zookeeper.zooring.RingUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class RingSimpleTest {
    private final Logger logger = LoggerFactory.getLogger(RingSimpleTest.class);

    @Test
    void singleNode() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);

        NodeService s = new NodeService("localhost:2181", "1")
            .withServerListCallback(()->latch.countDown())
            .withPath("/members");

        Assertions.assertTrue(s.connect());
        logger.info("Server connected");
        latch.await(15, TimeUnit.SECONDS);

        for (int i=0;i<1000;i++)
            Assertions.assertTrue(s.getServer(s.getServer(UUID.randomUUID().toString())).equals("1"));

    }

    @Test
    void multipleNodes() throws InterruptedException{
        int services = 10;
        final CountDownLatch latch = new CountDownLatch(services);
        Map<String, NodeService> map = new HashMap<>();


        Set<String> init = Collections.newSetFromMap(new ConcurrentHashMap<>());
        for (int address=0;address<services;address++){
            final String thisId = String.valueOf(address);
            init.add(thisId);
            NodeService s = new NodeService("localhost:2181", null, UUID.randomUUID().toString(), thisId)
                    .withServerListCallback(()->{
                        if (init.contains(thisId)) {
                            System.out.println("liberou "+thisId);
                            latch.countDown();
                        }
                        init.remove(thisId);
                    })
                    .withPath("/members");
            Assertions.assertTrue(s.connect());
            map.put(String.valueOf(address), s);
        }
        logger.info("Server connected");
        latch.await(15, TimeUnit.SECONDS);
        logger.info("all servers connected");

        final CountDownLatch latch2 = new CountDownLatch(1);
        ForkJoinPool.commonPool().execute(()->{
            long start = System.currentTimeMillis();
//            map.values().forEach(v->v.);
        });



        String target = UUID.randomUUID().toString();

        Thread.sleep(5000);

        Set<String> targets = map.entrySet().stream().map(el->el.getValue().getServer(target)).collect(Collectors.toSet());
        assert targets.size() == 1;
        map.values().forEach(el->el.disconnect());


//        Map<String, Integer> count = new HashMap<>();
//        map.keySet().forEach(k->count.put(k, 0));
//
//
//        int misses = 0;
//        for (int i=0;i<15000;i++) {
//            String server = String.valueOf(ThreadLocalRandom.current().nextInt(0, services));
//            String resource = UUID.randomUUID().toString();
//            String target = map.get(server).getServer(resource);
//            if (target != null)
//                count.put(target, count.getOrDefault(target, 0)+1);
//            else
//                misses++;
//
//        }
//
//        Assertions.assertFalse(count.values().stream().anyMatch(el->el.equals(0)));
//        Assertions.assertTrue(misses == 0);
    }

    @Test
    void testRingNodes(){
        for (int i=0;i<10;i++){
            System.out.println(RingUtils.fromString(UUID.randomUUID().toString()));
        }
    }



}
