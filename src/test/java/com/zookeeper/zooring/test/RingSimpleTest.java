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
    private static final Logger logger = LoggerFactory.getLogger(RingSimpleTest.class);

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
            Assertions.assertTrue(s.getNodeAddress(s.getNodeAddress(UUID.randomUUID().toString())).equals("1"));

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
                            latch.countDown();
                        }
                        init.remove(thisId);
                    })
                    .withPath("/members");
            Assertions.assertTrue(s.connect());
            map.put(String.valueOf(address), s);
        }
        latch.await(15, TimeUnit.SECONDS);
        logger.info("all servers are connected");

        final CountDownLatch latch2 = new CountDownLatch(1);
        ForkJoinPool.commonPool().execute(()->{
            while (true){
                if (map.entrySet().stream().anyMatch(el->el.getValue().getFingerAddresses().size() != services)) {
                    try { Thread.sleep(500); } catch (Exception ex) { }
                }else{
                    break;
                }
            }
            latch2.countDown();
        });

        latch2.await(15, TimeUnit.SECONDS);

        //check whether all servers are responding the same server for a given set of random resource ids
        for (int i=0;i<10000;i++) {
            String target = UUID.randomUUID().toString();
            Set<String> targets = map.entrySet().stream().map(el -> el.getValue().getNodeAddress(target)).collect(Collectors.toSet());
            assert targets.size() == 1;
        }
        map.values().forEach(el->el.disconnect());

    }

    @Test
    void testRingNodes(){
        for (int i=0;i<10;i++) {
            System.out.println(RingUtils.fromString(UUID.randomUUID().toString()).toString());
        }

    }



}
