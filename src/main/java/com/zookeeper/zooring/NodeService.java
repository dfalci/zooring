package com.zookeeper.zooring;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.CuratorEventType;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class NodeService {
    private final Logger logger = LoggerFactory.getLogger(NodeService.class);
    private final String connectionString;
    private final RetryPolicy policy;
    private final String id;
    private final String data;

    private CuratorFramework c;
    private CommManager m;
    private final AtomicBoolean ready = new AtomicBoolean(false);

    private String membersPath = "/members";

    private final FingerTable fingerTable;
    private ReadWriteLock lock = new ReentrantReadWriteLock();
    private Runnable serverListCallback;
    private Consumer<ConnectionState> connectedCallback;

    public NodeService(String connectionString, String address){
        this(connectionString, null, null, address);
    }

    public NodeService(String connectionString, RetryPolicy policy, String id, String data){
        this.id = id == null? UUID.randomUUID().toString():id;
        this.data = (data == null)?"":data;
        this.connectionString = connectionString;
        this.policy = policy == null?new ExponentialBackoffRetry(1000, 5):policy;
        this.fingerTable = new FingerTable();
    }

    public NodeService withPath(String path){
        this.membersPath = path;
        return this;
    }

    public NodeService withServerListCallback(Runnable serverListCallback){
        this.serverListCallback = serverListCallback;
        return this;
    }

    public NodeService withConnectedCallback(Consumer<ConnectionState> connectedCallback){
        this.connectedCallback = connectedCallback;
        return this;
    }

    public void disconnect(){
        this.m.close();
    }

    public boolean connect(){
        logger.info("connecting");
        this.c = CuratorFrameworkFactory.newClient(this.connectionString, this.policy);

        this.c.getUnhandledErrorListenable().addListener((String message, Throwable e) ->{
            logger.error("unhandled exception: {}", message, e);
        });

        this.c.getConnectionStateListenable().addListener((f, state)->{
            this.ready.set(state.isConnected());
            logger.debug("{} - NewState : {} - Ready {}", this.id, state, state.isConnected());
            if (this.connectedCallback != null)
                this.connectedCallback.accept(state);
        });

        this.c.getCuratorListenable().addListener((client, event) ->{
            this.ready.set(!event.getType().equals(CuratorEventType.CLOSING));
            logger.debug("{} - Event : {} - Ready {}", this.id, event.getType(), this.ready.get());
        });

        this.c.start();
        try {
            if (!this.c.blockUntilConnected(15000, TimeUnit.MILLISECONDS)) {
                logger.error("COULD NOT CONNECT TO ZOOKEEPER");
                return false;
            }
        }catch(InterruptedException ex){
            logger.error("COULD NOT CONNECT TO ZOOKEEPER");
            return false;
        }
        this.m = new CommManager(c, this.membersPath, this.id, this.data.getBytes(), this::onServerListChanged);
        this.m.start();
        return true;
    }

    private void onServerListChanged(ChildData data){
        this.refreshServerList();
        if (this.serverListCallback!=null)
            try {
                CompletableFuture.runAsync(()-> this.serverListCallback.run(), ForkJoinPool.commonPool());
            }catch(Exception ex){
                logger.error("Error while calling callback", ex);
            }
    }

    private void refreshServerList(){
        this.lock.writeLock().lock();
        try {
            this.fingerTable.fill(this.m.getCurrentMembers());
        }finally {
            this.lock.writeLock().unlock();
        }
    }

    public Set<String> getFingerAddresses(){
        return this.fingerTable.nodes.stream().map(el->el.address).collect(Collectors.toSet());
    }

    public Set<String> getFingerIds(){
        return this.fingerTable.nodes.stream().map(el->el.id).collect(Collectors.toSet());
    }

    public String getNodeAddress(String resourceId){
        if (!this.ready.get())
            throw new IllegalStateException("invalid connection state");

        this.lock.readLock().lock();
        try {
            if (resourceId == null)
                throw new IllegalArgumentException("invalid resouurce");
            NodeAddress n = this.fingerTable.getServer(resourceId);
            if (n == null)
                return null;
            return n.address;
        }finally{
            this.lock.readLock().unlock();
        }
    }

    /**
     * checks if a given payload belongs to the current node
     * @param data
     * @return
     */
    public boolean isCurrentNode(String data){
        return this.data.equals(data);
    }

    /**
     * Given a list of resources, filters those which should not be held in the current instance
     * @param resources
     * @return
     */
    public List<String> checkRemovables(List<String> resources){
        return resources.stream().map(this::getNodeAddress).filter(el->!isCurrentNode(el)).collect(Collectors.toList());
    }

    class NodeAddress{
        String id;
        String address;
        BigInteger rep;

        NodeAddress(Map.Entry<String, byte[]> d){
            this.id = d.getKey();
            this.address = d.getValue()!=null?new String(d.getValue()):null;
            this.rep = RingUtils.fromString(d.getKey());
        }
    }

    class FingerTable{
        private final List<NodeAddress> nodes = new ArrayList<>();

        void fill(Map<String, byte[]> items){
            this.nodes.clear();
            if (items != null || items.size() > 0)
                this.nodes.addAll(items.entrySet().stream().map(NodeAddress::new).collect(Collectors.toList()));
            this.nodes.sort((i1, i2) -> i1.id.compareTo(i2.id));
        }

        NodeAddress getServer(String resourceId){
            BigInteger target = RingUtils.fromString(resourceId);
            if (this.nodes.size() == 0)
                return null;

            if (this.nodes.size() == 1)
                return this.nodes.get(0);

            for(NodeAddress n : this.nodes){
                if (n.rep.compareTo(target) > 0)
                    return n;
            }
            return this.nodes.get(this.nodes.size()-1);
        }
    }
}
