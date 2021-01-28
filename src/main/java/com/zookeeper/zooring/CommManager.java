package com.zookeeper.zooring;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.CuratorCache;
import org.apache.curator.framework.recipes.cache.CuratorCacheBridge;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;
import org.apache.curator.framework.recipes.nodes.PersistentNode;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.utils.ThreadUtils;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Consumer;

import static org.apache.curator.framework.recipes.cache.CuratorCacheAccessor.parentPathFilter;

public class CommManager implements Closeable{
    private final Logger logger = LoggerFactory.getLogger(CommManager.class);
    private final PersistentNode pen;
    private final CuratorCacheBridge cache;
    private final String membershipPath;
    private final String thisId;
    private final Consumer<ChildData> onUpdated;

    public CommManager(CuratorFramework client, String membershipPath, String thisId, Consumer<ChildData> onUpdated){
        this(client, membershipPath, thisId, CuratorFrameworkFactory.getLocalAddress(), onUpdated);
    }

    public CommManager(CuratorFramework client, String membershipPath, String thisId, byte[] payload, Consumer<ChildData> onUpdated){
        this.onUpdated = onUpdated;
        this.membershipPath = membershipPath;
        this.thisId = Preconditions.checkNotNull(thisId, "thisId cannot be null");

        cache = CuratorCache.bridgeBuilder(client, membershipPath).build();
        cache.listenable().addListener(this::onDataUpdated);
        pen = new PersistentNode(client, CreateMode.EPHEMERAL, false, ZKPaths.makePath(membershipPath, thisId), payload);
    }

    public void onDataUpdated(CuratorCacheListener.Type type, ChildData oldData, ChildData data) {
        logger.info("{} - {} - {}", type, oldData, data);
        this.onUpdated.accept(data);
    }

    public void start(){
        pen.start();
        try{
            cache.start();
        }catch ( Exception e ){
            ThreadUtils.checkInterrupted(e);
            Throwables.propagate(e);
        }
    }

    public void setThisData(byte[] data){
        try{
            pen.setData(data);
        }catch ( Exception e ){
            ThreadUtils.checkInterrupted(e);
            Throwables.propagate(e);
        }
    }

    public void close(){
        CloseableUtils.closeQuietly(cache);
        CloseableUtils.closeQuietly(pen);
    }

    public Map<String, byte[]> getCurrentMembers(){
        ImmutableMap.Builder<String, byte[]> builder = ImmutableMap.builder();
        boolean thisIdAdded = false;

        Iterator<ChildData> iterator = cache.stream().filter(parentPathFilter(membershipPath)).iterator();
        while ( iterator.hasNext() ){
            ChildData data = iterator.next();
            String id = idFromPath(data.getPath());
            thisIdAdded = thisIdAdded || id.equals(thisId);
            builder.put(id, data.getData());
        }
        if ( !thisIdAdded )
            builder.put(thisId, pen.getData());
        return builder.build();
    }

    public String idFromPath(String path){
        return ZKPaths.getNodeFromPath(path);
    }
}