# ZOORing

Minimal implementation of a DHT ring (Distributed Hash Table) of instances using Apache Zookeeper + Apache Curator. Used for sticky routing requests relying on resource ids.

The eventually consistent process verifies the currently connected nodes from the ring and assigns one of them as the responsible for any resource request. It monitors the node's availability across the ring, redistributing the load when a node fails or when new nodes join the ring.

##### more later

#### Hello world

```java
NodeService s = new NodeService("zookeeperhost:zookeeperport", null, UUID.randomUUID().toString(), "current host address. ex.: 10.1.0.15")
   .withServerListCallback(()->{
       System.out.println("Connection list has changed in zookeeper");
   })
   .withPath("/members");//path where to connect
s.connect();
//retrieves the address from the instance which is responsible for a given resource key
s.getServer(UUID.randomUUID().toString()); 
```

  

