# ZOORing

Minimal implementation of a DHT ring (Distributed Hash Table) of instances using Apache Zookeeper + Apache Curator. Used for sticky routing requests relying on resource ids.

The eventually consistent process, verifies the current connected nodes from the ring and assigns one of them as the responsible for any call to a resource. It monitors nodes availability all across the ring, redistributing the load when a node fails or when new nodes join the ring.

More later

  

