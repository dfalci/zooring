package com.visual.zookeeper;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.UUID;

public class RingUtils {

    public static BigInteger fromString(String id){
        try {
            return fromUUID(UUID.fromString(id));
        }catch(IllegalArgumentException ex){
            return BigInteger.valueOf(id.hashCode());
        }
    }

    public static BigInteger fromUUID(UUID u){
        ByteBuffer bb = ByteBuffer.wrap(new byte[16]);
        bb.putLong(u.getMostSignificantBits());
        bb.putLong(u.getLeastSignificantBits());
        return new BigInteger(bb.array());
    }

    public static BigInteger getPartition(String id, BigInteger nNodes){
        return fromString(id).mod(nNodes);
    }

}
