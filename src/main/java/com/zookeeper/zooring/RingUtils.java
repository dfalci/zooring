package com.zookeeper.zooring;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.UUID;

public class RingUtils {

    private static MessageDigest md;
    private static BigInteger baseM;
    public static int nBits = 160;

    static{
        try {
            md = MessageDigest.getInstance("SHA-1");
            baseM = (BigInteger.valueOf(2).pow(nBits)).subtract(BigInteger.valueOf(1));
        }catch(Throwable t){
            t.printStackTrace();
        }
    }

    public static BigInteger hashIt(String value){
        byte[] b = md.digest(value.getBytes());
        return new BigInteger(1, b);
    }

    public static BigInteger fromString(String id){
        try {
            return fromUUID(UUID.fromString(id));
        }catch(IllegalArgumentException ex){
            return hashIt(id);
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
