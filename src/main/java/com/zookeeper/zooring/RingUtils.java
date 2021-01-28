package com.zookeeper.zooring;

import java.math.BigInteger;

public class RingUtils {

    /**
     * Generates a 32-bit hash using the built-in java hash algorithm
     * @param value
     * @return
     */
    public static BigInteger hashIt(String value){
        return BigInteger.valueOf(value.hashCode());
    }

    public static BigInteger fromString(String id){
        return hashIt(id);
    }


}
