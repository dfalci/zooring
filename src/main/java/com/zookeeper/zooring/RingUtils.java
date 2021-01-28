package com.zookeeper.zooring;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import java.math.BigInteger;

public class RingUtils {
    private static final HashFunction hash64 = Hashing.sipHash24();
    private static final HashFunction hash128 = Hashing.murmur3_128();

    /**
     * Generates a 32-bit hash using the built-in java hash algorithm
     * @param value
     * @return
     */
    public static BigInteger hashIt32(String value){
        return BigInteger.valueOf(value.hashCode());
    }

    /**
     * Generates a 64-bit hash using the built-in java hash algorithm
     * @param value
     * @return
     */
    public static BigInteger hashIt64(String value){
        return new BigInteger(1, hash64.hashBytes(value.getBytes()).asBytes());
    }

    public static BigInteger hashIt128(String value){
        return new BigInteger(1, hash128.hashBytes(value.getBytes()).asBytes());
    }

    public static BigInteger fromString(String id){
        return hashIt128(id);
    }


}
