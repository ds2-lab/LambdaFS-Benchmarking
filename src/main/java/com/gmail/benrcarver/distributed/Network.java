package com.gmail.benrcarver.distributed;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryonet.EndPoint;

/**
 * KryoNet is the TCP library used. It uses the Kryo serialization library.
 * The Kryo serialization library requires that you register any non-default classes
 * that you will be serializing (which, in our case, means any objects we'll be sending
 * over TCP). So, this class is used by the Commander and Follower to register the
 * objects they'll be sending back and forth to each other.
 */
public class Network {
    public static void register (EndPoint endPoint) {
        Kryo kryo = endPoint.getKryo();
        kryo.register(String[].class);
        kryo.register(DistributedBenchmarkResult.class);
    }
}
