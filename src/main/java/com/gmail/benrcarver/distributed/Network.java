package com.gmail.benrcarver.distributed;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryonet.EndPoint;
import io.hops.metrics.OperationPerformed;
import io.hops.metrics.TransactionAttempt;
import io.hops.metrics.TransactionEvent;

import java.util.HashMap;

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

        kryo.setReferences(true);

        kryo.register(String[].class);
        kryo.register(DistributedBenchmarkResult.class);
        kryo.register(OperationPerformed.class);
        kryo.register(HashMap.class);
        kryo.register(TransactionEvent.class);
        kryo.register(TransactionAttempt.class);
        kryo.register(java.util.HashMap.class);
        kryo.register(java.util.ArrayList.class);
        kryo.register(byte[].class);
        kryo.register(OperationPerformed[].class);
    }
}
