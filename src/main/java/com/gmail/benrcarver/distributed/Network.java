package com.gmail.benrcarver.distributed;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryonet.EndPoint;

public class Network {
    public static void register (EndPoint endPoint) {
        Kryo kryo = endPoint.getKryo();

        kryo.setReferences(true);
        kryo.setRegistrationRequired(false);
        // kryo.setWarnUnregisteredClasses(true);

        kryo.register(String[].class);
        kryo.register(DistributedBenchmarkResult.class);
        kryo.register(java.util.HashMap.class);
        kryo.register(java.util.ArrayList.class);
        kryo.register(java.util.List.class);
        kryo.register(byte[].class);
        kryo.register(java.util.HashMap[].class);
    }
}
