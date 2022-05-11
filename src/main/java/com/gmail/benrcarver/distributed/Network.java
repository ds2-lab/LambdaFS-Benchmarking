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
        kryo.setRegistrationRequired(false);
        // kryo.setWarnUnregisteredClasses(true);

        kryo.register(String[].class);
        kryo.register(DistributedBenchmarkResult.class);
        kryo.register(OperationPerformed.class);
        kryo.register(TransactionEvent.class);
        kryo.register(TransactionAttempt.class);
        kryo.register(java.util.HashMap.class);
        kryo.register(java.util.ArrayList.class);
        kryo.register(java.util.List.class);
        kryo.register(byte[].class);
        kryo.register(OperationPerformed[].class);
        kryo.register(java.util.HashMap[].class);

//        kryo.register(org.apache.commons.math3.stat.descriptive.SynchronizedDescriptiveStatistics.class);
//        kryo.register(org.apache.commons.math3.util.ResizableDoubleArray.class);
//        kryo.register(org.apache.commons.math3.util.ResizableDoubleArray.ExpansionMode.class);
//        kryo.register(double[].class);
//        kryo.register(org.apache.commons.math3.stat.descriptive.moment.GeometricMean.class);
//        kryo.register(org.apache.commons.math3.stat.descriptive.summary.SumOfLogs.class);
//        kryo.register(org.apache.commons.math3.stat.descriptive.moment.Kurtosis.class);
//        //kryo.register(org.apache.commons.math3.stat.descriptive.moment.FourthMoment.class);
//        kryo.register(org.apache.commons.math3.stat.descriptive.rank.Max.class);
//        kryo.register(org.apache.commons.math3.stat.descriptive.moment.Mean.class);
//        //kryo.register(org.apache.commons.math3.stat.descriptive.moment.FirstMoment.class);
//        kryo.register(org.apache.commons.math3.stat.descriptive.rank.Min.class);
//        kryo.register(org.apache.commons.math3.stat.descriptive.rank.Percentile.class);
//        kryo.register(int[].class);
//        kryo.register(org.apache.commons.math3.stat.descriptive.rank.Percentile.EstimationType.class);
//        kryo.register(org.apache.commons.math3.util.KthSelector.class);
//        kryo.register(org.apache.commons.math3.util.MedianOf3PivotingStrategy.class);
//        kryo.register(org.apache.commons.math3.stat.ranking.NaNStrategy.class);
//        kryo.register(org.apache.commons.math3.stat.descriptive.moment.Skewness.class);
//        //kryo.register(org.apache.commons.math3.stat.descriptive.moment.ThirdMoment.class);
//        kryo.register(org.apache.commons.math3.stat.descriptive.summary.Sum.class);
//        kryo.register(org.apache.commons.math3.stat.descriptive.summary.SumOfSquares.class);
//        kryo.register(org.apache.commons.math3.stat.descriptive.moment.Variance.class);
//        kryo.register(org.apache.commons.math3.stat.descriptive.moment.SecondMoment.class);
    }
}
