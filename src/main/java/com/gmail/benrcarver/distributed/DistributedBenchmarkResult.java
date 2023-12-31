package com.gmail.benrcarver.distributed;

import java.io.Serializable;
import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import io.hops.metrics.TransactionEvent;

import io.hops.metrics.OperationPerformed;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

/**
 * Encapsulates the result of running a particular benchmark. This class holds onto various metrics that
 * we may find interesting or want to report, such as cache hits and misses, start and stop time for the benchmark,
 * throughput, etc.
 */
public class DistributedBenchmarkResult implements Serializable {
    public String opId;
    public String jvmId;
    public int operation;
    public int numOpsPerformed;
    public OperationPerformed[] opsPerformed;
    public ConcurrentHashMap<String, List<TransactionEvent>> txEvents;

    /**
     * Duration in seconds.
     */
    public double durationSeconds;

    /**
     * Start time as an Epoch millisecond.
     */
    public long startTime;

    /**
     * End time as an Epoch millisecond.
     */
    public long stopTime;

    /**
     * Number of caches hits encountered during the experiment.
     */
    public int cacheHits;

    /**
     * Number of caches misses encountered during the experiment.
     */
    public int cacheMisses;

    /**
     * The number of failed FS operations.
     */
    public int numFailures;


    /**
     * The amount of time (units: milliseconds) spent garbage-collecting by all NameNodes during the experiment.
     */
    public double gcTimeMs;

    /**
     * The number of garbage collections across all NameNodes during the experiment.
     */
    public int numGCs;

    /**
     * TCP latencies obtained during the workload whose result is encapsulated by this DistributedBenchmarkResult instance.
     */
    public DescriptiveStatistics tcpLatencyStatistics;

    /**
     * HTTP latencies obtained during the workload whose result is encapsulated by this DistributedBenchmarkResult instance.
     */
    public DescriptiveStatistics httpLatencyStatistics;

    public static DistributedBenchmarkResult FAILED_RESULT = new DistributedBenchmarkResult(
            "FAILED", -1, -1, -1, -1, -1);

    public DistributedBenchmarkResult() { }


    private DistributedBenchmarkResult(String opId, int operation, int numOpsPerformed,
                                      double duration, long startTime, long stopTime) {
        this(opId, operation, numOpsPerformed, duration, startTime, stopTime, 0, 0);
    }

    public DistributedBenchmarkResult(String opId, int operation, int numOpsPerformed,
                                      double duration, long startTime, long stopTime,
                                      int cacheHits, int cacheMisses) {
        this(opId, operation, numOpsPerformed, duration, startTime, stopTime, cacheHits, cacheMisses,
                null, null);
    }

    public DistributedBenchmarkResult(String opId, int operation, int numOpsPerformed, double duration, long startTime,
                                      long stopTime, int cacheHits, int cacheMisses, OperationPerformed[] opsPerformed,
                                      ConcurrentHashMap<String, List<TransactionEvent>> txEvents) {
        this(opId, operation, numOpsPerformed, duration, startTime, stopTime, cacheHits, cacheMisses,
                opsPerformed, txEvents, null, null, -1);
    }

    /**
     * Constructor.
     *
     * @param opId Unique ID of the operation.
     * @param operation Internal ID of the operation (like the constant integer value that refers to this
     *                  operation in the code).
     * @param numOpsPerformed How many operations we performed (at a high/abstract level, as certain operations may
     *                        actually perform a number of real FS calls).
     * @param duration How long, in seconds.
     * @param startTime Start time, epoch millisecond.
     * @param stopTime End time, epoch millisecond.
     */
    public DistributedBenchmarkResult(String opId, int operation, int numOpsPerformed, double duration, long startTime,
                                      long stopTime, int cacheHits, int cacheMisses, OperationPerformed[] opsPerformed,
                                      ConcurrentHashMap<String, List<TransactionEvent>> txEvents,
                                      DescriptiveStatistics tcpLatencyStatistics,
                                      DescriptiveStatistics httpLatencyStatistics, int numFailures) {
        this.opId = opId;
        this.operation = operation;
        this.numOpsPerformed = numOpsPerformed;
        this.durationSeconds = duration;
        this.startTime = startTime;
        this.stopTime = stopTime;
        this.cacheHits = cacheHits;
        this.cacheMisses = cacheMisses;
        this.opsPerformed = opsPerformed;
        this.txEvents = txEvents;
        this.tcpLatencyStatistics = tcpLatencyStatistics;
        this.httpLatencyStatistics = httpLatencyStatistics;
        this.numFailures = numFailures;
//        this.gcTimeMs = gcTimeMs;
//        this.numGCs = numGCs;

        this.jvmId = ManagementFactory.getRuntimeMXBean().getName();
    }

    public void setOperationId(String operationId) {
        this.opId = operationId;
    }

    public void setOperation(int operation) { this.operation = operation; }

    /**
     * Return throughput in form of ops/sec.
     */
    public double getOpsPerSecond() {
        return (numOpsPerformed / durationSeconds);
    }

    @Override
    public String toString() {
        String str = "DistributedBenchmarkResult(opId=" + opId + ", operation=" + operation + ", numOpsPerformed=" +
                numOpsPerformed + ", duration=" + durationSeconds + "sec, throughput=" + getOpsPerSecond() +
                " ops/sec, startTime=" + startTime + ", stopTime=" + stopTime + ", cacheHits=" + cacheHits +
                ", cacheMisses=" + cacheMisses + ", numFailures=" + numFailures +
                ", gcTime(Seconds)=" + gcTimeMs + ", numGCs=" + numGCs;

        if (tcpLatencyStatistics != null)
            str += ", avgTcpLatency=" + (tcpLatencyStatistics.getN() > 0 ? tcpLatencyStatistics.getMean() : 0);

        if (httpLatencyStatistics != null)
            str += ", avgHttpLatency=" + (httpLatencyStatistics.getN() > 0 ? httpLatencyStatistics.getMean() : 0);

        return str + ")";
    }
}
