package com.gmail.benrcarver.distributed;

import com.gmail.benrcarver.distributed.workload.BMOpStats;
import org.apache.commons.math3.stat.descriptive.SynchronizedDescriptiveStatistics;

import java.io.Serializable;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

    public Map<String, List<BMOpStats>> opsStats;

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

    public double[] latencyStatistics;

    public DistributedBenchmarkResult() { }

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
     * @param latencyStatistics Latency statistics for the associated workload.
     */
    public DistributedBenchmarkResult(String opId, int operation, int numOpsPerformed,
                                      double duration, long startTime, long stopTime,
                                      double[] latencyStatistics,
                                      Map<String, List<BMOpStats>> opsStats) {
        this.opId = opId;
        this.operation = operation;
        this.numOpsPerformed = numOpsPerformed;
        this.durationSeconds = duration;
        this.startTime = startTime;
        this.stopTime = stopTime;
        this.latencyStatistics = latencyStatistics;
        this.jvmId = ManagementFactory.getRuntimeMXBean().getName();
        this.opsStats = opsStats;
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
     * @param latencyStatistics Latency statistics for the associated workload.
     */
    public DistributedBenchmarkResult(String opId, int operation, int numOpsPerformed,
                                      double duration, long startTime, long stopTime,
                                      double[] latencyStatistics) {
        this(opId, operation, numOpsPerformed, duration, startTime, stopTime, latencyStatistics, new HashMap<>());
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
        return "DistributedBenchmarkResult(opId=" + opId + ", operation=" + operation + ", numOpsPerformed=" +
                numOpsPerformed + ", duration=" + durationSeconds + "sec, throughput=" + getOpsPerSecond() + " ops/sec, startTime=" + startTime + ", stopTime=" +
                stopTime + ")";
    }
}
