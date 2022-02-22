package com.gmail.benrcarver.distributed;

import java.io.Serializable;

public class DistributedBenchmarkResult implements Serializable {
    public String opId;
    public int operation;
    public int numOpsPerformed;

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
     */
    public DistributedBenchmarkResult(String opId, int operation, int numOpsPerformed,
                                      double duration, long startTime, long stopTime) {
        this.opId = opId;
        this.operation = operation;
        this.numOpsPerformed = numOpsPerformed;
        this.durationSeconds = duration;
        this.startTime = startTime;
        this.stopTime = stopTime;
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
                numOpsPerformed + ", duration=" + durationSeconds + "sec, startTime=" + startTime + ", stopTime=" +
                stopTime + ")";
    }
}
