package com.gmail.benrcarver.distributed;

import java.io.Serializable;

public class DistributedBenchmarkResult implements Serializable {
    public String opId;
    public int operation;
    public int numOpsPerformed;
    public double durationSeconds;
    public long startTime;
    public long stopTime;

    public DistributedBenchmarkResult() { }

    public DistributedBenchmarkResult(String opId, int operation, int numOpsPerformed, double duration, long startTime,
                                      long stopTime) {
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

    /**
     * Return throughput in form of ops/sec.
     */
    public double getOpsPerSecond() {
        return (numOpsPerformed / durationSeconds);
    }

    @Override
    public String toString() {
        return "DistributedBenchmarkResult(opId=" + opId + ", operation=" + operation + ", numOpsPerformed=" +
                numOpsPerformed + ", duration=" + durationSeconds + "ms, startTime=" + startTime + ", stopTime=" +
                stopTime + ")";
    }
}
