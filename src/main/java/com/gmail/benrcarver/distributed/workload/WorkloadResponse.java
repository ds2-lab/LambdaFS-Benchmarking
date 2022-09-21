package com.gmail.benrcarver.distributed.workload;

import com.gmail.benrcarver.distributed.DistributedBenchmarkResult;

import java.io.Serializable;

public class WorkloadResponse implements Serializable {
    public final boolean erred;
    public final DistributedBenchmarkResult result;

    public WorkloadResponse(boolean erred, DistributedBenchmarkResult result) {
        this.erred = erred;
        this.result = result;
    }
}
