package com.gmail.benrcarver.distributed.workload;

import com.gmail.benrcarver.distributed.DistributedBenchmarkResult;

import java.io.Serializable;

public class WorkloadResponse implements Serializable {
    public boolean erred;
    public DistributedBenchmarkResult result;

    public WorkloadResponse() { }

    public WorkloadResponse(boolean erred, DistributedBenchmarkResult result) {
        this.erred = erred;
        this.result = result;
    }
}
