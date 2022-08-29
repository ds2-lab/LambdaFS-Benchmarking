package com.gmail.benrcarver.distributed;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DistributedFileSystem;

public abstract class FSOperation {
    public final String endpoint;

    public FSOperation(String endpoint) {
        this.endpoint = endpoint;
    }

    /**
     * Implement this to run whatever the thread is supposed to do for the benchmark.
     * @param hdfs A newly-created DFS instance to use for issuing FS operations.
     */
    public abstract boolean call(final DistributedFileSystem hdfs, String path, String content);
}
