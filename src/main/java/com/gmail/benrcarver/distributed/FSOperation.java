package com.gmail.benrcarver.distributed;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DistributedFileSystem;

public abstract class FSOperation {
    public final String endpoint;
    public final Configuration conf;

    public FSOperation(String endpoint, Configuration conf) {
        this.endpoint = endpoint;
        this.conf = conf;
    }

    /**
     * Implement this to run whatever the thread is supposed to do for the benchmark.
     * @param hdfs A newly-created DFS instance to use for issuing FS operations.
     */
    public abstract void call(final DistributedFileSystem hdfs, String path, String content);
}
