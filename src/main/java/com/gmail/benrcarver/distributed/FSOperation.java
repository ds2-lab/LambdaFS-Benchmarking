package com.gmail.benrcarver.distributed;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DistributedFileSystem;

/**
 * Used in the {@link Commands} class, specifically by the {@link Commands#executeBenchmark} function.
 *
 * Every function that calls {@link Commands#executeBenchmark} passes in an instance of {@link FSOperation}.
 * This enables the {@link Commands#executeBenchmark} to be used generically regardless of the actual
 * operation being performed.
 */
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
