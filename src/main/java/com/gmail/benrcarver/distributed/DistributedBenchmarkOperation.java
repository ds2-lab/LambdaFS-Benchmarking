package com.gmail.benrcarver.distributed;

import org.apache.hadoop.hdfs.DistributedFileSystem;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;

/**
 * Serves a similar purpose to the {@link FSOperation} class. This is used within {@link Commander} to run arbitrary
 * distributed benchmarks all using the same function, namely {@link Commander#performDistributedBenchmark}.
 */
public abstract class DistributedBenchmarkOperation {
    public abstract DistributedBenchmarkResult call(final DistributedFileSystem sharedHdfs,
                                                    final String nameNodeEndpoint, int numThreads,
                                                    int opsPerFile, String inputPath, boolean shuffle,
                                                    int opCode, List<String> directories)
            throws IOException, InterruptedException;
}
