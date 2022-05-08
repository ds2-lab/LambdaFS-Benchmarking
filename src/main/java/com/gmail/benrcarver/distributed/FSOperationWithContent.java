package com.gmail.benrcarver.distributed;

import org.apache.hadoop.hdfs.DistributedFileSystem;

/**
 * Encapsulates a generic FS read operation. Most of them take a target path, an instance of DistributedFileSystem
 * used to make the actual call, the nameNodeEndpoint used to create the fully-qualified target path, and finally
 * an additional content parameter.
 */
@FunctionalInterface
public interface FSOperationWithContent extends FSOperation {
    void doOperation(String path, DistributedFileSystem hdfs, String nameNodeEndpoint, String content);
}
