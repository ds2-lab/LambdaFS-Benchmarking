package com.gmail.benrcarver.distributed;

import org.apache.hadoop.hdfs.DistributedFileSystem;

/**
 * Encapsulates a generic FS read operation. Most of them take a target path, an instance of DistributedFileSystem
 * used to make the actual call, and the nameNodeEndpoint used to create the fully-qualified target path.
 *
 * Sometimes there is an additional 'content' argument (e.g., for write operations). For that, we use the
 * {@link FSOperationWithContent} interface.
 */
@FunctionalInterface
public interface StandardFSOperation extends FSOperation {
    void doOperation(String path, DistributedFileSystem hdfs, String nameNodeEndpoint);
}
