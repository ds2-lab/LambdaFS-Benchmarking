package com.gmail.benrcarver.distributed;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.hadoop.hdfs.DistributedFileSystem;

import static com.gmail.benrcarver.distributed.Commands.*;

/**
 * Used in the {@link Commands} class, specifically by the {@link Commands#executeBenchmark} function.
 *
 * Every function that calls {@link Commands#executeBenchmark} passes in an instance of {@link FSOperation}.
 * This enables the {@link Commands#executeBenchmark} to be used generically regardless of the actual
 * operation being performed.
 */
public abstract class FSOperation {

    public FSOperation() {}

    public FSOperation(String endpoint) {}

    /**
     * Implement this to run whatever the thread is supposed to do for the benchmark.
     * @param hdfs A newly-created DFS instance to use for issuing FS operations.
     */
    public abstract boolean call(final DistributedFileSystem hdfs, String path, String content);

    public static FSOperation NOT_SUPPORTED = new FSOperation() {
        @Override
        public boolean call(DistributedFileSystem hdfs, String path, String content) {
            throw new NotImplementedException("The requested operation is not supported.");
        }
    };

    public static FSOperation CREATE_FILE = new FSOperation() {
        @Override
        public boolean call(DistributedFileSystem hdfs, String path, String content) {
            return createFile(path, content, hdfs);
        }
    };

    public static FSOperation LIST_DIR_NO_PRINT = new FSOperation() {
        @Override
        public boolean call(DistributedFileSystem hdfs, String path, String content) {
            return listDirectoryNoPrint(hdfs, path);
        }
    };

    public static FSOperation READ_FILE = new FSOperation() {
        @Override
        public boolean call(DistributedFileSystem hdfs, String path, String content) {
            return readFile(path, hdfs);
        }
    };

    public static FSOperation DIR_INFO = new FSOperation() {
        @Override
        public boolean call(DistributedFileSystem hdfs, String path, String content) {
            return getFileStatus(path, hdfs);
        }
    };

    public static FSOperation FILE_INFO = new FSOperation() {
        @Override
        public boolean call(DistributedFileSystem hdfs, String path, String content) {
            return getFileStatus(path, hdfs);
        }
    };

    public static FSOperation MKDIRS = new FSOperation() {
        @Override
        public boolean call(DistributedFileSystem hdfs, String path, String content) {
            return mkdir(path, hdfs);
        }
    };

    public static FSOperation DELETE_FILE = new FSOperation() {
        @Override
        public boolean call(DistributedFileSystem hdfs, String path, String content) {
            return delete(path, hdfs);
        }
    };

    public static FSOperation RENAME_FILE = new FSOperation() {
        @Override
        public boolean call(DistributedFileSystem hdfs, String path, String content) {
            return readFile(path, hdfs);
        }
    };

}
