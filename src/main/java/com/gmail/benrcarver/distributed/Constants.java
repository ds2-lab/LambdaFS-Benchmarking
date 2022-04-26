package com.gmail.benrcarver.distributed;

public class Constants {
    public static final String NAMENODE_ENDPOINT = "NAMENODE_ENDPOINT";
    public static final String OPERATION = "OPERATION";
    public static final String RESULT = "RESULT";
    public static final String OPERATION_ID = "OPERATION_ID";
    public static final String HDFS_CONFIG_PATH = "HDFS_CONFIG_PATH";

    ////////////////
    // OPERATIONS //
    ////////////////
    public static final int OP_RESULT = -101;
    public static final int OP_REGISTRATION = -100;
    public static final int OP_TRIGGER_CLIENT_GC = -9;
    public static final int OP_CHANGE_POST_TRIAL_SLEEP = -8;
    public static final int OP_GET_ACTIVE_NAMENODES = -7;
    public static final int OP_SET_CONSISTENCY_PROTOCOL_ENABLED = - 6;
    public static final int OP_SET_LOG_LEVEL = -5;
    public static final int OP_CLEAR_STATISTICS = -4;
    public static final int OP_WRITE_STATISTICS = -3;
    public static final int OP_PRINT_OPS_PERFORMED = -2;
    public static final int OP_PRINT_TCP_DEBUG = -1;
    public static final int OP_EXIT = 0;
    public static final int OP_CREATE_FILE = 1;
    public static final int OP_MKDIR = 2;
    public static final int OP_READ_FILE = 3;
    public static final int OP_RENAME = 4;
    public static final int OP_DELETE = 5;
    public static final int OP_LIST = 6;
    public static final int OP_APPEND = 7;
    public static final int OP_CREATE_SUBTREE = 8;
    public static final int OP_PING = 9;
    public static final int OP_PREWARM = 10;
    public static final int OP_WRITE_FILES_TO_DIR = 11;
    public static final int OP_READ_FILES = 12;
    public static final int OP_DELETE_FILES = 13;
    public static final int OP_WRITE_FILES_TO_DIRS = 14;
    public static final int OP_WEAK_SCALING_READS = 15;
    public static final int OP_STRONG_SCALING_READS = 16;
    public static final int OP_WEAK_SCALING_WRITES = 17;
    public static final int OP_STRONG_SCALING_WRITES = 18;
    public static final int OP_CREATE_DIRECTORIES = 19;
    public static final int OP_WEAK_SCALING_READS_V2 = 20;
}
