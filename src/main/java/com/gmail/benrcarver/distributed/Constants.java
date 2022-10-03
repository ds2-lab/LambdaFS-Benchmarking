package com.gmail.benrcarver.distributed;

public class Constants {
    public static final String NAMENODE_ENDPOINT = "NAMENODE_ENDPOINT";
    public static final String OPERATION = "OPERATION";
    public static final String RESULT = "RESULT";
    public static final String OPERATION_ID = "OPERATION_ID";
    public static final String HDFS_CONFIG_PATH = "HDFS_CONFIG_PATH";
    public static final String TRACK_OP_PERFORMED = "TRACK_OP_PERFORMED";
    public static final String BENCHMARK_MODE = "BENCHMARK_MODE";

    ////////////////
    // OPERATIONS //
    ////////////////
    public static final int OP_RESULT = -101;
    public static final int OP_REGISTRATION = -100;
    public static final int OP_SET_HTTP_TCP_REPLACEMENT_CHANCE = -14;
    public static final int OP_ESTABLISH_CONNECTIONS = -13;
    public static final int OP_SAVE_LATENCIES_TO_FILE = -12;
    public static final int OP_TOGGLE_BENCHMARK_MODE = -11;
    public static final int OP_TOGGLE_OPS_PERFORMED_FOLLOWERS = -10;
    public static final int OP_TRIGGER_CLIENT_GC = -9;
    public static final int OP_CHANGE_POST_TRIAL_SLEEP = -8;
    public static final int OP_GET_ACTIVE_NAMENODES = -7;
    public static final int OP_SET_CONSISTENCY_PROTOCOL_ENABLED = - 6;
    public static final int OP_SET_LOG_LEVEL = -5;
    public static final int OP_CLEAR_METRIC_DATA = -4;
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
    public static final int OP_GET_FILE_STATUS = 21;
    public static final int OP_LIST_DIRECTORIES_FROM_FILE = 23;
    public static final int OP_STAT_FILES_WEAK_SCALING = 24;
    public static final int OP_MKDIR_WEAK_SCALING = 25;
    public static final int OP_PREPARE_GENERATED_WORKLOAD = 26;
    public static final int OP_DO_WARMUP_FOR_PREPARED_WORKLOAD = 27;
    public static final int OP_DO_RANDOM_WORKLOAD= 28;
    public static final int OP_ABORT_RANDOM_WORKLOAD = 29;
    public static final int OP_CREATE_FROM_FILE = 30;

    public static String INTLVD_CREATE_FILES_PERCENTAGE_KEY = "interleaved.create.files.percentage";
    public static double INTLVD_CREATE_FILES_PERCENTAGE_DEFAULT = 0;

    public static String RAW_READ_FILES_PHASE_DURATION_KEY = "raw.read.files.phase.duration";
    public static long   RAW_READ_FILES_PHASE_DURATION_DEFAULT = 0;

    public static String INTLVD_READ_FILES_PERCENTAGE_KEY = "interleaved.read.files.percentage";
    public static double INTLVD_READ_FILES_PERCENTAGE_DEFAULT = 0;

    public static String INTERLEAVED_BM_IAT_DISTRIBUTION_KEY = "interleaved.bm.iat.distribution";
    public static String INTERLEAVED_BM_IAT_DISTRIBUTION_DEFAULT = "UNIFORM";

    public static String INTERLEAVED_BM_IAT_POISSON_LAMBDA_KEY = "interleaved.bm.iat.poisson.lambda";
    public static double INTERLEAVED_BM_IAT_POISSON_LAMBDA_DEFAULT = 10.0; // Expected concurrency

    public static String INTERLEAVED_BM_IAT_PARETO_ALPHA_KEY = "interleaved.bm.iat.pareto.alpha";
    public static double INTERLEAVED_BM_IAT_PARETO_ALPHA_DEFAULT = 2; // Scale parameter

    public static String INTERLEAVED_BM_IAT_PARETO_LOCATION_KEY = "interleaved.bm.iat.pareto.location";
    public static double INTERLEAVED_BM_IAT_PARETO_LOCATION_DEFAULT = 10.0; // Expected concurrency


    public static String INTERLEAVED_BM_DURATION_KEY = "interleaved.bm.duration";
    public static long   INTERLEAVED_BM_DURATION_DEFAULT = 60*1000;

    public static String RAW_RENAME_FILES_PHASE_DURATION_KEY = "raw.rename.files.phase.duration";
    public static long   RAW_RENAME_FILES_PHASE_DURATION_DEFAULT = 0;

    public static String INTLVD_RENAME_FILES_PERCENTAGE_KEY = "interleaved.rename.files.percentage";
    public static double INTLVD_RENAME_FILES_PERCENTAGE_DEFAULT = 0;

    public static String RAW_LS_FILE_PHASE_DURATION_KEY = "raw.ls.files.phase.duration";
    public static long   RAW_LS_FILE_PHASE_DURATION_DEFAULT = 0;

    public static String INTLVD_LS_FILE_PERCENTAGE_KEY = "interleaved.ls.files.percentage";
    public static double INTLVD_LS_FILE_PERCENTAGE_DEFAULT = 0;

    public static String RAW_LS_DIR_PHASE_DURATION_KEY = "raw.ls.dirs.phase.duration";
    public static long   RAW_LS_DIR_PHASE_DURATION_DEFAULT = 0;

    public static String INTERLEAVED_WORKLOAD_NAME_KEY = "interleaved.workload.name";
    public static String INTERLEAVED_WORKLOAD_NAME_DEFAULT = "default";

    public static String INTLVD_LS_DIR_PERCENTAGE_KEY = "interleaved.ls.dirs.percentage";
    public static double INTLVD_LS_DIR_PERCENTAGE_DEFAULT = 0;

    public static String RAW_DElETE_FILES_PHASE_DURATION_KEY = "raw.delete.files.phase.duration";
    public static long   RAW_DELETE_FILES_PHASE_DURATION_DEFAULT = 0;

    public static String INTLVD_DELETE_FILES_PERCENTAGE_KEY = "interleaved.delete.files.percentage";
    public static double INTLVD_DELETE_FILES_PERCENTAGE_DEFAULT = 0;

    public static String RAW_CHMOD_FILES_PHASE_DURATION_KEY = "raw.chmod.files.phase.duration";
    public static long   RAW_CHMOD_FILES_PHASE_DURATION_DEFAULT = 0;

    public static String RAW_CHMOD_DIRS_PHASE_DURATION_KEY = "raw.chmod.dirs.phase.duration";
    public static long   RAW_CHMOD_DIRS_PHASE_DURATION_DEFAULT = 0;

    public static String INTLVD_CHMOD_FILES_PERCENTAGE_KEY = "interleaved.chmod.files.percentage";
    public static double INTLVD_CHMOD_FILES_PERCENTAGE_DEFAULT = 0;

    public static String INTLVD_CHMOD_DIRS_PERCENTAGE_KEY = "interleaved.chmod.dirs.percentage";
    public static double INTLVD_CHMOD_DIRS_PERCENTAGE_DEFAULT = 0;

    public static String RAW_MKDIR_PHASE_DURATION_KEY = "raw.mkdir.phase.duration";
    public static long   RAW_MKDIR_PHASE_DURATION_DEFAULT = 0;

    public static String INTERLEAVED_BM_IAT_UNIT_KEY = "interleaved.bm.iat.unit";
    public static int INTERLEAVED_BM_IAT_UNIT_DEFAULT = 1;

    public static String INTERLEAVED_BM_IAT_SKIPUNIT_KEY = "interleaved.bm.iat.skipunit";
    public static int INTERLEAVED_BM_IAT_SKIPUNIT_DEFAULT = 0;

    public static String INTLVD_MKDIR_PERCENTAGE_KEY = "interleaved.mkdir.percentage";
    public static double INTLVD_MKDIR_PERCENTAGE_DEFAULT = 0;

    public static String RAW_SETREPLICATION_PHASE_DURATION_KEY = "raw.file.setReplication.phase.duration";
    public static long   RAW_SETREPLICATION_PHASE_DURATION_DEFAULT = 0;

    public static String INTLVD_SETREPLICATION_PERCENTAGE_KEY = "interleaved.file.setReplication.percentage";
    public static double INTLVD_SETREPLICATION_PERCENTAGE_DEFAULT = 0;

    public static String RAW_GET_FILE_INFO_PHASE_DURATION_KEY = "raw.file.getInfo.phase.duration";
    public static long   RAW_GET_FILE_INFO_PHASE_DURATION_DEFAULT = 0;

    public static String INTLVD_GET_FILE_INFO_PERCENTAGE_KEY = "interleaved.file.getInfo.percentage";
    public static double INTLVD_GET_FILE_INFO_PERCENTAGE_DEFAULT = 0;

    public static String RAW_GET_DIR_INFO_PHASE_DURATION_KEY = "raw.dir.getInfo.phase.duration";
    public static long   RAW_GET_DIR_INFO_PHASE_DURATION_DEFAULT = 0;

    public static String INTLVD_GET_DIR_INFO_PERCENTAGE_KEY = "interleaved.dir.getInfo.percentage";
    public static double INTLVD_GET_DIR_INFO_PERCENTAGE_DEFAULT = 0;

    public static String RAW_FILE_APPEND_PHASE_DURATION_KEY = "raw.file.append.phase.duration";
    public static long   RAW_FILE_APPEND_PHASE_DURATION_DEFAULT = 0;

    public static String INTLVD_APPEND_FILE_PERCENTAGE_KEY = "interleaved.file.append.percentage";
    public static double INTLVD_APPEND_FILE_PERCENTAGE_DEFAULT = 0;

    public static String RAW_FILE_CHANGE_USER_PHASE_DURATION_KEY = "raw.file.change.user.phase.duration";
    public static long   RAW_FILE_CHANGE_USER_PHASE_DURATION_DEFAULT = 0;

    public static String INTLVD_FILE_CHANGE_USER_PERCENTAGE_KEY = "interleaved.file.change.user.percentage";
    public static double INTLVD_FILE_CHANGE_USER_PERCENTAGE_DEFAULT = 0;

    public static String RAW_DIR_CHANGE_USER_PHASE_DURATION_KEY = "raw.dir.change.user.phase.duration";
    public static long   RAW_DIR_CHANGE_USER_PHASE_DURATION_DEFAULT = 0;

    public static String INTLVD_DIR_CHANGE_USER_PERCENTAGE_KEY = "interleaved.dir.change.user.percentage";
    public static double INTLVD_DIR_CHANGE_USER_PERCENTAGE_DEFAULT = 0;

    public static String BENCHMARK_RANDOM_SEED_KEY = "benchmark.random.seed";
    public static long   BENCHMARK_RANDOM_SEED_DEFAULT = 0;

    public static String  BENCHMARK_DRYRUN_KEY = "benchmark.dryrun";
    public static boolean BENCHMARK_DRYRUN_DEFAULT = false;

    public static String NUM_WORKER_THREADS_KEY = "num.worker.threads";
    public static int    NUM_WORKER_THREADS_DEFAULT = 1;

    public static String BASE_DIR_KEY = "base.dir";
    public static String BASE_DIR_DEFAULT = "/generatedWorkload";

    public static String FILES_TO_CREATE_IN_WARM_UP_PHASE_KEY = "files.to.create.in.warmup.phase";
    public static int FILES_TO_CREATE_IN_WARM_UP_PHASE_DEFAULT = 10;

    public static String WARM_UP_PHASE_WAIT_TIME_KEY = "warmup.phase.wait.time";
    public static int    WARM_UP_PHASE_WAIT_TIME_DEFAULT = 1 * 60 * 1000;

    public static String DIR_PER_DIR_KEY= "dir.per.dir";
    public static int    DIR_PER_DIR_DEFAULT = 2;

    public static String FILES_PER_DIR_KEY= "files.per.dir";
    public static int    FILES_PER_DIR_DEFAULT = 16;

    public static String  READ_FILES_FROM_DISK= "read.files.from.disk";
    public static boolean READ_FILES_FROM_DISK_DEFAULT=false;

    public static String DISK_FILES_PATH="disk.files.path";
    public static String DISK_FILES_PATH_DEFAULT="~";

    public static String  TREE_DEPTH_KEY = "tree.depth";
    public static int     TREE_DEPTH_DEFAULT = 3;

    public static final String LEADER_WORKER_WARMUP_DELAY_KEY= "leader.worker.warmup.delay";
    public static final int    LEADER_WORKER_WARMUP_DELAY_KEY_DEFAULT = 0;
}
