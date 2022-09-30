package com.gmail.benrcarver.distributed.coin;

import com.gmail.benrcarver.distributed.Constants;
import com.gmail.benrcarver.distributed.workload.BenchmarkDistribution;

import java.io.*;
import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.*;

import static com.gmail.benrcarver.distributed.coin.FileSizeMultiFaceCoin.isTwoDecimalPlace;

/**
 * Originally written by Salman Niazi, the author of HopsFS.
 */
public class BMConfiguration implements Serializable {
    private Properties props = null;

    public BMConfiguration() {

    }

    /**
     * @param configPath Path to workload configuration file.
     */
    public BMConfiguration(String configPath) throws IOException {
        this.props = loadPropFile(configPath);
    }

    public static void printHelp() {
        System.out.println("You are doomed");
    }


    private Properties loadPropFile(String file) throws IOException {
        Properties props = new Properties();
        InputStream input = new FileInputStream(file);
        props.load(input);
        return props;
    }

    public int getThreadsPerWorker() {
        return getInt(Constants.NUM_WORKER_THREADS_KEY, Constants.NUM_WORKER_THREADS_DEFAULT);
    }

    public long getBenchMarkRandomSeed() {
        return getLong(Constants.BENCHMARK_RANDOM_SEED_KEY, Constants.BENCHMARK_RANDOM_SEED_DEFAULT);
    }

    public boolean getBenchmarkDryrun() {
        return getBoolean(Constants.BENCHMARK_DRYRUN_KEY, Constants.BENCHMARK_DRYRUN_DEFAULT);
    }

    public BigDecimal getInterleavedBmCreateFilesPercentage() {
        return getBigDecimal(Constants.INTLVD_CREATE_FILES_PERCENTAGE_KEY, Constants.INTLVD_CREATE_FILES_PERCENTAGE_DEFAULT);
    }

    public long getRawBmReadFilesPhaseDuration() {
        return getLong(Constants.RAW_READ_FILES_PHASE_DURATION_KEY, Constants.RAW_READ_FILES_PHASE_DURATION_DEFAULT);
    }

    public BigDecimal getInterleavedBmReadFilesPercentage() {
        return getBigDecimal(Constants.INTLVD_READ_FILES_PERCENTAGE_KEY, Constants.INTLVD_READ_FILES_PERCENTAGE_DEFAULT);
    }

    public long getRawBmRenameFilesPhaseDuration() {
        return getLong(Constants.RAW_RENAME_FILES_PHASE_DURATION_KEY, Constants.RAW_RENAME_FILES_PHASE_DURATION_DEFAULT);
    }

    public BigDecimal getInterleavedBmRenameFilesPercentage() {
        return getBigDecimal(Constants.INTLVD_RENAME_FILES_PERCENTAGE_KEY, Constants.INTLVD_RENAME_FILES_PERCENTAGE_DEFAULT);
    }

    public long getRawBmDeleteFilesPhaseDuration() {
        return getLong(Constants.RAW_DElETE_FILES_PHASE_DURATION_KEY, Constants.RAW_DELETE_FILES_PHASE_DURATION_DEFAULT);
    }

    public BigDecimal getInterleavedBmDeleteFilesPercentage() {
        return getBigDecimal(Constants.INTLVD_DELETE_FILES_PERCENTAGE_KEY, Constants.INTLVD_DELETE_FILES_PERCENTAGE_DEFAULT);
    }

    public long getRawBmChmodFilesPhaseDuration() {
        return getLong(Constants.RAW_CHMOD_FILES_PHASE_DURATION_KEY, Constants.RAW_CHMOD_FILES_PHASE_DURATION_DEFAULT);
    }

    public long getRawBmChmodDirsPhaseDuration() {
        return getLong(Constants.RAW_CHMOD_DIRS_PHASE_DURATION_KEY, Constants.RAW_CHMOD_DIRS_PHASE_DURATION_DEFAULT);
    }

    public BigDecimal getInterleavedBmChmodFilesPercentage() {
        return getBigDecimal(Constants.INTLVD_CHMOD_FILES_PERCENTAGE_KEY, Constants.INTLVD_CHMOD_FILES_PERCENTAGE_DEFAULT);
    }

    public BigDecimal getInterleavedBmChmodDirsPercentage() {
        return getBigDecimal(Constants.INTLVD_CHMOD_DIRS_PERCENTAGE_KEY, Constants.INTLVD_CHMOD_DIRS_PERCENTAGE_DEFAULT);
    }

    public long getRawBmLsFilePhaseDuration() {
        return getLong(Constants.RAW_LS_FILE_PHASE_DURATION_KEY, Constants.RAW_LS_FILE_PHASE_DURATION_DEFAULT);
    }

    public BigDecimal getInterleavedBmLsFilePercentage() {
        return getBigDecimal(Constants.INTLVD_LS_FILE_PERCENTAGE_KEY, Constants.INTLVD_LS_FILE_PERCENTAGE_DEFAULT);
    }

    public long getRawBmLsDirPhaseDuration() {
        return getLong(Constants.RAW_LS_DIR_PHASE_DURATION_KEY, Constants.RAW_LS_DIR_PHASE_DURATION_DEFAULT);
    }
    public static String INTERLEAVED_WORKLOAD_NAME_KEY = "interleaved.workload.name";
    public static double INTERLEAVED_WORKLOAD_NAME_DEFAULT = 0;

    public String getInterleavedBmWorkloadName() {
        return getString(Constants.INTERLEAVED_WORKLOAD_NAME_KEY, Constants.INTERLEAVED_WORKLOAD_NAME_DEFAULT);
    }

    public BigDecimal getInterleavedBmLsDirPercentage() {
        return getBigDecimal(Constants.INTLVD_LS_DIR_PERCENTAGE_KEY, Constants.INTLVD_LS_DIR_PERCENTAGE_DEFAULT);
    }

    public long getRawBmMkdirPhaseDuration() {
        return getLong(Constants.RAW_MKDIR_PHASE_DURATION_KEY, Constants.RAW_MKDIR_PHASE_DURATION_DEFAULT);
    }

    public int getInterleavedBMIaTUnit() {
        return getInt(Constants.INTERLEAVED_BM_IAT_UNIT_KEY, Constants.INTERLEAVED_BM_IAT_UNIT_DEFAULT);
    }

    public int getInterleavedBMIaTSkipUnit() {
        return getInt(Constants.INTERLEAVED_BM_IAT_SKIPUNIT_KEY, Constants.INTERLEAVED_BM_IAT_SKIPUNIT_DEFAULT);
    }
    
    public long getInterleavedBmDuration() {
        return getLong(Constants.INTERLEAVED_BM_DURATION_KEY, Constants.INTERLEAVED_BM_DURATION_DEFAULT);
    }

    public BenchmarkDistribution getInterleavedBMIaTDistribution() {
        String val = getString(Constants.INTERLEAVED_BM_IAT_DISTRIBUTION_KEY, Constants.INTERLEAVED_BM_IAT_DISTRIBUTION_DEFAULT);
        return BenchmarkDistribution.valueOf(val);
    }

    public double getInterleavedBMIaTPoissonLambda() {
        return getDouble(Constants.INTERLEAVED_BM_IAT_POISSON_LAMBDA_KEY, Constants.INTERLEAVED_BM_IAT_POISSON_LAMBDA_DEFAULT);
    }

    public double getInterleavedBMIaTParetoAlpha() {
        return getDouble(Constants.INTERLEAVED_BM_IAT_PARETO_ALPHA_KEY, Constants.INTERLEAVED_BM_IAT_PARETO_ALPHA_DEFAULT);
    }

    public double getInterleavedBMIaTParetoLocation() {
        return getDouble(Constants.INTERLEAVED_BM_IAT_PARETO_LOCATION_KEY, Constants.INTERLEAVED_BM_IAT_PARETO_LOCATION_DEFAULT);
    }

    public BigDecimal getInterleavedBmMkdirPercentage() {
        return getBigDecimal(Constants.INTLVD_MKDIR_PERCENTAGE_KEY, Constants.INTLVD_MKDIR_PERCENTAGE_DEFAULT);
    }

    public long getRawBmSetReplicationPhaseDuration() {
        return getLong(Constants.RAW_SETREPLICATION_PHASE_DURATION_KEY, Constants.RAW_SETREPLICATION_PHASE_DURATION_DEFAULT);
    }

    public BigDecimal getInterleavedBmSetReplicationPercentage() {
        return getBigDecimal(Constants.INTLVD_SETREPLICATION_PERCENTAGE_KEY, Constants.INTLVD_SETREPLICATION_PERCENTAGE_DEFAULT);
    }

    public long getRawBmGetFileInfoPhaseDuration() {
        return getLong(Constants.RAW_GET_FILE_INFO_PHASE_DURATION_KEY, Constants.RAW_GET_FILE_INFO_PHASE_DURATION_DEFAULT);
    }

    public BigDecimal getInterleavedBmGetFileInfoPercentage() {
        return getBigDecimal(Constants.INTLVD_GET_FILE_INFO_PERCENTAGE_KEY, Constants.INTLVD_GET_FILE_INFO_PERCENTAGE_DEFAULT);
    }

    public long getRawBmGetDirInfoPhaseDuration() {
        return getLong(Constants.RAW_GET_DIR_INFO_PHASE_DURATION_KEY, Constants.RAW_GET_DIR_INFO_PHASE_DURATION_DEFAULT);
    }

    public BigDecimal getInterleavedBmGetDirInfoPercentage() {
        return getBigDecimal(Constants.INTLVD_GET_DIR_INFO_PERCENTAGE_KEY, Constants.INTLVD_GET_DIR_INFO_PERCENTAGE_DEFAULT);
    }

    public long getRawBmAppendFilePhaseDuration() {
        return getLong(Constants.RAW_FILE_APPEND_PHASE_DURATION_KEY, Constants.RAW_FILE_APPEND_PHASE_DURATION_DEFAULT);
    }

    public BigDecimal getInterleavedBmAppendFilePercentage() {
        return getBigDecimal(Constants.INTLVD_APPEND_FILE_PERCENTAGE_KEY, Constants.INTLVD_APPEND_FILE_PERCENTAGE_DEFAULT);
    }

    public int getTreeDepth() {
        return getInt(Constants.TREE_DEPTH_KEY, Constants.TREE_DEPTH_DEFAULT);
    }
    
    public String getBaseDir() {
        return getString(Constants.BASE_DIR_KEY, Constants.BASE_DIR_DEFAULT);
    }

    public int getFilesToCreateInWarmUpPhase() {
        return getInt(Constants.FILES_TO_CREATE_IN_WARM_UP_PHASE_KEY, Constants.FILES_TO_CREATE_IN_WARM_UP_PHASE_DEFAULT);
    }

    public int getWarmUpPhaseWaitTime() {
        return getInt(Constants.WARM_UP_PHASE_WAIT_TIME_KEY, Constants.WARM_UP_PHASE_WAIT_TIME_DEFAULT);
    }

    public int getDirPerDir() {
        return getInt(Constants.DIR_PER_DIR_KEY, Constants.DIR_PER_DIR_DEFAULT);
    }

    public int getFilesPerDir() {
        return getInt(Constants.FILES_PER_DIR_KEY, Constants.FILES_PER_DIR_DEFAULT);
    }

    public long getRawFileChangeUserPhaseDuration() {
        return getLong(Constants.RAW_FILE_CHANGE_USER_PHASE_DURATION_KEY, Constants.RAW_FILE_CHANGE_USER_PHASE_DURATION_DEFAULT);
    }

    public BigDecimal getInterleavedBmFileChangeOwnerPercentage() {
        return getBigDecimal(Constants.INTLVD_FILE_CHANGE_USER_PERCENTAGE_KEY, Constants.INTLVD_FILE_CHANGE_USER_PERCENTAGE_DEFAULT);
    }

    public long getRawDirChangeUserPhaseDuration() {
        return getLong(Constants.RAW_DIR_CHANGE_USER_PHASE_DURATION_KEY, Constants.RAW_DIR_CHANGE_USER_PHASE_DURATION_DEFAULT);
    }

    public BigDecimal getInterleavedBmDirChangeOwnerPercentage() {
        return getBigDecimal(Constants.INTLVD_DIR_CHANGE_USER_PERCENTAGE_KEY, Constants.INTLVD_DIR_CHANGE_USER_PERCENTAGE_DEFAULT);
    }

    public boolean getReadFilesFromDisk(){
        return getBoolean(Constants.READ_FILES_FROM_DISK, Constants.READ_FILES_FROM_DISK_DEFAULT);
    }

    public String getDiskNameSpacePath(){
        return getString(Constants.DISK_FILES_PATH, Constants.DISK_FILES_PATH_DEFAULT);
    }

    public int getWorkerWarmUpDelay(){
        return getInt(Constants.LEADER_WORKER_WARMUP_DELAY_KEY, Constants.LEADER_WORKER_WARMUP_DELAY_KEY_DEFAULT);
    }

    private int getInt(String key, int defaultVal) {
        String val = props.getProperty(key, Integer.toString(defaultVal));
        return Integer.parseInt(val);
    }

    private long getLong(String key, long defaultVal) {
        String val = props.getProperty(key, Long.toString(defaultVal));
        return Long.parseLong(val);
    }

    private short getShort(String key, short defaultVal) {
        String val = props.getProperty(key, Short.toString(defaultVal));
        return Short.parseShort(val);
    }

    private boolean getBoolean(String key, boolean defaultVal) {
        String val = props.getProperty(key, Boolean.toString(defaultVal));
        return Boolean.parseBoolean(val);
    }

    private double getDouble(String key, double defaultVal) {
        String val = props.getProperty(key, Double.toString(defaultVal));
        return Double.parseDouble(val);
    }

    private String getString(String key, String defaultVal) {
        String val = props.getProperty(key, defaultVal);
        if(val != null){
            val.trim();
        }
        return val;
    }

    private BigDecimal getBigDecimal(String key, double defaultVal) {
        if (!isTwoDecimalPlace(defaultVal)) {
            throw new IllegalArgumentException("Wrong default Value. Only one decimal place is supported. Value " + defaultVal + " key: " + key);
        }

        double userVal = Double.parseDouble(props.getProperty(key, Double.toString(defaultVal)));
        if (!isTwoDecimalPlace(userVal)) {
            throw new IllegalArgumentException("Wrong user value. Only one decimal place is supported. Value " + userVal + " key: " + key);
        }

        //System.out.println(key+" value "+userVal);
        return new BigDecimal(userVal, new MathContext(4, RoundingMode.HALF_UP));
    }


}
