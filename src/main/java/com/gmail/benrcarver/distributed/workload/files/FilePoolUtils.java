package com.gmail.benrcarver.distributed.workload.files;

import com.gmail.benrcarver.distributed.FSOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;

public class FilePoolUtils {
    public static final Logger LOG = LoggerFactory.getLogger(FilePoolUtils.class);
    private static final ThreadLocal<FilePool> filePools = new ThreadLocal<>();
    private static final AtomicInteger filePoolCount = new AtomicInteger(0);

    public static String getPath(FSOperation opType, FilePool filePool) {
        String path;
        if (opType == FSOperation.FILE_INFO) {
            path = filePool.getFileToInfo();
        } else if (opType == FSOperation.DIR_INFO) {
            path = filePool.getDirToInfo();
        } else if (opType == FSOperation.LIST_DIR_NO_PRINT) {
            path = filePool.getDirToStat();
        } else if (opType == FSOperation.LIST_FILE_NO_PRINT) {
            path = filePool.getFileToStat();
        } else if (opType == FSOperation.READ_FILE) {
            path = filePool.getFileToRead();
        } else if (opType == FSOperation.MKDIRS) {
            path = filePool.getDirToCreate();
        } else if (opType == FSOperation.CREATE_FILE) {
            path = filePool.getFileToCreate();
        } else if (opType == FSOperation.DELETE_FILE) {
            path = filePool.getFileToDelete();
        } else if (opType == FSOperation.RENAME_FILE) {
            path = filePool.getFileToRename();
        } else {
            throw new IllegalStateException("Unexpected/unsupported FSOperation specified.");
        }

        return path;
    }

    public static FilePool getFilePool(String baseDir, int dirsPerDir, int filesPerDir) {
        FilePool filePool = filePools.get();
        if (filePool == null) {
            filePool = new FileTreeGenerator(baseDir, filesPerDir, dirsPerDir, 0);
            filePools.set(filePool);
            LOG.debug("New FilePool " +filePool+" created. Total :"+ filePoolCount.incrementAndGet());
        }else{
            LOG.debug("Reusing file pool obj "+filePool);
        }
        return filePool;
    }
}
