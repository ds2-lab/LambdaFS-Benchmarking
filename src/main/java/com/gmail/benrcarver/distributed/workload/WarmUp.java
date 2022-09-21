package com.gmail.benrcarver.distributed.workload;

import com.gmail.benrcarver.distributed.Commands;
import com.gmail.benrcarver.distributed.FSOperation;
import com.gmail.benrcarver.distributed.coin.BMConfiguration;
import com.gmail.benrcarver.distributed.workload.files.FilePool;
import com.gmail.benrcarver.distributed.workload.files.FilePoolUtils;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.Callable;

public class WarmUp implements Callable<Boolean> {
    public static final Logger LOG = LoggerFactory.getLogger(WarmUp.class);
    
    private DistributedFileSystem dfs;
    private FilePool filePool;
    private final int filesToCreate;
    private final String stage;
    private final BMConfiguration bmConf;
    private final boolean dryrun;

    public WarmUp(int filesToCreate, BMConfiguration bmConf,
                      String stage) throws IOException {
        this.filesToCreate = filesToCreate;
        this.stage = stage;
        this.bmConf = bmConf;
        this.dryrun = bmConf.getBenchmarkDryrun();
    }

    @Override
    public Boolean call() throws Exception {
        try {
            return callImpl();
        }
        catch (Exception e) {
            LOG.debug("Exception in warmup: " + e);
            throw e;
        }
    }

    public boolean callImpl() throws Exception {
        if (!dryrun) {
            dfs = Commands.getHdfsClient(null);
        }
        filePool = FilePoolUtils.getFilePool(bmConf.getBaseDir(), bmConf.getDirPerDir(),
                bmConf.getFilesPerDir());
        String filePath = null;

        LOG.debug("Attempting to create a total of " + filesToCreate + " file(s).");
        for (int i = 0; i < filesToCreate; i++) {
            try {
                filePath = filePool.getFileToCreate();
                LOG.debug("Creating file '" + filePath + "' now...");
                if (!dryrun) {
                    FSOperation.CREATE_FILE.call(dfs, filePath, "");
                    filePool.fileCreationSucceeded(filePath);
                    FSOperation.READ_FILE.call(dfs, filePath, "");
                } else {
                    filePool.fileCreationSucceeded(filePath);
                }
            } catch (Exception e) {
                LOG.error("Exception encountered:", e);
            }
        }

        LOG.debug("Warmed up!");
        return true;
    }
}