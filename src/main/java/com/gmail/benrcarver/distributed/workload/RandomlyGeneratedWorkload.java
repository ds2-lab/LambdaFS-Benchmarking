package com.gmail.benrcarver.distributed.workload;

import com.gmail.benrcarver.distributed.Commands;
import com.gmail.benrcarver.distributed.FSOperation;
import com.gmail.benrcarver.distributed.coin.BMConfiguration;
import com.gmail.benrcarver.distributed.coin.InterleavedMultiFaceCoin;
import com.gmail.benrcarver.distributed.workload.files.FilePool;
import com.gmail.benrcarver.distributed.workload.files.FilePoolUtils;
import com.gmail.benrcarver.distributed.workload.limiter.*;
import org.apache.commons.math3.stat.descriptive.SynchronizedDescriptiveStatistics;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class RandomlyGeneratedWorkload {
    public static final Logger LOG = LoggerFactory.getLogger(RandomlyGeneratedWorkload.class);

    protected final BMConfiguration bmConf;

    private long duration;
    private long startTime = 0;
    AtomicLong operationsCompleted = new AtomicLong(0);
    AtomicLong operationsFailed = new AtomicLong(0);
    Map<FSOperation, AtomicLong> operationsStats = new HashMap<>();
    HashMap<FSOperation, ArrayList<BMOpStats>> opsStats = new HashMap<>();
    SynchronizedDescriptiveStatistics avgLatency = new SynchronizedDescriptiveStatistics();
    private final RateLimiter limiter;
    private FilePool sharedFilePool;
    private final ExecutorService executor;

    public RandomlyGeneratedWorkload(BMConfiguration bmConf) {
        BenchmarkDistribution distribution = bmConf.getInterleavedBMIaTDistribution();
        if (distribution == BenchmarkDistribution.POISSON) {
            limiter = new DistributionRateLimiter(bmConf, new PoissonGenerator(bmConf));
        } else if (distribution == BenchmarkDistribution.PARETO) {
            limiter = new DistributionRateLimiter(bmConf, new ParetoGenerator(bmConf));
        } else {
            limiter = new RateNoLimiter();
        }

        this.bmConf = bmConf;
        this.executor = Executors.newFixedThreadPool(bmConf.getThreadsPerWorker());
    }

    /*
    protected WarmUpCommand.Response warmUp(WarmUpCommand.Request cmd)
            throws IOException, InterruptedException {
        // Warm-up is done in two stages.
        // In the first phase all the parent dirs are created
        // and then in the second stage we create the further
        // file/dir in the parent dir.

        if (bmConf.getFilesToCreateInWarmUpPhase() > 1) {
            List<Callable<Boolean>> workers = new ArrayList<>();
            // Stage 1
            for (int i = 0; i < bmConf.getThreadsPerWorker(); i++) {
                Callable<Boolean> worker = new WarmUp(1, bmConf, "Warming up. Stage1: Creating Parent Dirs. ");
                workers.add(worker);
            }
            executor.invokeAll(workers); // blocking call
            workers.clear();

            // Stage 2
            for (int i = 0; i < bmConf.getThreadsPerWorker(); i++) {
                Callable worker = new WarmUp(bmConf.getFilesToCreateInWarmUpPhase() - 1,
                        bmConf, "Warming up. Stage2: Creating files/dirs. ");
                workers.add(worker);
            }
            executor.invokeAll(workers); // blocking call
            LOG.info("Finished. Warmup Phase. Created ("+bmConf.getThreadsPerWorker()+"*"+bmConf.getFilesToCreateInWarmUpPhase()+") = "+
                    (bmConf.getThreadsPerWorker()*bmConf.getFilesToCreateInWarmUpPhase())+" files. ");
            workers.clear();
        }

        return new NamespaceWarmUp.Response();
    }

    @Override
    public String toString() {
        return "RandomlyGeneratedWorkload(startTime=" + startTime + ", duration=" + duration + ")";
    }

    protected BenchmarkCommand.Response processCommandInternal(BenchmarkCommand.Request command) throws IOException, InterruptedException {
        BMConfiguration config = ((InterleavedBenchmarkCommand.Request) command).getConfig();

        duration = config.getInterleavedBmDuration();
        LOG.info("Starting " + command.getBenchMarkType() + " for duration " + duration);
        List workers = new ArrayList<Worker>();
        // Add limiter as a worker if supported
        WorkerRateLimiter workerLimiter = null;
        if (limiter instanceof WorkerRateLimiter) {
            workerLimiter = (WorkerRateLimiter) limiter;
            workers.add(workerLimiter);
        }
        for (int i = 0; i < bmConf.getThreadsPerWorker(); i++) {
            Callable worker = new Worker(config);
            workers.add(worker);
        }
        startTime = System.currentTimeMillis();
        if (workerLimiter != null) {
            workerLimiter.setStart(startTime);
            workerLimiter.setDuration(duration);
            workerLimiter.setStat("completed", operationsCompleted);
        }

        executor.invokeAll(workers); // blocking call

        long totalTime = System.currentTimeMillis() - startTime;

        LOG.info("Finished " + command.getBenchMarkType() + " in " + totalTime);

        double speed = (operationsCompleted.get() / (double) totalTime) * 1000;

        InterleavedBenchmarkCommand.Response response =
                new InterleavedBenchmarkCommand.Response(totalTime, operationsCompleted.get(), operationsFailed.get(),
                        speed, opsStats, avgLatency.getMean(), new ArrayList<String>(), 999);
        return response;
    }

    public class Worker implements Callable<Object> {

        private DistributedFileSystem dfs;
        private FilePool filePool;
        private InterleavedMultiFaceCoin opCoin;
        private BMConfiguration config;

        public Worker(BMConfiguration config) {
            this.config = config;
        }

        @Override
        public Object call() {
            dfs = Commands.getHdfsClient(null);
            try {
                filePool = (FilePool)sharedFilePool.clone();
            } catch (CloneNotSupportedException e) {
                filePool = FilePoolUtils.getFilePool(bmConf.getBaseDir(),
                        bmConf.getDirPerDir(), bmConf.getFilesPerDir());
            }

            opCoin = new InterleavedMultiFaceCoin(config.getInterleavedBmCreateFilesPercentage(),
                    config.getInterleavedBmAppendFilePercentage(),
                    config.getInterleavedBmReadFilesPercentage(),
                    config.getInterleavedBmRenameFilesPercentage(),
                    config.getInterleavedBmDeleteFilesPercentage(),
                    config.getInterleavedBmLsFilePercentage(),
                    config.getInterleavedBmLsDirPercentage(),
                    config.getInterleavedBmChmodFilesPercentage(),
                    config.getInterleavedBmChmodDirsPercentage(),
                    config.getInterleavedBmMkdirPercentage(),
                    config.getInterleavedBmSetReplicationPercentage(),
                    config.getInterleavedBmGetFileInfoPercentage(),
                    config.getInterleavedBmGetDirInfoPercentage(),
                    config.getInterleavedBmFileChangeOwnerPercentage(),
                    config.getInterleavedBmDirChangeOwnerPercentage()
            );
            while (true) {
                try {
                    if ((System.currentTimeMillis() - startTime) > duration) {
                        return null;
                    }

                    FSOperation op = opCoin.flip();

                    // Wait for the limiter to allow the operation
                    if (!limiter.checkRate()) {
                        return null;
                    }

                    performOperation(op);

                } catch (Exception e) {
                    LOG.error("Exception encountered:", e);
                }
            }
        }

        private void performOperation(FSOperation opType) throws IOException {
            if (LOG.isDebugEnabled())
                LOG.debug("Performing operation: " + opType.getName());
            String path = FilePoolUtils.getPath(opType, filePool);
            if (path != null) {
                boolean retVal = false;
                long opExeTime = 0;
                long opStartTime = System.nanoTime();
                try {
                    opType.call(dfs, path, "");
                    opExeTime = System.nanoTime() - opStartTime;
                    retVal = true;
                } catch (Exception e) {
                    LOG.error("Exception encountered:", e);
                }
                updateStats(opType, retVal, new BMOpStats(opStartTime, opExeTime));
            } else {
                LOG.debug("Could not perform operation " + opType + ". Got Null from the file pool");
            }
        }

        private void updateStats(FSOperation opType, boolean success, BMOpStats stats) {
            AtomicLong stat = operationsStats.get(opType);
            if (stat == null) { // this should be synchronized to get accurate stats. However, this will slow down and these stats are just for log messages. Some inconsistencies are OK
                stat = new AtomicLong(0);
                operationsStats.put(opType, stat);
            }
            stat.incrementAndGet();

            if (success) {
                operationsCompleted.incrementAndGet();
                avgLatency.addValue(stats.OpDuration);
            } else {
                operationsFailed.incrementAndGet();
            }

        }
    }

    private double speedPSec(long ops, long startTime) {
        long timePassed = (System.currentTimeMillis() - startTime);
        double opsPerMSec = (double) (ops) / (double) timePassed;
        return opsPerMSec * 1000;
    }
    */
}