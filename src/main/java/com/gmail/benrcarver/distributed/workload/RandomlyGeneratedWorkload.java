package com.gmail.benrcarver.distributed.workload;

import com.gmail.benrcarver.distributed.Commands;
import com.gmail.benrcarver.distributed.Constants;
import com.gmail.benrcarver.distributed.DistributedBenchmarkResult;
import com.gmail.benrcarver.distributed.FSOperation;
import com.gmail.benrcarver.distributed.coin.BMConfiguration;
import com.gmail.benrcarver.distributed.coin.InterleavedMultiFaceCoin;
import com.gmail.benrcarver.distributed.workload.files.FilePool;
import com.gmail.benrcarver.distributed.workload.files.FilePoolUtils;
import com.gmail.benrcarver.distributed.workload.limiter.*;
import io.hops.metrics.OperationPerformed;
import org.apache.commons.math3.stat.descriptive.SynchronizedDescriptiveStatistics;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

// TODO: Commander actually start and drive workload.
// TODO: Collect timeline latency & throughput data.

public class RandomlyGeneratedWorkload {
    public static final Logger LOG = LoggerFactory.getLogger(RandomlyGeneratedWorkload.class);

    public enum WorkloadState {
            CREATED, WARMING_UP, READY, EXECUTING, FINISHED, ERRED
    };

    protected final BMConfiguration bmConf;

    private long duration;
    private volatile long startTime = 0;
    AtomicInteger operationsCompleted = new AtomicInteger(0);
    AtomicLong operationsFailed = new AtomicLong(0);
    Map<FSOperation, AtomicLong> operationsStats = new HashMap<>();
    SynchronizedDescriptiveStatistics avgLatency = new SynchronizedDescriptiveStatistics();
    private final RateLimiter limiter;
    private final ExecutorService executor;
    private final double percentType2Workers;

    private WorkloadState currentState = WorkloadState.CREATED;

    private final SynchronizedDescriptiveStatistics tcpLatency = new SynchronizedDescriptiveStatistics();
    private final SynchronizedDescriptiveStatistics httpLatency = new SynchronizedDescriptiveStatistics();
    protected BlockingQueue<List<OperationPerformed>> operationsPerformed;

    // Used to synchronize threads; they each connect to HopsFS and then
    // count down. So, they all cannot start until they are all connected.
    final CountDownLatch startLatch;

    // Used to synchronize threads; they block when they finish executing to avoid using CPU cycles
    // by aggregating their results. Once all the threads have finished, they aggregate their results.
    final CountDownLatch endLatch;
    final Semaphore readySemaphore;
    final Semaphore endSemaphore;

    private final DistributedFileSystem sharedHdfs;

    private final String workloadId;

    public RandomlyGeneratedWorkload(BMConfiguration bmConf, DistributedFileSystem sharedHdfs, int numWorkers, String id) {
        BenchmarkDistribution distribution = bmConf.getInterleavedBMIaTDistribution();
        if (distribution == BenchmarkDistribution.POISSON) {
            limiter = new DistributionRateLimiter(bmConf, new PoissonGenerator(bmConf), numWorkers, id);
        } else if (distribution == BenchmarkDistribution.PARETO) {
            limiter = new DistributionRateLimiter(bmConf, new ParetoGenerator(bmConf), numWorkers, id);
        } else {
            limiter = new RateNoLimiter();
        }

        int numThreads = bmConf.getThreadsPerWorker();

        this.workloadId = id;
        this.bmConf = bmConf;
        this.executor = Executors.newFixedThreadPool(numThreads + 1);
        this.sharedHdfs = sharedHdfs;

        endSemaphore = new Semaphore((numThreads * -1) + 1);
        readySemaphore = new Semaphore((numThreads * -1) + 1);
        endLatch = new CountDownLatch(numThreads);
        startLatch = new CountDownLatch(numThreads + 1);

        percentType2Workers = bmConf.getPercentWorkersType2();

        operationsPerformed = new ArrayBlockingQueue<>(bmConf.getThreadsPerWorker());
    }

    public void closeLimiterFileStream() throws IOException {
        if (this.limiter instanceof DistributionRateLimiter) {
            ((DistributionRateLimiter)limiter).closeFileStream();
        }
    }

    public void doWarmup() throws InterruptedException {
        LOG.debug("Beginning warm-up for random workload in approximately 2 seconds.");
        TimeUnit.MILLISECONDS.sleep(2000);

        currentState = WorkloadState.WARMING_UP;

        if (bmConf.getFilesToCreateInWarmUpPhase() > 1) {
            List<Callable<Boolean>> workers = new ArrayList<>();

            int numThreads = 1;
            while (numThreads <= bmConf.getThreadsPerWorker()) {
                LOG.info("Creating " + numThreads + " workers now...");

                if (numThreads > 128)
                    throw new IllegalStateException("Attempting to create too many threads: " + numThreads);

                for (int i = 0; i < numThreads; i++) {
                    Callable<Boolean> worker = new WarmUpWorker(1, bmConf,
                            "Warming up. Stage0: Warming up clients. ", sharedHdfs);
                    workers.add(worker);
                }

                executor.invokeAll(workers); // blocking call
                workers.clear();

                if (numThreads == 1)
                    numThreads = 8;
                else
                    numThreads += 8;
            }

            LOG.info("Finished initial warm-up. Moving onto Stage 1 of Warm-Up: Creating Parent Dirs.");

            LOG.debug("Creating " + bmConf.getFilesToCreateInWarmUpPhase() + " files/directories.");

            for (int i = 0; i < bmConf.getThreadsPerWorker(); i++) {
                Callable<Boolean> worker = new WarmUpWorker(1, bmConf,
                        "Warming up. Stage1: Creating Parent Dirs. ", sharedHdfs);
                workers.add(worker);
            }

            executor.invokeAll(workers); // blocking call
            workers.clear();

            LOG.info("Finished creating parent dirs. Moving onto Stage 2.");

            // Stage 2
            for (int i = 0; i < bmConf.getThreadsPerWorker(); i++) {
                Callable<Boolean> worker = new WarmUpWorker(bmConf.getFilesToCreateInWarmUpPhase() - 1,
                        bmConf, "Warming up. Stage2: Creating files/dirs. ", sharedHdfs);
                workers.add(worker);
            }
            executor.invokeAll(workers); // blocking call
            LOG.debug("Finished. Warmup Phase. Created ("+bmConf.getThreadsPerWorker()+"*"+bmConf.getFilesToCreateInWarmUpPhase()+") = "+
                    (bmConf.getThreadsPerWorker()*bmConf.getFilesToCreateInWarmUpPhase())+" files. ");
            workers.clear();
        }

        currentState = WorkloadState.READY;

        LOG.debug("Warm-up completed.");
        TimeUnit.MILLISECONDS.sleep(500);
    }

    public DistributedBenchmarkResult doWorkload(String opId) throws InterruptedException, ExecutionException {
        currentState = WorkloadState.EXECUTING;

        duration = bmConf.getInterleavedBmDuration();
        LOG.info("Executing randomly-generated workload " + opId + " for duration " + duration + " ms.");
        List<Callable<Object>> workers = new ArrayList<>();
        // Add limiter as a worker if supported
        WorkerRateLimiter workerLimiter = null;
        if (limiter instanceof WorkerRateLimiter) {
            workerLimiter = (WorkerRateLimiter) limiter;
            workers.add(workerLimiter);
        }

        int numWorkerThreads = bmConf.getThreadsPerWorker();
        int numType2Workers = (int)(numWorkerThreads * percentType2Workers);
        int numType1Workers = numWorkerThreads - numType2Workers;

        assert(numType1Workers + numType2Workers == numWorkerThreads);

        LOG.info("Creating a total of " + (numType2Workers + numType1Workers) + " worker thread(s).");
        LOG.info("There will be " + numType2Workers + " Type 2 workers.");
        LOG.info("There will be " + numType1Workers + " Type 1 workers.\n");

        LOG.info("Type 1 Worker Percentages:");
        LOG.info("CREATE: " + bmConf.getInterleavedBmCreateFilesPercentage());
        LOG.info("RENAME: " + bmConf.getInterleavedBmRenameFilesPercentage());
        LOG.info("DELETE: " + bmConf.getInterleavedBmDeleteFilesPercentage());
        LOG.info("MKDIR: " + bmConf.getInterleavedBmMkdirPercentage());
        LOG.info("LS DIR: " + bmConf.getInterleavedBmLsDirPercentage());
        LOG.info("LS FILE: " + bmConf.getInterleavedBmLsFilePercentage());
        LOG.info("STAT FILE: " + bmConf.getInterleavedBmGetFileInfoPercentage());
        LOG.info("STAT DIR: " + bmConf.getInterleavedBmGetDirInfoPercentage());
        LOG.info("READ: " + bmConf.getInterleavedBmReadFilesPercentage() + "\n");

        LOG.info("Type 2 Worker Percentages:");
        LOG.info("CREATE2: " + bmConf.getInterleavedBmCreateFilesPercentage2());
        LOG.info("RENAME2: " + bmConf.getInterleavedBmRenameFilesPercentage2());
        LOG.info("DELETE2: " + bmConf.getInterleavedBmDeleteFilesPercentage2());
        LOG.info("MKDIR2: " + bmConf.getInterleavedBmMkdirPercentage2());
        LOG.info("LS DIR2: " + bmConf.getInterleavedBmLsDirPercentage2());
        LOG.info("LS FILE2: " + bmConf.getInterleavedBmLsFilePercentage2());
        LOG.info("STAT FILE2: " + bmConf.getInterleavedBmGetFileInfoPercentage2());
        LOG.info("STAT DIR2: " + bmConf.getInterleavedBmGetDirInfoPercentage2());
        LOG.info("READ2: " + bmConf.getInterleavedBmReadFilesPercentage2() + "\n");

        for (int i = 0; i < numType2Workers; i++) {
            Callable<Object> worker = new Worker(bmConf, true);
            workers.add(worker);
        }

        for (int i = 0; i < numType1Workers; i++) {
            Callable<Object> worker = new Worker(bmConf, false);
            workers.add(worker);
        }

        if (workerLimiter != null) {
            workerLimiter.setStart(startTime);
            workerLimiter.setDuration(duration);
            workerLimiter.setStat("completed", operationsCompleted);
        }

        List<Future<Object>> futures = new ArrayList<>();
        for (Callable<Object> worker : workers) {
            Future<Object> future = executor.submit(worker);
            futures.add(future);
        }

        LOG.debug("Main thread acquiring 'ready' semaphore...");
        readySemaphore.acquire();                   // Will block until all client threads are ready to go.
        LOG.debug("Main thread acquired 'ready' semaphore!");
        TimeUnit.MILLISECONDS.sleep(250);
        LOG.debug("Starting workload NOW.");
        TimeUnit.MILLISECONDS.sleep(250);
        startTime = System.currentTimeMillis();     // Start the clock.
        startLatch.countDown();                     // Let the threads start.

        boolean acquired = false;
        try {
            acquired = endSemaphore.tryAcquire(duration * 2, TimeUnit.MILLISECONDS);
        } catch (InterruptedException ex) {
            LOG.error("Interrupted while waiting for workload to complete:", ex);
        }
        long endTime = System.currentTimeMillis();
        long totalTime = endTime - startTime;
        // executor.invokeAll(workers); // blocking call

        if (!acquired)
            LOG.error("Timed-out waiting for workload after " + totalTime + " milliseconds.");
        else
            LOG.info("Randomly-generated workload ended after " + totalTime + " ms.");

        int numFailed = 0;
        if (acquired) {
            for (Future<Object> future : futures) {
                future.get();
            }
        } else {
            int numSucceeded = 0;
            for (Future<Object> future : futures) {
                if (future.isDone() && !future.isCancelled()) {
                    numSucceeded++;
                    future.get(); // Not really necessary.
                } else {
                    numFailed++;
                }
            }

            if (numFailed > 0) {
                LOG.error(numFailed + " worker" + (numFailed == 1 ? " " : "s ") + "failed to complete successfully.");
            }
            LOG.info(numSucceeded + "/" + (numFailed + numSucceeded) + " worker(s) completed successfully.");
        }
        
        List<OperationPerformed> allOpsPerformed = new ArrayList<>();

        for (List<OperationPerformed> opsPerformed : operationsPerformed) {
            allOpsPerformed.addAll(opsPerformed);
        }

        LOG.info("Size of opsPerformed list: " + allOpsPerformed.size());

        DistributedBenchmarkResult result = new DistributedBenchmarkResult(opId, Constants.OP_PREPARE_GENERATED_WORKLOAD,
                operationsCompleted.get(), totalTime / 1.0e3, startTime, endTime, 0, 0,
                allOpsPerformed.toArray(new OperationPerformed[0]), null, tcpLatency, httpLatency, numFailed);

        currentState = WorkloadState.FINISHED;
        return result;
    }

    public WorkloadState getCurrentState() { return this.currentState; }

    @Override
    public String toString() {
        return "RandomlyGeneratedWorkload(startTime=" + startTime + ", duration=" + duration + ")";
    }

    public class Worker implements Callable<Object> {
        private FilePool filePool;
        private final BMConfiguration config;
        private final boolean isType2;

        public Worker(BMConfiguration config) {
            this(config, true);
        }

        public Worker(BMConfiguration config, boolean isType2) {
            this.config = config;
            this.isType2 = isType2;
        }

        private void extractMetrics(DistributedFileSystem dfs) throws InterruptedException {
            endLatch.countDown();

            try {
                endLatch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            for (double latency : dfs.getLatencyHttpStatistics().getValues()) {
                httpLatency.addValue(latency);
            }

            //if (LOG.isDebugEnabled()) LOG.debug("[THREAD " + threadId + "] Collecting TCP latencies.");
            for (double latency : dfs.getLatencyTcpStatistics().getValues()) {
                tcpLatency.addValue(latency);
            }

            operationsPerformed.add(dfs.getOperationsPerformed());

            // Clear the metric data associated with the client and return it to the pool.
            Commands.clearMetricDataNoPrompt(dfs);
            Commands.returnHdfsClient(dfs);
        }

        /**
         * Return a coin created from the configuration file based on whether this is a Type 1 or Type 2 worker.
         *
         * Type 1 and Type 2 workers simply have different percentages for the operations they perform.
         */
        private InterleavedMultiFaceCoin getCoin() {
            if (isType2) {
                return new InterleavedMultiFaceCoin(config.getInterleavedBmCreateFilesPercentage2(),
                        config.getInterleavedBmAppendFilePercentage2(),
                        config.getInterleavedBmReadFilesPercentage2(),
                        config.getInterleavedBmRenameFilesPercentage2(),
                        config.getInterleavedBmDeleteFilesPercentage2(),
                        config.getInterleavedBmLsFilePercentage2(),
                        config.getInterleavedBmLsDirPercentage2(),
                        config.getInterleavedBmChmodFilesPercentage2(),
                        config.getInterleavedBmChmodDirsPercentage2(),
                        config.getInterleavedBmMkdirPercentage2(),
                        config.getInterleavedBmSetReplicationPercentage2(),
                        config.getInterleavedBmGetFileInfoPercentage2(),
                        config.getInterleavedBmGetDirInfoPercentage2(),
                        config.getInterleavedBmFileChangeOwnerPercentage2(),
                        config.getInterleavedBmDirChangeOwnerPercentage2()
                );
            } else {
                return new InterleavedMultiFaceCoin(config.getInterleavedBmCreateFilesPercentage(),
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
            }
        }

        @Override
        public Object call() throws FileNotFoundException {
            DistributedFileSystem dfs = Commands.getHdfsClient(sharedHdfs, false);
            filePool = FilePoolUtils.getFilePool(bmConf.getBaseDir(), bmConf.getDirPerDir(), bmConf.getFilesPerDir(),
                    bmConf.getTreeDepth(), bmConf.isFixedDepthTree(), bmConf.isExistingSubtree(), bmConf.getExistingSubtreePath());

            InterleavedMultiFaceCoin opCoin = getCoin();

            LOG.debug("Acquiring 'ready' semaphore now...");
            readySemaphore.release(); // Ready to start. Once all threads have done this, the timer begins.

            LOG.debug("Acquired 'ready' semaphore. Counting down start latch now...");
            startLatch.countDown(); // Wait for the main thread's signal to actually begin.

            try {
                LOG.debug("Awaiting 'start' latch now...");
                startLatch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            LOG.debug("Go go go!");

            int numOperations = 0;
            while (true) {
                for (int i = 0; i < 5000; i++) {
                    try {
                        if ((System.currentTimeMillis() - startTime) > duration) {
                            // This way, we don't have to wait for all the statistics to be added to lists and whatnot.
                            // As soon as the threads finish, they call release() on the endSemaphore. Once all threads have
                            // done this, we designate the benchmark as ended and record the stop time. Then we join the threads
                            // so that all the statistics are placed into the appropriate collections where we can aggregate them.
                            endSemaphore.release();
                            extractMetrics(dfs);
                            return null;
                        }

                        FSOperation op = opCoin.flip();

                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Generated " + op.getName() + " operation! Completed " + numOperations +
                                    " operations so far. Running average throughput: " +
                                    (numOperations / ((System.currentTimeMillis() - startTime) / 1.0e3)));
                        }

                        // Wait for the limiter to allow the operation
                        if (!limiter.checkRate()) {
                            // This way, we don't have to wait for all the statistics to be added to lists and whatnot.
                            // As soon as the threads finish, they call release() on the endSemaphore. Once all threads have
                            // done this, we designate the benchmark as ended and record the stop time. Then we join the threads
                            // so that all the statistics are placed into the appropriate collections where we can aggregate them.
                            endSemaphore.release();
                            extractMetrics(dfs);
                            return null;
                        }

                        performOperation(op, dfs);
                        numOperations++;
                    } catch (Exception e) {
                        LOG.error("Exception encountered:", e);
                    }
                }

                if (LOG.isDebugEnabled())
                    LOG.debug("Completed " + numOperations + " operations so far. Time elapsed: " +
                            (System.currentTimeMillis() - startTime) + " ms. Running average throughput: " +
                            (numOperations / ((System.currentTimeMillis() - startTime) / 1.0e3)));
            }
        }

        private static final String RENAMED = "R0N0";
        private void performOperation(FSOperation operation, DistributedFileSystem dfs) {
            if (LOG.isDebugEnabled())
                LOG.debug("Performing operation: " + operation.getName());
            String path = FilePoolUtils.getPath(operation, filePool);
            if (path != null) {
                boolean retVal;
                try {
                    if (operation == FSOperation.RENAME_FILE) {
                        int currentCounter = 0;
                        String to = path;
                        if (path.contains(RENAMED)) {
                            int index1 = path.lastIndexOf(RENAMED);
                            int index2 = path.lastIndexOf("_");
                            String counter = path.substring(index1 + RENAMED.length() + 1, index2);
                            to = path.substring(0, index1 - 1);
                            currentCounter = Integer.parseInt(counter);
                        }
                        currentCounter++;
                        to = to + "_" + RENAMED + "_" + currentCounter + "_" + "Times";
                        retVal = operation.call(dfs, path, to);

                        if (retVal) {
                            operationsCompleted.incrementAndGet();
                            filePool.fileRenamed(path, to);
                        }
                    } else {
                        retVal = operation.call(dfs, path, "");
                        if (retVal) {
                            operationsCompleted.incrementAndGet();
                            if (operation == FSOperation.CREATE_FILE)
                                filePool.fileCreationSucceeded(path);
                        }
                    }
                } catch (Exception e) {
                    LOG.error("Exception encountered:", e);
                }

                // updateStats(operation, retVal, new BMOpStats(opStartTime, opExeTime));
            } else {
                LOG.debug("Could not perform operation " + operation + ". Got Null from the file pool");
            }
        }

//        private void updateStats(FSOperation opType, boolean success, BMOpStats stats) {
//            AtomicLong stat = operationsStats.get(opType);
//            if (stat == null) {
//                // this should be synchronized to get accurate stats. However, this will slow
//                // down and these stats are just for log messages. Some inconsistencies are OK.
//                stat = new AtomicLong(0);
//                operationsStats.put(opType, stat);
//            }
//            stat.incrementAndGet();
//
//            if (success) {
//                operationsCompleted.incrementAndGet();
//                avgLatency.addValue(stats.OpDuration);
//            } else {
//                operationsFailed.incrementAndGet();
//            }
//
//        }
    }
}