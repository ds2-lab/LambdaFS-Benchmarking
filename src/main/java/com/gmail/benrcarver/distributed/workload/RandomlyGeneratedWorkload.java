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
import org.apache.commons.math3.stat.descriptive.SynchronizedDescriptiveStatistics;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private long startTime = 0;
    AtomicInteger operationsCompleted = new AtomicInteger(0);
    AtomicLong operationsFailed = new AtomicLong(0);
    Map<FSOperation, AtomicLong> operationsStats = new HashMap<>();
    HashMap<FSOperation, ArrayList<BMOpStats>> opsStats = new HashMap<>();
    SynchronizedDescriptiveStatistics avgLatency = new SynchronizedDescriptiveStatistics();
    private final RateLimiter limiter;
    private final ExecutorService executor;

    private WorkloadState currentState = WorkloadState.CREATED;

    private SynchronizedDescriptiveStatistics tcpLatency = new SynchronizedDescriptiveStatistics();
    private SynchronizedDescriptiveStatistics httpLatency = new SynchronizedDescriptiveStatistics();

    // Used to synchronize threads; they each connect to HopsFS and then
    // count down. So, they all cannot start until they are all connected.
    final CountDownLatch startLatch;

    // Used to synchronize threads; they block when they finish executing to avoid using CPU cycles
    // by aggregating their results. Once all the threads have finished, they aggregate their results.
    final CountDownLatch endLatch;
    final Semaphore readySemaphore;
    final Semaphore endSemaphore;

    private final DistributedFileSystem sharedHdfs;

    public RandomlyGeneratedWorkload(BMConfiguration bmConf, DistributedFileSystem sharedHdfs) {
        BenchmarkDistribution distribution = bmConf.getInterleavedBMIaTDistribution();
        if (distribution == BenchmarkDistribution.POISSON) {
            limiter = new DistributionRateLimiter(bmConf, new PoissonGenerator(bmConf));
        } else if (distribution == BenchmarkDistribution.PARETO) {
            limiter = new DistributionRateLimiter(bmConf, new ParetoGenerator(bmConf));
        } else {
            limiter = new RateNoLimiter();
        }

        int numThreads = bmConf.getThreadsPerWorker();

        this.bmConf = bmConf;
        this.executor = Executors.newFixedThreadPool(numThreads);
        this.sharedHdfs = sharedHdfs;

        endSemaphore = new Semaphore((numThreads * -1) + 1);
        readySemaphore = new Semaphore((numThreads * -1) + 1);
        endLatch = new CountDownLatch(numThreads);
        startLatch = new CountDownLatch(numThreads + 1);
    }

    public void doWarmup() throws InterruptedException {
        currentState = WorkloadState.WARMING_UP;

        if (bmConf.getFilesToCreateInWarmUpPhase() > 1) {
            List workers = new ArrayList<WarmUp>();

            int numThreads = 1;
            while (numThreads <= bmConf.getThreadsPerWorker()) {
                LOG.info("Creating " + numThreads + " workers now...");
                for (int i = 0; i < numThreads; i++) {
                    Callable worker = new WarmUp(1, bmConf,
                            "Warming up. Stage0: Warming up clients. ", sharedHdfs);
                    workers.add(worker);
                }

                executor.invokeAll(workers); // blocking call
                workers.clear();

                if (numThreads == 1)
                    numThreads = 8;
                else
                    numThreads += 8;

                if (numThreads > 128)
                    throw new IllegalStateException("Attempting to create too many threads: " + numThreads);
            }

            LOG.info("Finished initial warm-up. Moving onto Stage 1 of Warm-Up: Creating Parent Dirs.");

            for (int i = 0; i < bmConf.getThreadsPerWorker(); i++) {
                Callable worker = new WarmUp(1, bmConf,
                        "Warming up. Stage1: Creating Parent Dirs. ", sharedHdfs);
                workers.add(worker);
            }

            executor.invokeAll(workers); // blocking call
            workers.clear();

            LOG.info("Finished creating parent dirs. Moving onto Stage 2.");

            // Stage 2
            for (int i = 0; i < bmConf.getThreadsPerWorker(); i++) {
                Callable worker = new WarmUp(bmConf.getFilesToCreateInWarmUpPhase() - 1,
                        bmConf, "Warming up. Stage2: Creating files/dirs. ", sharedHdfs);
                workers.add(worker);
            }
            executor.invokeAll(workers); // blocking call
            LOG.debug("Finished. Warmup Phase. Created ("+bmConf.getThreadsPerWorker()+"*"+bmConf.getFilesToCreateInWarmUpPhase()+") = "+
                    (bmConf.getThreadsPerWorker()*bmConf.getFilesToCreateInWarmUpPhase())+" files. ");
            workers.clear();
        }

        currentState = WorkloadState.READY;
    }

    public DistributedBenchmarkResult doWorkload(String opId) throws InterruptedException, ExecutionException {
        currentState = WorkloadState.EXECUTING;

        duration = bmConf.getInterleavedBmDuration();
        LOG.info("Executing randomly-generated workload for duration " + duration + " ms.");
        List<Callable<Object>> workers = new ArrayList<>();
        // Add limiter as a worker if supported
        WorkerRateLimiter workerLimiter = null;
        if (limiter instanceof WorkerRateLimiter) {
            workerLimiter = (WorkerRateLimiter) limiter;
            workers.add(workerLimiter);
        }
        for (int i = 0; i < bmConf.getThreadsPerWorker(); i++) {
            Callable<Object> worker = new Worker(bmConf);
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

        readySemaphore.acquire();                   // Will block until all client threads are ready to go.
        startTime = System.currentTimeMillis();     // Start the clock.
        startLatch.countDown();                     // Let the threads start.

        endSemaphore.acquire();
        long endTime = System.currentTimeMillis();
        long totalTime = endTime - startTime;
        // executor.invokeAll(workers); // blocking call

        LOG.info("Finished randomly-generated workload in " + totalTime + " ms.");

        for (Future<Object> future : futures) {
            future.get();
        }

        DistributedBenchmarkResult result = new DistributedBenchmarkResult(opId, Constants.OP_PREPARE_GENERATED_WORKLOAD,
                operationsCompleted.get(), totalTime, startTime, endTime, -1, -1, null,
                null, tcpLatency, httpLatency);

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
        private InterleavedMultiFaceCoin opCoin;
        private BMConfiguration config;

        public Worker(BMConfiguration config) {
            this.config = config;
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

            // First clear the metric data associated with the client.
            Commands.clearMetricDataNoPrompt(dfs);
            Commands.returnHdfsClient(dfs);
        }

        @Override
        public Object call() throws InterruptedException {
            DistributedFileSystem dfs = Commands.getHdfsClient(sharedHdfs);
            filePool = FilePoolUtils.getFilePool(bmConf.getBaseDir(),
                    bmConf.getDirPerDir(), bmConf.getFilesPerDir());

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

            readySemaphore.release(); // Ready to start. Once all threads have done this, the timer begins.

            startLatch.countDown(); // Wait for the main thread's signal to actually begin.

            while (true) {
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

                    // Wait for the limiter to allow the operation
                    if (!limiter.checkRate()) {
                        Commands.returnHdfsClient(dfs);

                        // This way, we don't have to wait for all the statistics to be added to lists and whatnot.
                        // As soon as the threads finish, they call release() on the endSemaphore. Once all threads have
                        // done this, we designate the benchmark as ended and record the stop time. Then we join the threads
                        // so that all the statistics are placed into the appropriate collections where we can aggregate them.
                        endSemaphore.release();
                        extractMetrics(dfs);
                        return null;
                    }

                    performOperation(op, dfs);

                } catch (Exception e) {
                    LOG.error("Exception encountered:", e);
                }
            }
        }

        private void performOperation(FSOperation opType, DistributedFileSystem dfs) {
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
}