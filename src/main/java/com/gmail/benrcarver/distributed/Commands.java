package com.gmail.benrcarver.distributed;

import com.gmail.benrcarver.distributed.util.TreeNode;
import com.gmail.benrcarver.distributed.util.Utils;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;

import io.hops.metrics.TransactionEvent;
import io.hops.metrics.TransactionAttempt;
import io.hops.transaction.context.TransactionsStats;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.commons.math3.stat.descriptive.SynchronizedDescriptiveStatistics;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.fs.FileStatus;
import io.hops.metrics.OperationPerformed;

import static com.gmail.benrcarver.distributed.Constants.OP_DELETE_FILES;
import static com.gmail.benrcarver.distributed.Constants.OP_STRONG_SCALING_READS;

public class Commands {
    public static final Log LOG = LogFactory.getLog(Commands.class);
    private static Scanner scanner = new Scanner(System.in);

    public static void writeFilesToDirectories(DistributedFileSystem hdfs,
                                               final Configuration configuration,
                                               String nameNodeEndpoint)
            throws IOException, InterruptedException {
        System.out.print("Manually input (comma-separated list) [1], or specify file containing directories [2]? \n>");
        int choice = Integer.parseInt(scanner.nextLine());

        List<String> directories = null;
        if (choice == 1) {
            System.out.print("Please enter the directories as a comma-separated list:\n> ");
            String listOfDirectories = scanner.nextLine();
            directories = Arrays.asList(listOfDirectories.split(","));

            if (directories.size() == 1)
                LOG.info("1 directory specified.");
            else
                LOG.info(directories.size() + " directories specified.");
        }
        else if (choice == 2) {
            System.out.print("Please provide path to file containing HopsFS directories:\n> ");
            String filePath = scanner.nextLine();
            directories = Utils.getFilePathsFromFile(filePath);

            if (directories.size() == 1)
                LOG.info("1 directory specified in file.");
            else
                LOG.info(directories.size() + " directories specified in file.");
        }
        else {
            LOG.error("Invalid option specified (" + choice + "). Please enter \"1\" or \"2\" for this prompt.");
        }

        System.out.print("Number of threads? \n>");
        int numberOfThreads = Integer.parseInt(scanner.nextLine());

        int n = 10;
        System.out.print("Number of files per directory (default " + n + "):\n> ");
        try {
            n = Integer.parseInt(scanner.nextLine());
        } catch (NumberFormatException ex) {
            LOG.info("Defaulting to " + n + ".");
        }

        int minLength = 0;
        System.out.print("Min string length (default " + minLength + "):\n> ");
        try {
            minLength = Integer.parseInt(scanner.nextLine());
        } catch (NumberFormatException ex) {
            LOG.info("Defaulting to " + minLength + ".");
        }

        int maxLength = 0;
        System.out.print("Max string length (default " + maxLength + "):\n> ");
        try {
            maxLength = Integer.parseInt(scanner.nextLine());
        } catch (NumberFormatException ex) {
            LOG.info("Defaulting to " + maxLength + ".");
        }

        assert directories != null;
        writeFilesInternal(n, minLength, maxLength, numberOfThreads, directories, hdfs, configuration, nameNodeEndpoint);
    }

    public static void clearStatisticsPackages(DistributedFileSystem hdfs) {
        System.out.print("Are you sure? (y/N)\n> ");
        String input = scanner.nextLine();

        if (input.equalsIgnoreCase("y")) {
            hdfs.clearStatistics(true, true, true);
            hdfs.clearLatencyValues();

            LOG.debug("Cleared both statistics and latency values.");
        } else {
            LOG.info("NOT clearing statistics packages.");
        }
    }

    /**
     * Gets the user inputs for this benchmark, then calls the actual benchmark itself.
     */
    public static DistributedBenchmarkResult strongScalingBenchmark(final Configuration configuration,
                                                final DistributedFileSystem sharedHdfs,
                                                final String nameNodeEndpoint,
                                                int n, int readsPerFile, int numThreads, String inputPath)
            throws FileNotFoundException, InterruptedException {
        List<String> paths = Utils.getFilePathsFromFile(inputPath);

        if (paths.size() < n) {
            LOG.error("ERROR: The file should contain at least " + n +
                    " HopsFS file path(s); however, it contains just " + paths.size() + " HopsFS file path(s).");
            LOG.error("Aborting operation.");
            return null;
        }

        // Select a random subset of size n, where n is the number of files each thread should rea.d
        Collections.shuffle(paths);
        List<String> selectedPaths = paths.subList(0, n);

        Thread[] threads = new Thread[numThreads];

        // Used to synchronize threads; they each connect to HopsFS and then
        // count down. So, they all cannot start until they are all connected.
        final CountDownLatch startLatch = new CountDownLatch(numThreads);
        final Semaphore endSemaphore = new Semaphore((numThreads * -1) + 1);

        final BlockingQueue<List<OperationPerformed>> operationsPerformed =
                new java.util.concurrent.ArrayBlockingQueue<>(numThreads);
        final BlockingQueue<HashMap<String, TransactionsStats.ServerlessStatisticsPackage>> statisticsPackages
                = new ArrayBlockingQueue<>(numThreads);
        final BlockingQueue<HashMap<String, List<TransactionEvent>>> transactionEvents
                = new ArrayBlockingQueue<>(numThreads);

        final SynchronizedDescriptiveStatistics latencyHttp = new SynchronizedDescriptiveStatistics();
        final SynchronizedDescriptiveStatistics latencyTcp = new SynchronizedDescriptiveStatistics();
        final SynchronizedDescriptiveStatistics latencyBoth = new SynchronizedDescriptiveStatistics();

        for (int i = 0; i < numThreads; i++) {
            Thread thread = new Thread(() -> {
                DistributedFileSystem hdfs = Commander.initDfsClient(nameNodeEndpoint);

                startLatch.countDown();

                for (String filePath : selectedPaths) {
                    for (int j = 0; j < readsPerFile; j++)
                        readFile(filePath, hdfs, nameNodeEndpoint);
                }

                // This way, we don't have to wait for all the statistics to be added to lists and whatnot.
                // As soon as the threads finish, they call release() on the endSemaphore. Once all threads have
                // done this, we designate the benchmark as ended and record the stop time. Then we join the threads
                // so that all the statistics are placed into the appropriate collections where we can aggregate them.
                endSemaphore.release();

                operationsPerformed.add(hdfs.getOperationsPerformed());
                statisticsPackages.add(hdfs.getStatisticsPackages());
                transactionEvents.add(hdfs.getTransactionEvents());

                for (double latency : hdfs.getLatencyStatistics().getValues()) {
                    latencyBoth.addValue(latency);
                }

                for (double latency : hdfs.getLatencyHttpStatistics().getValues()) {
                    latencyHttp.addValue(latency);
                }

                for (double latency : hdfs.getLatencyTcpStatistics().getValues()) {
                    latencyTcp.addValue(latency);
                }

                try {
                    hdfs.close();
                } catch (IOException ex) {
                    LOG.error("Encountered IOException while closing DistributedFileSystem object:", ex);
                }
            });
            threads[i] = thread;
        }

        LOG.info("Starting threads.");
        long start = System.currentTimeMillis();
        for (Thread thread : threads) {
            thread.start();
        }

        // This way, we don't have to wait for all the statistics to be added to lists and whatnot.
        // As soon as the threads finish, they call release() on the endSemaphore. Once all threads have
        // done this, we designate the benchmark as ended and record the stop time. Then we join the threads
        // so that all the statistics are placed into the appropriate collections where we can aggregate them.
        endSemaphore.acquire();
        long end = System.currentTimeMillis();

        LOG.info("Benchmark completed in " + (end - start) + "ms. Joining threads now...");
        for (Thread thread : threads) {
            thread.join();
        }

        for (List<OperationPerformed> opsPerformed : operationsPerformed) {
            //LOG.info("Adding list of " + opsPerformed.size() + " operations performed to master/shared HDFS object.");
            sharedHdfs.addOperationPerformeds(opsPerformed);
        }

        for (HashMap<String, TransactionsStats.ServerlessStatisticsPackage> statPackages : statisticsPackages) {
            //LOG.info("Adding list of " + statPackages.size() + " statistics packages to master/shared HDFS object.");
            sharedHdfs.mergeStatisticsPackages(statPackages, true);
        }

        for (HashMap<String, List<TransactionEvent>> txEvents : transactionEvents) {
            // LOG.info("Merging " + txEvents.size() + " new transaction event(s) into master/shared HDFS object.");
            sharedHdfs.mergeTransactionEvents(txEvents, true);
        }

        double durationSeconds = (end - start) / 1000.0;
        double totalReads = (double)n * (double)readsPerFile * (double)numThreads;
        double throughput = (totalReads / durationSeconds);

        LOG.info("Latency TCP & HTTP (ms) [min: " + latencyBoth.getMin() + ", max: " + latencyBoth.getMax() +
                ", avg: " + latencyBoth.getMean() + ", std dev: " + latencyBoth.getStandardDeviation() +
                ", N: " + latencyBoth.getN() + "]");
        LOG.info("Latency TCP (ms) [min: " + latencyTcp.getMin() + ", max: " + latencyTcp.getMax() +
                ", avg: " + latencyTcp.getMean() + ", std dev: " + latencyTcp.getStandardDeviation() +
                ", N: " + latencyTcp.getN() + "]");
        LOG.info("Latency HTTP (ms) [min: " + latencyHttp.getMin() + ", max: " + latencyHttp.getMax() +
                ", avg: " + latencyHttp.getMean() + ", std dev: " + latencyHttp.getStandardDeviation() +
                ", N: " + latencyHttp.getN() + "]");
        LOG.info("Finished performing all " + totalReads + " file reads in " + durationSeconds);
        LOG.info("Throughput: " + throughput + " ops/sec.");

        sharedHdfs.addLatencies(latencyTcp.getValues(), latencyHttp.getValues());

        return new DistributedBenchmarkResult(null, OP_STRONG_SCALING_READS, (int)totalReads, durationSeconds,
                start, end);
    }



    /**
     * Read N files using N threads (so, each thread reads 1 file). Each file is read a specified number of times.
     *
     * This is the WEAK SCALING (read) benchmark.
     *
     * @param n Number of files to read.
     * @param readsPerFile How many times each file should be read.
     * @param inputPath Path to local file containing HopsFS filepaths (of the files to read).
     */
    public static DistributedBenchmarkResult readNFiles(final Configuration configuration,
                                                        final DistributedFileSystem sharedHdfs,
                                                        final String nameNodeEndpoint,
                                                        int n,int readsPerFile, String inputPath, boolean shuffle)
            throws InterruptedException, FileNotFoundException {
        List<String> paths = Utils.getFilePathsFromFile(inputPath);

        if (shuffle) {
            LOG.debug("Shuffling paths.");
            Collections.shuffle(paths);
        }

        if (paths.size() < n) {
            LOG.error("ERROR: The file should contain at least " + n +
                    " HopsFS file path(s); however, it contains just " + paths.size() + " HopsFS file path(s).");
            LOG.error("Aborting operation.");
            return null;
        }

        Thread[] threads = new Thread[n];

        // Used to synchronize threads; they each connect to HopsFS and then
        // count down. So, they all cannot start until they are all connected.
        final CountDownLatch latch = new CountDownLatch(n);
        final Semaphore endSemaphore = new Semaphore((n * -1) + 1);

        final java.util.concurrent.BlockingQueue<List<OperationPerformed>> operationsPerformed =
                new java.util.concurrent.ArrayBlockingQueue<>(n);
        final BlockingQueue<HashMap<String, TransactionsStats.ServerlessStatisticsPackage>> statisticsPackages
                = new ArrayBlockingQueue<>(n);
        final BlockingQueue<HashMap<String, List<TransactionEvent>>> transactionEvents
                = new ArrayBlockingQueue<>(n);

        final SynchronizedDescriptiveStatistics latencyHttp = new SynchronizedDescriptiveStatistics();
        final SynchronizedDescriptiveStatistics latencyTcp = new SynchronizedDescriptiveStatistics();
        final SynchronizedDescriptiveStatistics latencyBoth = new SynchronizedDescriptiveStatistics();

        for (int i = 0; i < n; i++) {
            final String filePath = paths.get(i);
            Thread thread = new Thread(() -> {
                DistributedFileSystem hdfs = Commander.initDfsClient(nameNodeEndpoint);

                latch.countDown();

                for (int j = 0; j < readsPerFile; j++)
                    readFile(filePath, hdfs, nameNodeEndpoint);

                // This way, we don't have to wait for all the statistics to be added to lists and whatnot.
                // As soon as the threads finish, they call release() on the endSemaphore. Once all threads have
                // done this, we designate the benchmark as ended and record the stop time. Then we join the threads
                // so that all the statistics are placed into the appropriate collections where we can aggregate them.
                endSemaphore.release();

                operationsPerformed.add(hdfs.getOperationsPerformed());
                statisticsPackages.add(hdfs.getStatisticsPackages());
                transactionEvents.add(hdfs.getTransactionEvents());

                for (double latency : hdfs.getLatencyStatistics().getValues()) {
                    latencyBoth.addValue(latency);
                }

                for (double latency : hdfs.getLatencyHttpStatistics().getValues()) {
                    latencyHttp.addValue(latency);
                }

                for (double latency : hdfs.getLatencyTcpStatistics().getValues()) {
                    latencyTcp.addValue(latency);
                }

                try {
                    hdfs.close();
                } catch (IOException ex) {
                    LOG.error("Encountered IOException while closing DistributedFileSystem object:", ex);
                }
            });
            threads[i] = thread;
        }

        LOG.info("Starting threads.");
        long start = System.currentTimeMillis();
        for (Thread thread : threads) {
            thread.start();
        }

        // This way, we don't have to wait for all the statistics to be added to lists and whatnot.
        // As soon as the threads finish, they call release() on the endSemaphore. Once all threads have
        // done this, we designate the benchmark as ended and record the stop time. Then we join the threads
        // so that all the statistics are placed into the appropriate collections where we can aggregate them.
        endSemaphore.acquire();
        long end = System.currentTimeMillis();

        LOG.info("Benchmark completed in " + (end - start) + "ms. Joining threads now...");
        for (Thread thread : threads) {
            thread.join();
        }

        int totalCacheHits = 0;
        int totalCacheMisses = 0;

        for (List<OperationPerformed> opsPerformed : operationsPerformed) {
            sharedHdfs.addOperationPerformeds(opsPerformed);

            for (OperationPerformed op : opsPerformed) {
                totalCacheHits += op.getMetadataCacheHits();
                totalCacheMisses += op.getMetadataCacheMisses();
            }
        }

        for (HashMap<String, TransactionsStats.ServerlessStatisticsPackage> statPackages : statisticsPackages) {
            sharedHdfs.mergeStatisticsPackages(statPackages, true);
        }

        for (HashMap<String, List<TransactionEvent>> txEvents : transactionEvents) {
            sharedHdfs.mergeTransactionEvents(txEvents, true);
        }

        // double durationSeconds = duration.getSeconds() + (duration.getNano() / 1e9);
        double durationSeconds = (end - start) / 1000.0;
        double totalReads = (double)n * (double)readsPerFile;
        double throughput = (totalReads / durationSeconds);
        LOG.info("Finished performing all " + totalReads + " file reads in " + durationSeconds);

        LOG.info("Latency TCP & HTTP (ms) [min: " + latencyBoth.getMin() + ", max: " + latencyBoth.getMax() +
                ", avg: " + latencyBoth.getMean() + ", std dev: " + latencyBoth.getStandardDeviation() +
                ", N: " + latencyBoth.getN() + "]");
        LOG.info("Latency TCP (ms) [min: " + latencyTcp.getMin() + ", max: " + latencyTcp.getMax() +
                ", avg: " + latencyTcp.getMean() + ", std dev: " + latencyTcp.getStandardDeviation() +
                ", N: " + latencyTcp.getN() + "]");
        LOG.info("Latency HTTP (ms) [min: " + latencyHttp.getMin() + ", max: " + latencyHttp.getMax() +
                ", avg: " + latencyHttp.getMean() + ", std dev: " + latencyHttp.getStandardDeviation() +
                ", N: " + latencyHttp.getN() + "]");
        LOG.info("Cache Hits: " + totalCacheHits + ", Cache Misses: " + totalCacheMisses);
        LOG.info("Throughput: " + throughput + " ops/sec.");

        sharedHdfs.addLatencies(latencyTcp.getValues(), latencyHttp.getValues());

        return new DistributedBenchmarkResult(null, OP_STRONG_SCALING_READS, (int)totalReads, durationSeconds,
                start, end);
    }

    /**
     * Print the operations performed. Optionally write them to a CSV.
     */
    public static void printOperationsPerformed(DistributedFileSystem hdfs) throws IOException {
        System.out.print("Write to CSV? \n> ");
        String input = scanner.nextLine();

        hdfs.printOperationsPerformed();

        HashMap<String, List<TransactionEvent>> transactionEvents = hdfs.getTransactionEvents();
        ArrayList<TransactionEvent> allTransactionEvents = new ArrayList<TransactionEvent>();

        DescriptiveStatistics httpStatistics = new DescriptiveStatistics();
        DescriptiveStatistics tcpStatistics = new DescriptiveStatistics();

        for (Map.Entry<String, List<TransactionEvent>> entry : transactionEvents.entrySet()) {
            allTransactionEvents.addAll(entry.getValue());
        }

        System.out.println("====================== Transaction Events ====================================================================================");

        System.out.println("\n-- SUMS ----------------------------------------------------------------------------------------------------------------------");
        System.out.println(TransactionEvent.getMetricsHeader());
        System.out.println(TransactionEvent.getMetricsString(TransactionEvent.getSums(allTransactionEvents)));

        System.out.println("\n-- AVERAGES ------------------------------------------------------------------------------------------------------------------");
        System.out.println(TransactionEvent.getMetricsHeader());
        System.out.println(TransactionEvent.getMetricsString(TransactionEvent.getAverages(allTransactionEvents)));

        System.out.println("\n==============================================================================================================================");

        if (input.equalsIgnoreCase("y")) {
            System.out.print("File path? (no extension)\n> ");
            String baseFilePath = scanner.nextLine();

            BufferedWriter opsPerformedWriter = new BufferedWriter(new FileWriter(baseFilePath + ".csv"));
            List<OperationPerformed> operationsPerformed = hdfs.getOperationsPerformed();

            opsPerformedWriter.write(OperationPerformed.getHeader());
            opsPerformedWriter.newLine();
            for (OperationPerformed op : operationsPerformed) {
                op.write(opsPerformedWriter);
            }
            opsPerformedWriter.close();

            BufferedWriter txEventsWriter = new BufferedWriter(new FileWriter(baseFilePath + "-txevents.csv"));

            txEventsWriter.write(TransactionEvent.getHeader());
            txEventsWriter.newLine();

            for (Map.Entry<String, List<TransactionEvent>> entry : transactionEvents.entrySet()) {
                List<TransactionEvent> txEvents = entry.getValue();

                for (TransactionEvent transactionEvent : txEvents) {
                    transactionEvent.write(txEventsWriter);
                }
            }

            txEventsWriter.close();
        }
    }

    public static void readFilesOperation(final Configuration configuration,
                                          DistributedFileSystem sharedHdfs,
                                          final String nameNodeEndpoint)
            throws InterruptedException {
        System.out.print("Path to local file containing HopsFS/HDFS paths:\n> ");
        String localFilePath = scanner.nextLine();

        System.out.print("Reads per file:\n> ");
        int readsPerFile = Integer.parseInt(scanner.nextLine());

        System.out.print("Number of threads:\n> ");
        int numThreads = Integer.parseInt(scanner.nextLine());

        readFiles(localFilePath, readsPerFile, numThreads, configuration, sharedHdfs, nameNodeEndpoint);
    }

    public static DistributedBenchmarkResult deleteFilesOperation(DistributedFileSystem sharedHdfs, final String nameNodeEndpoint) {
        System.out.print("Path to file containing HopsFS paths? \n> ");
        String input = scanner.nextLine();
        return deleteFiles(input, sharedHdfs, nameNodeEndpoint);
    }

    /**
     * Delete the files listed in the file specified by the path argument.
     * @param localPath Text file containing HopsFS file paths to delete.
     */
    public static DistributedBenchmarkResult deleteFiles(String localPath, DistributedFileSystem sharedHdfs, final String nameNodeEndpoint) {
        List<String> paths;
        try {
            paths = Utils.getFilePathsFromFile(localPath);
        } catch (FileNotFoundException ex) {
            LOG.error("Could not find file: '" + localPath + "'");
            return null;
        }

        int numSuccessfulDeletes = 0;
        long s = System.currentTimeMillis();
        List<Long> latencies = new ArrayList<>();
        for (String path : paths) {
            try {
                Path filePath = new Path(nameNodeEndpoint + path);
                long start = System.currentTimeMillis();
                boolean success = sharedHdfs.delete(filePath, true);
                long end = System.currentTimeMillis();

                if (success) {
                    LOG.debug("\t Successfully deleted '" + path + "' in " + (end - start) + " ms.");
                    numSuccessfulDeletes++;
                }
                else
                    LOG.error("\t Failed to delete '" + path + "' after " + (end - start) + " ms.");

                latencies.add(end - start);
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
        long t = System.currentTimeMillis();
        double durationSeconds = (t - s) / 1000.0;

        LOG.info("Finished performing all " + paths.size() + " delete operations in " + durationSeconds + " sec.");
        LOG.info("Total number of ops: " + paths.size() + ". Number of successful ops: " + numSuccessfulDeletes + ".");
        double throughput = (numSuccessfulDeletes / durationSeconds);
        LOG.info("Throughput: " + throughput + " ops/sec.");
        LOG.info("Throughput (ALL ops): " + (paths.size() / durationSeconds) + " ops/sec.");
        /*LOG.info("(Note that, if we were deleting directories, then the number of 'actual' deletes " +
                "-- and therefore the overall throughput -- could be far higher...");*/

        LOG.info("Latencies (ms): ");
        for (Long latency : latencies)
            System.out.println(latency);

        return new DistributedBenchmarkResult(null, OP_DELETE_FILES, paths.size(), durationSeconds, s, t);
    }

    /**
     * Specify a file on the local filesystem containing a bunch of HopsFS file paths. Read the local file in order
     * to get all the HopsFS file paths, then read those files a configurable number of times.
     * @param path Path to file containing a bunch of HopsFS files.
     * @param readsPerFile Number of times each file should be read.
     * @param numThreads Number of threads to use when performing the reads concurrently.
     */
    public static void readFiles(String path, int readsPerFile, int numThreads,
                                 final Configuration configuration, DistributedFileSystem sharedHdfs,
                                 final String nameNodeEndpoint) throws InterruptedException {
        List<String> paths;
        try {
            paths = Utils.getFilePathsFromFile(path);
        } catch (FileNotFoundException ex) {
            LOG.error("Could not find file: '" + path + "'");
            return;
        }
        int n = paths.size();

        int filesPerArray = (int)Math.floor((double)n/numThreads);
        int remainder = n % numThreads;

        if (remainder != 0) {
            LOG.info("Assigning all but last thread " + filesPerArray +
                    " files. The last thread will be assigned " + remainder + " files.");
        } else {
            LOG.info("Assigning each thread " + filesPerArray + " files.");
        }

        String[][] pathsPerThread = Utils.splitArray(paths.toArray(new String[0]), filesPerArray);

        assert pathsPerThread != null;
        LOG.info("pathsPerThread.length: " + pathsPerThread.length);

        Thread[] threads = new Thread[numThreads];

        // Used to synchronize threads; they each connect to HopsFS and then
        // count down. So, they all cannot start until they are all connected.
        final CountDownLatch latch = new CountDownLatch(numThreads);
        final Semaphore endSemaphore = new Semaphore((numThreads * -1) + 1);

        final java.util.concurrent.BlockingQueue<List<OperationPerformed>> operationsPerformed =
                new java.util.concurrent.ArrayBlockingQueue<>(numThreads);
        final BlockingQueue<HashMap<String, TransactionsStats.ServerlessStatisticsPackage>> statisticsPackages
                = new ArrayBlockingQueue<>(numThreads);
        final BlockingQueue<HashMap<String, List<TransactionEvent>>> transactionEvents
                = new ArrayBlockingQueue<>(numThreads);

        final SynchronizedDescriptiveStatistics latencyHttp = new SynchronizedDescriptiveStatistics();
        final SynchronizedDescriptiveStatistics latencyTcp = new SynchronizedDescriptiveStatistics();
        final SynchronizedDescriptiveStatistics latencyBoth = new SynchronizedDescriptiveStatistics();

        for (int i = 0; i < numThreads; i++) {
            final String[] pathsForThread = pathsPerThread[i];
            Thread thread = new Thread(() -> {
                DistributedFileSystem hdfs = Commander.initDfsClient(nameNodeEndpoint);

                latch.countDown();

                for (String filePath : pathsForThread) {
                    for (int j = 0; j < readsPerFile; j++)
                        readFile(filePath, hdfs, nameNodeEndpoint);
                }

                // This way, we don't have to wait for all the statistics to be added to lists and whatnot.
                // As soon as the threads finish, they call release() on the endSemaphore. Once all threads have
                // done this, we designate the benchmark as ended and record the stop time. Then we join the threads
                // so that all the statistics are placed into the appropriate collections where we can aggregate them.
                endSemaphore.release();

                operationsPerformed.add(hdfs.getOperationsPerformed());
                statisticsPackages.add(hdfs.getStatisticsPackages());
                transactionEvents.add(hdfs.getTransactionEvents());

                for (double latency : hdfs.getLatencyStatistics().getValues()) {
                    latencyBoth.addValue(latency);
                }

                for (double latency : hdfs.getLatencyHttpStatistics().getValues()) {
                    latencyHttp.addValue(latency);
                }

                for (double latency : hdfs.getLatencyTcpStatistics().getValues()) {
                    latencyTcp.addValue(latency);
                }

                try {
                    hdfs.close();
                } catch (IOException ex) {
                    LOG.error("Encountered IOException while closing DistributedFileSystem object:", ex);
                }
            });
            threads[i] = thread;
        }

        LOG.info("Starting threads.");
        long start = System.currentTimeMillis();
        for (Thread thread : threads) {
            thread.start();
        }

        // This way, we don't have to wait for all the statistics to be added to lists and whatnot.
        // As soon as the threads finish, they call release() on the endSemaphore. Once all threads have
        // done this, we designate the benchmark as ended and record the stop time. Then we join the threads
        // so that all the statistics are placed into the appropriate collections where we can aggregate them.
        endSemaphore.acquire();
        long end = System.currentTimeMillis();

        LOG.info("Benchmark completed in " + (end - start) + "ms. Joining threads now...");
        for (Thread thread : threads) {
            thread.join();
        }

        int totalCacheHits = 0;
        int totalCacheMisses = 0;

        for (List<OperationPerformed> opsPerformed : operationsPerformed) {
            sharedHdfs.addOperationPerformeds(opsPerformed);

            for (OperationPerformed op : opsPerformed) {
                totalCacheHits += op.getMetadataCacheHits();
                totalCacheMisses += op.getMetadataCacheMisses();
            }
        }

        for (HashMap<String, TransactionsStats.ServerlessStatisticsPackage> statPackages : statisticsPackages) {
            sharedHdfs.mergeStatisticsPackages(statPackages, true);
        }

        for (HashMap<String, List<TransactionEvent>> txEvents : transactionEvents) {
            sharedHdfs.mergeTransactionEvents(txEvents, true);
        }

        double durationSeconds = (end - start) / 1000.0;

        LOG.info("Finished performing all " + (readsPerFile * paths.size()) + " file reads in " + durationSeconds);
        double totalReads = (double)n * (double)readsPerFile;
        double throughput = (totalReads / durationSeconds);

        LOG.info("Latency TCP & HTTP (ms) [min: " + latencyBoth.getMin() + ", max: " + latencyBoth.getMax() +
                ", avg: " + latencyBoth.getMean() + ", std dev: " + latencyBoth.getStandardDeviation() +
                ", N: " + latencyBoth.getN() + "]");
        LOG.info("Latency TCP (ms) [min: " + latencyTcp.getMin() + ", max: " + latencyTcp.getMax() +
                ", avg: " + latencyTcp.getMean() + ", std dev: " + latencyTcp.getStandardDeviation() +
                ", N: " + latencyTcp.getN() + "]");
        LOG.info("Latency HTTP (ms) [min: " + latencyHttp.getMin() + ", max: " + latencyHttp.getMax() +
                ", avg: " + latencyHttp.getMean() + ", std dev: " + latencyHttp.getStandardDeviation() +
                ", N: " + latencyHttp.getN() + "]");
        LOG.info("Throughput: " + throughput + " ops/sec.");

        sharedHdfs.addLatencies(latencyTcp.getValues(), latencyHttp.getValues());
    }

    /**
     * Write a bunch of files to a target directory.
     *
     * @param sharedHdfs Passed by main thread. We only use this if we're doing single-threaded.
     */
    public static void writeFilesToDirectory(DistributedFileSystem sharedHdfs,
                                             final Configuration configuration,
                                             final String nameNodeEndpoint)
            throws InterruptedException, IOException {
        System.out.print("Target directory:\n> ");
        String targetDirectory = scanner.nextLine();

        System.out.print("Number of files:\n> ");
        int n = Integer.parseInt(scanner.nextLine());

        System.out.print("Min string length:\n> ");
        int minLength = Integer.parseInt(scanner.nextLine());

        System.out.print("Max string length:\n> ");
        int maxLength = Integer.parseInt(scanner.nextLine());

        // If 'y', create the files one-by-one. If 'n', we'll use a configurable number of threads.
        System.out.print("Sequentially create files? [Y/n]\n>");
        String resp = scanner.nextLine();

        int numThreads = 1;
        // If they answered anything other than 'y', then abort.
        if (resp.equalsIgnoreCase("n")) {
            System.out.print("Number of threads:\n> ");
            numThreads = Integer.parseInt(scanner.nextLine());
        }

        writeFilesInternal(n, minLength, maxLength, numThreads, Collections.singletonList(targetDirectory),
                sharedHdfs, configuration, nameNodeEndpoint);
    }

    /**
     * Write a bunch of files to a bunch of directories.
     *
     * @param n Number of files per directory.
     * @param minLength Minimum length of randomly-generated file contents.
     * @param maxLength Maximum length of randomly-generated file contents.
     * @param numThreads The number of threads to use when performing the operation.
     * @param targetDirectories The target directories.
     * @param sharedHdfs Shared/master DistributedFileSystem instance.
     * @param configuration Configuration for per-thread DistributedFileSystem objects.
     */
    public static DistributedBenchmarkResult writeFilesInternal(int n, int minLength, int maxLength, int numThreads,
                                    List<String> targetDirectories, DistributedFileSystem sharedHdfs,
                                    Configuration configuration, final String nameNodeEndpoint)
            throws IOException, InterruptedException {
        // Generate the file contents and file names.
        int totalNumberOfFiles = n * targetDirectories.size();
        LOG.info("Generating " + n + " files for each directory (total of " + totalNumberOfFiles + " files.");
        final String[] targetPaths = new String[totalNumberOfFiles];
        int counter = 0;
        double filesPerSec = 0.0;

        String[] content = Utils.getVariableLengthRandomStrings(totalNumberOfFiles, minLength, maxLength);

        for (String targetDirectory : targetDirectories) {
            String[] targetFiles = Utils.getFixedLengthRandomStrings(n, 15);

            for (String targetFile : targetFiles) {
                targetPaths[counter++] = targetDirectory + "/" + targetFile;
            }
        }

        LOG.info("Generated a total of " + totalNumberOfFiles + " file(s).");

        Utils.write("./output/writeToDirectoryPaths-" + Instant.now().toEpochMilli()+ ".txt", targetPaths);

        long start, end;
        int numSuccess = 0;
        if (numThreads == 1) {
            start = System.currentTimeMillis();

            numSuccess = createFiles(targetPaths, content, sharedHdfs, nameNodeEndpoint);

            end = System.currentTimeMillis();
            LOG.info("");
            LOG.info("");
            LOG.info("===============================");
        } else {
            int filesPerArray = (int)Math.floor((double)totalNumberOfFiles / numThreads);
            int remainder = totalNumberOfFiles % numThreads;

            if (remainder != 0) {
                LOG.info("Assigning all but last thread " + filesPerArray +
                        " files. The last thread will be assigned " + remainder + " files.");
            } else {
                LOG.info("Assigning each thread " + filesPerArray + " files.");
            }

            final String[][] contentPerArray = Utils.splitArray(content, filesPerArray);
            final String[][] targetPathsPerArray = Utils.splitArray(targetPaths, filesPerArray);

            assert targetPathsPerArray != null;
            assert contentPerArray != null;

            final CountDownLatch latch = new CountDownLatch(numThreads);
            final Semaphore endSemaphore = new Semaphore((numThreads * -1) + 1);

            Thread[] threads = new Thread[numThreads];

            final java.util.concurrent.BlockingQueue<List<OperationPerformed>> operationsPerformed =
                    new java.util.concurrent.ArrayBlockingQueue<>(numThreads);
            final BlockingQueue<HashMap<String, TransactionsStats.ServerlessStatisticsPackage>> statisticsPackages
                    = new ArrayBlockingQueue<>(numThreads);
            final BlockingQueue<HashMap<String, List<TransactionEvent>>> transactionEvents
                    = new ArrayBlockingQueue<>(numThreads);
            final BlockingQueue<Integer> numSuccessPerThread = new ArrayBlockingQueue<>(numThreads);

            final SynchronizedDescriptiveStatistics latencyHttp = new SynchronizedDescriptiveStatistics();
            final SynchronizedDescriptiveStatistics latencyTcp = new SynchronizedDescriptiveStatistics();
            final SynchronizedDescriptiveStatistics latencyBoth = new SynchronizedDescriptiveStatistics();

            for (int i = 0; i < numThreads; i++) {
                final int idx = i;
                Thread thread = new Thread(() -> {
                    DistributedFileSystem hdfs = Commander.initDfsClient(nameNodeEndpoint);

                    latch.countDown();
                    int localNumSuccess = createFiles(targetPathsPerArray[idx], contentPerArray[idx], hdfs, nameNodeEndpoint);

                    endSemaphore.release();

                    operationsPerformed.add(hdfs.getOperationsPerformed());
                    statisticsPackages.add(hdfs.getStatisticsPackages());
                    transactionEvents.add(hdfs.getTransactionEvents());
                    numSuccessPerThread.add(localNumSuccess);

                    for (double latency : hdfs.getLatencyStatistics().getValues()) {
                        latencyBoth.addValue(latency);
                    }

                    for (double latency : hdfs.getLatencyHttpStatistics().getValues()) {
                        latencyHttp.addValue(latency);
                    }

                    for (double latency : hdfs.getLatencyTcpStatistics().getValues()) {
                        latencyTcp.addValue(latency);
                    }

                    try {
                        hdfs.close();
                    } catch (IOException ex) {
                        LOG.error("Encountered IOException while closing DistributedFileSystem object:", ex);
                    }
                });
                threads[i] = thread;
            }

            LOG.info("Starting threads.");
            start = System.currentTimeMillis();
            for (Thread thread : threads) {
                thread.start();
            }

            // This way, we don't have to wait for all the statistics to be added to lists and whatnot.
            // As soon as the threads finish, they call release() on the endSemaphore. Once all threads have
            // done this, we designate the benchmark as ended and record the stop time. Then we join the threads
            // so that all the statistics are placed into the appropriate collections where we can aggregate them.
            endSemaphore.acquire();
            end = System.currentTimeMillis();

            LOG.info("Benchmark completed in " + (end - start) + "ms. Joining threads now...");
            for (Thread thread : threads) {
                thread.join();
            }

            LOG.info("");
            LOG.info("");
            LOG.info("===============================");
            LOG.info("Latency TCP & HTTP (ms) [min: " + latencyBoth.getMin() + ", max: " + latencyBoth.getMax() +
                    ", avg: " + latencyBoth.getMean() + ", std dev: " + latencyBoth.getStandardDeviation() +
                    ", N: " + latencyBoth.getN() + "]");
            LOG.info("Latency TCP (ms) [min: " + latencyTcp.getMin() + ", max: " + latencyTcp.getMax() +
                    ", avg: " + latencyTcp.getMean() + ", std dev: " + latencyTcp.getStandardDeviation() +
                    ", N: " + latencyTcp.getN() + "]");
            LOG.info("Latency HTTP (ms) [min: " + latencyHttp.getMin() + ", max: " + latencyHttp.getMax() +
                    ", avg: " + latencyHttp.getMean() + ", std dev: " + latencyHttp.getStandardDeviation() +
                    ", N: " + latencyHttp.getN() + "]");

            sharedHdfs.addLatencies(latencyTcp.getValues(), latencyHttp.getValues());

            for (List<OperationPerformed> opsPerformed : operationsPerformed) {
                //LOG.info("Adding list of " + opsPerformed.size() + " operations performed to master/shared HDFS object.");
                sharedHdfs.addOperationPerformeds(opsPerformed);
            }

            for (HashMap<String, TransactionsStats.ServerlessStatisticsPackage> statPackages : statisticsPackages) {
                //LOG.info("Adding list of " + statPackages.size() + " statistics packages to master/shared HDFS object.");
                sharedHdfs.mergeStatisticsPackages(statPackages, true);
            }

            for (HashMap<String, List<TransactionEvent>> txEvents : transactionEvents) {
                //LOG.info("Merging " + txEvents.size() + " new transaction event(s) into master/shared HDFS object.");
                sharedHdfs.mergeTransactionEvents(txEvents, true);
            }

            for (Integer localNumSuccess : numSuccessPerThread) {
                numSuccess += localNumSuccess;
            }
        }

        double durationSeconds = (end - start) / 1000.0;
        filesPerSec = numSuccess / durationSeconds;
        LOG.info("Number of successful write operations: " + numSuccess);
        LOG.info("Number of failed write operations: " + (totalNumberOfFiles - numSuccess));
        LOG.info("Time elapsed: " + durationSeconds);
        LOG.info("Aggregate throughput: " + filesPerSec + " ops/sec.");

        // Print the throughput including failed ops if there was at least one failed op.
        if (totalNumberOfFiles != numSuccess)
            LOG.info("Aggregate throughput including failures: " + (totalNumberOfFiles / durationSeconds) + " ops/sec.");

        return new DistributedBenchmarkResult(null, 0, numSuccess,
                durationSeconds, start, end);
    }

    public static void createDirectories(DistributedFileSystem hdfs, String nameNodeEndpoint) {
        System.out.print("Base name for the directories:\n> ");
        String baseDirName = scanner.nextLine();

        int numDirectoriesToCreate = getIntFromUser("How many directories should be created?");

        long s = System.currentTimeMillis();
        for (int i = 0; i < numDirectoriesToCreate; i++) {
            mkdir(baseDirName + i + "/", hdfs, nameNodeEndpoint);
        }
        long t = System.currentTimeMillis();

        double durationSeconds = (t - s) / 1000.0;
        double throughput = (double)numDirectoriesToCreate / durationSeconds;

        LOG.info("Created " + numDirectoriesToCreate + " directories in " + durationSeconds + " seconds.");
        LOG.info("Throughput: " + throughput + " ops/sec.");
    }

    public static void createSubtree(DistributedFileSystem hdfs, String nameNodeEndpoint) {
        System.out.print("Subtree root directory:\n> ");
        String subtreeRootPath = scanner.nextLine();

        System.out.print("Subtree depth:\n> ");
        int subtreeDepth = Integer.parseInt(scanner.nextLine());

        System.out.print("Max subdirs:\n> ");
        int maxSubDirs = Integer.parseInt(scanner.nextLine());

        int height = subtreeDepth + 1;
        double totalPossibleDirectories = (Math.pow(maxSubDirs, height + 1) - 1) / (maxSubDirs - 1);
        LOG.info("\nThis could create a maximum of " + totalPossibleDirectories + " directories.");
        System.out.print("Is this okay? [y/N]\n >");

        String resp = scanner.nextLine();

        // If they answered anything other than 'y', then abort.
        if (!resp.equalsIgnoreCase("y")) {
            LOG.info("\nAborting.");
            return;
        }

        int directoriesCreated = 0;
        int filesCreated = 0;

        Instant start = Instant.now();

        int currentDepth = 0;

        mkdir(subtreeRootPath, hdfs, nameNodeEndpoint);
        directoriesCreated++;

        Stack<TreeNode> directoryStack = new Stack<TreeNode>();
        TreeNode subtreeRoot = new TreeNode(subtreeRootPath, new ArrayList<TreeNode>());
        directoryStack.push(subtreeRoot);

        while (currentDepth <= subtreeDepth) {
            LOG.info("=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=");
            LOG.info("CURRENT DEPTH: " + currentDepth);
            LOG.info("DIRECTORIES CREATED: " + directoriesCreated);
            LOG.info("=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=");
            List<Stack<TreeNode>> currentDepthStacks = new ArrayList<>();
            while (!directoryStack.empty()) {
                TreeNode directory = directoryStack.pop();

                String basePath = directory.getPath() + "/dir";

                Stack<TreeNode> stack = createChildDirectories(basePath, maxSubDirs, hdfs, nameNodeEndpoint);
                directory.addChildren(stack);
                directoriesCreated += stack.size();
                currentDepthStacks.add(stack);
            }

            for (Stack<TreeNode> stack : currentDepthStacks) {
                directoryStack.addAll(stack);
            }

            currentDepth++;
        }

        Instant end = Instant.now();
        Duration subtreeCreationDuration = Duration.between(start, end);

        LOG.info("=== Subtree Creation Completed ===");
        LOG.info("Time elapsed: " + subtreeCreationDuration.toString());
        LOG.info("Directories created: " + directoriesCreated);
        LOG.info("Files created: " + filesCreated + "\n");

        LOG.info("subtreeRoot children: " + subtreeRoot.getChildren().size());
        LOG.info(subtreeRoot.toString());

        LOG.info("==================================");
    }

    public static Stack<TreeNode> createChildDirectories(String basePath, int subDirs,
                                                         DistributedFileSystem hdfs, String nameNodeEndpoint) {
        Stack<TreeNode> directoryStack = new Stack<TreeNode>();
        for (int i = 0; i < subDirs; i++) {
            String path = basePath + i;
            mkdir(path, hdfs, nameNodeEndpoint);
            TreeNode node = new TreeNode(path, new ArrayList<TreeNode>());
            directoryStack.push(node);
        }

        return directoryStack;
    }

    public static void createFileOperation(DistributedFileSystem hdfs, String nameNodeEndpoint) {
        System.out.print("File path:\n> ");
        String fileName = scanner.nextLine();
        System.out.print("File contents:\n> ");
        String fileContents = scanner.nextLine().trim();

        createFile(fileName, fileContents, hdfs, nameNodeEndpoint);
    }

    /**
     * Create files using the names and contents provide by the two parameters.
     *
     * The two argument lists must have the same length.
     *
     * @param names File names.
     * @param content File contents.
     *
     * @return The number of successful create operations.
     */
    public static int createFiles(String[] names, String[] content, DistributedFileSystem hdfs, String nameNodeEndpoint) {
        assert(names.length == content.length);

        int numSuccess = 0;

        for (int i = 0; i < names.length; i++) {
            LOG.info("Writing file " + i + "/" + names.length);
            long s = System.currentTimeMillis();
            boolean success = createFile(names[i], content[i], hdfs, nameNodeEndpoint);
            if (success) numSuccess++;
            long t = System.currentTimeMillis();
            LOG.info("Wrote file " + (i+1) + "/" + names.length + " in " + (t - s) + " ms.");
        }

        return numSuccess;
    }

    /**
     * Create a new file with the given name and contents.
     * @param name The name of the file.
     * @param contents The content to be written to the file.
     *
     * @return True if the create operation succeeds, otherwise false.
     */
    public static boolean createFile(String name, String contents,
                                  DistributedFileSystem hdfs,
                                  String nameNodeEndpoint) {
        Path filePath = new Path(nameNodeEndpoint + name);

        try {
            FSDataOutputStream outputStream = hdfs.create(filePath);

            if (!contents.isEmpty()) {
                BufferedWriter br = new BufferedWriter(new OutputStreamWriter(outputStream, "UTF-8"));
                br.write(contents);
                br.close();
                LOG.info("\t Successfully created non-empty file '" + filePath + "'");
            } else {
                LOG.info("\t Successfully created empty file '" + filePath + "'");
                outputStream.close();
            }

            return true;
        } catch (IOException ex) {
            ex.printStackTrace();
        }

        return false;
    }

    public static void renameOperation(DistributedFileSystem hdfs, String nameNodeEndpoint) {
        System.out.print("Original file path:\n> ");
        String originalFileName = scanner.nextLine();
        System.out.print("Renamed file path:\n> ");
        String renamedFileName = scanner.nextLine();

        Path filePath = new Path(nameNodeEndpoint + originalFileName);
        Path filePathRename = new Path(nameNodeEndpoint + renamedFileName);

        try {
            LOG.info("\t Original file path: \"" + originalFileName + "\"");
            LOG.info("\t New file path: \"" + renamedFileName + "\"");
            hdfs.rename(filePath, filePathRename);
            LOG.info("\t Finished rename operation.");
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    public static void listOperation(DistributedFileSystem hdfs, String nameNodeEndpoint) {
        System.out.print("Target directory:\n> ");
        String targetDirectory = scanner.nextLine();

        try {
            FileStatus[] fileStatus = hdfs.listStatus(new Path(nameNodeEndpoint + targetDirectory));
            LOG.info("Directory '" + targetDirectory + "' contains " + fileStatus.length + " files.");
            for(FileStatus status : fileStatus)
                LOG.info(status.getPath().toString());
            LOG.info("Directory '" + targetDirectory + "' contains " + fileStatus.length + " files.");
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    /**
     * Create a new directory with the given path.
     * @param path The path of the new directory.
     */
    public static void mkdir(String path, DistributedFileSystem hdfs, String nameNodeEndpoint) {
        Path filePath = new Path(nameNodeEndpoint + path);

        try {
            LOG.info("\t Attempting to create new directory: \"" + path + "\"");
            boolean directoryCreated = hdfs.mkdirs(filePath);
            LOG.info("\t Directory created successfully: " + directoryCreated);
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    public static void mkdirOperation(DistributedFileSystem hdfs, String nameNodeEndpoint) {
        System.out.print("New directory path:\n> ");
        String newDirectoryName = scanner.nextLine();

        mkdir(newDirectoryName, hdfs, nameNodeEndpoint);
    }

    public static void appendOperation(DistributedFileSystem hdfs, String nameNodeEndpoint) {
        System.out.print("File path:\n> ");
        String fileName = scanner.nextLine();
        System.out.print("Content to append:\n> ");
        String fileContents = scanner.nextLine();

        Path filePath = new Path(nameNodeEndpoint + fileName);

        try {
            FSDataOutputStream outputStream = hdfs.append(filePath);
            LOG.info("\t Called append() successfully.");
            BufferedWriter br = new BufferedWriter(new OutputStreamWriter(outputStream, "UTF-8"));
            LOG.info("\t Created BufferedWriter object.");
            br.write(fileContents);
            LOG.info("\t Appended \"" + fileContents + "\" to file using BufferedWriter.");
            br.close();
            LOG.info("\t Closed BufferedWriter.");
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    public static void prewarmOperation(DistributedFileSystem hdfs) {
        System.out.print("Invocations per deployment:\n> ");
        int pingsPerDeployment = Integer.parseInt(scanner.nextLine());

        try {
            hdfs.prewarm(pingsPerDeployment);
        } catch (IOException ex) {
            LOG.info("Encountered IOException while pre-warming NNs.");
            ex.printStackTrace();
        }
    }

    public static void pingOperation(DistributedFileSystem hdfs) {
        System.out.print("Target deployment:\n> ");
        int targetDeployment = Integer.parseInt(scanner.nextLine());

        try {
            hdfs.ping(targetDeployment);
        } catch (IOException ex) {
            LOG.info("Encountered IOException while pinging NameNode deployment " +
                    targetDeployment + ".");
            ex.printStackTrace();
        }
    }

    public static void readOperation(DistributedFileSystem hdfs, String nameNodeEndpoint) {
        System.out.print("File path:\n> ");
        String fileName = scanner.nextLine();
        readFile(fileName, hdfs, nameNodeEndpoint);
    }

    /**
     *
     *
     * Read the HopsFS/HDFS file at the given path.
     * @param fileName The path to the file to read.
     */
    public static void readFile(String fileName, DistributedFileSystem hdfs, String nameNodeEndpoint) {
        Path filePath = new Path(nameNodeEndpoint + fileName);

        try {
            FSDataInputStream inputStream = hdfs.open(filePath);
            BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
            String line = null;

	    //LOG.info("CONTENTS OF FILE '" + fileName + "': ");
            while ((line = br.readLine()) != null)
                LOG.info(line);
            inputStream.close();
            br.close();
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    private static int getIntFromUser(String prompt) {
        System.out.print(prompt + "\n> ");
        return Integer.parseInt(scanner.nextLine());
    }

    public static void deleteOperation(DistributedFileSystem hdfs, final String nameNodeEndpoint) {
        System.out.print("File or directory path:\n> ");
        String targetPath = scanner.nextLine();

        Path filePath = new Path(nameNodeEndpoint + targetPath);

        long s = System.currentTimeMillis();
        try {
            boolean success = hdfs.delete(filePath, true);
            long t = System.currentTimeMillis();

            if (success)
                LOG.info("Successfully deleted '" + filePath + "' in " + (t - s) + " milliseconds.");
            else
                LOG.error("Failed to delete '" + filePath + "' after " + (t - s) + " milliseconds.");
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }
}
