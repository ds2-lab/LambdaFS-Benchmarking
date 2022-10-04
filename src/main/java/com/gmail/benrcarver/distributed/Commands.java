package com.gmail.benrcarver.distributed;

import com.gmail.benrcarver.distributed.util.TreeNode;
import com.gmail.benrcarver.distributed.util.Utils;

import java.io.*;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.commons.math3.stat.descriptive.SynchronizedDescriptiveStatistics;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.fs.FileStatus;

import static com.gmail.benrcarver.distributed.Constants.*;

// TODO: Condense the various functions and build some generic framework for executing benchmarks.
//       Like, many of the function calls here have a lot of boilerplate code to create the infrastructure
//       to temporarily store metrics info and print the results. The only difference is the FS operation
//       being called. So, at some point I can generify everything.
public class Commands {
    public static final Logger LOG = LoggerFactory.getLogger(Commands.class);
    private static final Scanner scanner = new Scanner(System.in);

    private static final String EMPTY_STRING = "";

    public static volatile boolean TRACK_OP_PERFORMED = false;

    /**
     * When true, we do not store OperationsPerformed, and we clear them after each trial,
     * in addition to performing GCs.
     */
    public static boolean BENCHMARKING_MODE = false;

    /**
     * Indicates whether the JVM is running a follower process.
     * If true, then we are a follower.
     * If false, then we are a commander (i.e., a leader).
     */
    public static volatile boolean IS_FOLLOWER = false;

    /**
     * Indicates whether the target filesystem is Serverless HopsFS or Vanilla HopsFS.
     *
     * If true, then the target filesystem is Serverless HopsFS.
     * If false, then the target filesystem is Vanilla HopsFS.
     */
    public static volatile boolean IS_SERVERLESS = true;

    /**
     * Used to cache clients for reuse.
     */
    public static BlockingQueue<DistributedFileSystem> hdfsClients
            = new ArrayBlockingQueue<>(1024);

    /**
     * Retrieve an HDFS client to use during a benchmark. This will attempt to reuse an existing client.
     * If none are available, then a new client is created.
     *
     * @return An HDFS client instance.
     */
    static synchronized DistributedFileSystem getHdfsClient() {
        DistributedFileSystem hdfs;
        hdfs = hdfsClients.poll();

        if (hdfs == null) {
            LOG.warn("No HDFS client instances available (size = " +
                    hdfsClients.size() + "). Creating a new client now...");
            hdfs = Commander.initDfsClient(Commander.NAME_NODE_ENDPOINT);
        }
        return hdfs;
    }

    static void returnHdfsClient(DistributedFileSystem hdfs) throws InterruptedException {
        hdfsClients.add(hdfs);
    }

    /**
     * Generic driver used to execute all/most benchmarks.
     *
     * @param numThreads Number of HopsFS clients to use (one per thread).
     * @param fileBatches Batches of files for each client. One batch per thread.
     * @param operationsPerFile The number of times each client should perform the given operation on a file.
     * @param opCode Identifies the benchmark being performed. Stored with the metric results of the benchmark.
     * @param operation This is what the threads call. We basically specify the FS operation here. Like, we provide
     *                  the code that executes the FS operation one time, and we execute it however many times
     *                  we're supposed to based on the {@code } parameter.
     * @return A result containing all the metric information and whatnot.
     */
    public static DistributedBenchmarkResult executeBenchmark(
            int numThreads,
            String[][] fileBatches,
            int operationsPerFile,
            int opCode,
            FSOperation operation) throws InterruptedException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("At start of benchmark, the HDFS Clients Cache has " + hdfsClients.size() + " clients.");
        }

        Thread[] threads = new Thread[numThreads];

        // Used to synchronize threads; they each connect to HopsFS and then
        // count down. So, they all cannot start until they are all connected.
        final CountDownLatch startLatch = new CountDownLatch(numThreads + 1);

        // Used to synchronize threads; they block when they finish executing to avoid using CPU cycles
        // by aggregating their results. Once all the threads have finished, they aggregate their results.
        final CountDownLatch endLatch = new CountDownLatch(numThreads);
        final Semaphore readySemaphore = new Semaphore((numThreads * -1) + 1);
        final Semaphore endSemaphore = new Semaphore((numThreads * -1) + 1);

        // Keep track of number of successful operations.
        AtomicInteger numSuccessfulOps = new AtomicInteger(0);
        AtomicInteger numOps = new AtomicInteger(0);

        SynchronizedDescriptiveStatistics totalLatencyStatistics = new SynchronizedDescriptiveStatistics();

        for (int i = 0; i < numThreads; i++) {
            final String[] filesForCurrentThread = fileBatches[i];
            final int threadId = i;
            Thread thread = new Thread(() -> {
                // DescriptiveStatistics latencyStatistics = new DescriptiveStatistics();
                DistributedFileSystem hdfs = getHdfsClient();

                readySemaphore.release(); // Ready to start. Once all threads have done this, the timer begins.

                startLatch.countDown(); // Wait for the main thread's signal to actually begin.

                try {
                    startLatch.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                int numSuccessfulOpsCurrentThread = 0;
                int numOpsCurrentThread = 0;

                for (String filePath : filesForCurrentThread) {
                    for (int j = 0; j < operationsPerFile; j++) {
                        // long start = System.currentTimeMillis();
                        if (operation.call(hdfs, filePath, EMPTY_STRING)) {
                            numSuccessfulOpsCurrentThread++;
                            // latencyStatistics.addValue(System.currentTimeMillis() - start);
                        }
                        numOpsCurrentThread++;
                    }
                }

                // This way, we don't have to wait for all the statistics to be added to lists and whatnot.
                // As soon as the threads finish, they call release() on the endSemaphore. Once all threads have
                // done this, we designate the benchmark as ended and record the stop time. Then we join the threads
                // so that all the statistics are placed into the appropriate collections where we can aggregate them.
                endSemaphore.release();

                if (LOG.isDebugEnabled()) LOG.debug("Thread " + threadId + " has finished executing.");

                endLatch.countDown();

                try {
                    endLatch.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                numSuccessfulOps.addAndGet(numSuccessfulOpsCurrentThread);
                numOps.addAndGet(numOpsCurrentThread);

                DescriptiveStatistics latencyStatistics = hdfs.getLatencyStatistics();

                if (Commander.PRIMARY_HDFS != null)
                    Commander.PRIMARY_HDFS.addLatencyValues(latencyStatistics.getValues());

                for (double latency : latencyStatistics.getValues())
                    totalLatencyStatistics.addValue(latency);

                try {
                    // Now return the client to the pool so that it can be used again in the future.
                    returnHdfsClient(hdfs);
                } catch (InterruptedException e) {
                    LOG.error("Encountered error when trying to return HDFS client. Closing it instead.");
                    e.printStackTrace();

                    LOG.warn("[THREAD " + threadId + "] Terminating HDFS connection.");
                    try {
                        hdfs.close();

                        LOG.warn("[THREAD " + threadId + "] Terminated HDFS connection.");
                    } catch (IOException ex) {
                        LOG.error("Encountered IOException while closing DistributedFileSystem object:", ex);
                    }
                }
            });
            threads[i] = thread;
        }

        LOG.info("Starting threads.");
        for (Thread thread : threads) {
            thread.start();
        }

        readySemaphore.acquire();                   // Will block until all client threads are ready to go.
        long start = System.currentTimeMillis();    // Start the clock.
        startLatch.countDown();                     // Let the threads start.

        // This way, we don't have to wait for all the statistics to be added to lists and whatnot.
        // As soon as the threads finish, they call release() on the endSemaphore. Once all threads have
        // done this, we designate the benchmark as ended and record the stop time. Then we join the threads
        // so that all the statistics are placed into the appropriate collections where we can aggregate them.
        endSemaphore.acquire();
        long end = System.currentTimeMillis();

        LOG.info("Benchmark completed in " + (end - start) + "ms. Joining the " + threads.length + " threads now...");
        for (Thread thread : threads) {
            thread.join();
        }

        double durationSeconds = (end - start) / 1.0e3;

        // TODO: Verify that I've calculated the total number of operations correctly.
        int numSuccess = numSuccessfulOps.get();
        double totalOperations = numOps.get();
        LOG.info("Finished performing all " + totalOperations + " operations in " + durationSeconds + " sec.");
        double totalThroughput = totalOperations / durationSeconds;
        double successThroughput = numSuccess / durationSeconds;
        LOG.info("Number of successful operations: " + numSuccess);
        LOG.info("Number of failed operations: " + (totalOperations - numSuccess));

        LOG.info("Total Throughput: " + totalThroughput + " ops/sec.");
        LOG.info("Successful Throughput: " + successThroughput + " ops/sec.");

        if (LOG.isDebugEnabled())
            LOG.debug("At end of benchmark, the HDFS Clients Cache has " + hdfsClients.size() + " clients.");

        return new DistributedBenchmarkResult(null, opCode, numSuccess,
                durationSeconds, start, end, totalLatencyStatistics);
    }

    public static DistributedBenchmarkResult writeFilesToDirectories()
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

        int filesPerDirectory = 10;
        System.out.print("Number of files per directory (default " + filesPerDirectory + "):\n> ");
        try {
            filesPerDirectory = Integer.parseInt(scanner.nextLine());
        } catch (NumberFormatException ex) {
            LOG.info("Defaulting to " + filesPerDirectory + ".");
        }

        if (directories == null)
            throw new IllegalArgumentException("No directories provided for Write-Files-to-Directories operation.");

        // Generate the file contents and file names. targetDirectories has length equal to numThreads
        // except when randomWrites is true (in which case, in may vary). But either way, each thread
        // will be reading n files, so this works.
        int totalNumberOfFiles = filesPerDirectory * directories.size();
        LOG.info("Generating " + filesPerDirectory + " files for each directory (total of " + totalNumberOfFiles + " files).");
        final String[] targetPaths = new String[totalNumberOfFiles];

        int counter = 0;
        // Generate file names and subsequently the full file paths.
        for (String targetDirectory : directories) {
            // File names.
            String[] targetFiles = Utils.getFixedLengthRandomStrings(filesPerDirectory, 15);

            // Create the full paths.
            for (String targetFile : targetFiles) {
                targetPaths[counter++] = targetDirectory + "/" + targetFile;
            }
        }

        LOG.info("Generated a total of " + totalNumberOfFiles + " file(s).");

        Utils.write("./output/writeToDirectoryPaths-" + Instant.now().toEpochMilli()+ ".txt", targetPaths);

        int numWritesPerThread = targetPaths.length / numberOfThreads;
        final String[][] targetPathsPerThread = Utils.splitArray(targetPaths, numWritesPerThread);
        assert(targetPathsPerThread != null);
        assert(targetPathsPerThread.length == numberOfThreads);

        return executeBenchmark(numberOfThreads, targetPathsPerThread,
                1, OP_WRITE_FILES_TO_DIRS, FSOperation.CREATE_FILE);
    }

    /**
     * Gets the user inputs for this benchmark, then calls the actual benchmark itself.
     */
    public static DistributedBenchmarkResult strongScalingBenchmarkOld(int numFilesPerThread, int readsPerFile,
                                                                       int numThreads, String inputPath)
            throws FileNotFoundException, InterruptedException {
        List<String> paths = Utils.getFilePathsFromFile(inputPath);

        if (paths.size() < numFilesPerThread) {
            LOG.error("ERROR: The file should contain at least " + numFilesPerThread +
                    " HopsFS file path(s); however, it contains just " + paths.size() + " HopsFS file path(s).");
            LOG.error("Aborting operation.");
            return null;
        }

        // Select a random subset of size n, where n is the number of files each thread should read.
        Collections.shuffle(paths);

        String[][] filesPerThread = new String[numThreads][numFilesPerThread];
        int counter = 0;
        for (int i = 0; i < numThreads; i++) {
            for (int j = 0; j < numFilesPerThread; j++) {
                // Select the first j files for thread i.
                filesPerThread[i][j] = paths.get(counter);
            }

            // Each thread receives a random subset of files.
            // So, we shuffle the paths each time to ensure each thread gets a random subset.
            Collections.shuffle(paths);
        }

        LOG.debug("Each of the " + numThreads + " thread(s) will read " + numFilesPerThread + " random file(s).");

        return executeBenchmark(numThreads, filesPerThread, readsPerFile, OP_STRONG_SCALING_READS,
                FSOperation.READ_FILE);
    }

    public static void clearMetricData(DistributedFileSystem hdfs) {
        if (!IS_SERVERLESS) {
            LOG.error("Clearing statistics packages is not supported by Vanilla HopsFS!");
            return;
        }

        System.out.print("Are you sure? (y/N)\n> ");
        String input = scanner.nextLine();

        if (input.equalsIgnoreCase("y")) {
            clearMetricDataNoPrompt(hdfs);

            for (DistributedFileSystem otherHdfs : hdfsClients) {
                clearMetricDataNoPrompt(otherHdfs);
            }

            LOG.debug("Cleared both statistics and latency values.");
        } else {
            LOG.info("NOT clearing statistics packages.");
        }
    }

    /**
     * Clear the statistics packages, transaction events, and operations performed metrics objects
     * on the given DistributedFileSystem instance.
     *
     * This also clears the latency values.
     *
     * @param hdfs The instance for which the aforementioned metrics objects should be cleared.
     */
    public static void clearMetricDataNoPrompt(DistributedFileSystem hdfs) {
        hdfs.clearLatencyStatistics();
    }

    /**
     * Each thread reads N files. There may be duplicates. We randomly select N files (with replacement) from
     * the source set of files.
     *
     * This is the WEAK SCALING (read) benchmark.
     *
     * @param numThreads Number of threads.
     * @param filesPerThread How many files should be read by each thread.
     * @param inputPath Path to local file containing HopsFS file paths (of the files to read).
     */
    public static DistributedBenchmarkResult statFilesWeakScaling(int numThreads,
                                                                  final int filesPerThread, String inputPath,
                                                                  boolean shuffle, int opCode)
            throws InterruptedException, FileNotFoundException {
        List<String> paths = Utils.getFilePathsFromFile(inputPath);

        if (shuffle) {
            LOG.debug("Shuffling paths.");
            Collections.shuffle(paths);
        }

        if (paths.size() < numThreads) {
            LOG.error("ERROR: The file should contain at least " + numThreads +
                    " HopsFS file path(s); however, it contains just " + paths.size() + " HopsFS file path(s).");
            LOG.error("Aborting operation.");
            return null;
        }

        Random rng = new Random(); // TODO: Optionally seed this?

        final String[][] fileBatches = new String[numThreads][filesPerThread];
        int counter = 0;
        for (int i = 0; i < numThreads; i++) {
            for (int j = 0; j < filesPerThread; j++) {
                int filePathIndex;
                if (shuffle)
                    filePathIndex = rng.nextInt(paths.size());
                else
                    filePathIndex = counter++;
                fileBatches[i][j] = paths.get(filePathIndex);
            }
            counter = 0;

            if (shuffle)
                Collections.shuffle(paths);
        }

        return executeBenchmark(numThreads, fileBatches, 1, opCode,
                FSOperation.FILE_INFO);
    }

    /**
     * Each thread reads N files. There may be duplicates. We randomly select N files (with replacement) from
     * the source set of files.
     *
     * This is the WEAK SCALING (read) benchmark.
     *
     * @param numThreads Number of threads.
     * @param filesPerThread How many files should be read by each thread.
     * @param inputPath Path to local file containing HopsFS file paths (of the files to read).
     */
    public static DistributedBenchmarkResult weakScalingBenchmarkV2(int numThreads,
                                                                    final int filesPerThread, String inputPath,
                                                                    boolean shuffle, int opCode)
            throws InterruptedException, FileNotFoundException {
        List<String> paths = Utils.getFilePathsFromFile(inputPath);

        if (shuffle) {
            LOG.debug("Shuffling paths.");
            Collections.shuffle(paths);
        }

        if (paths.size() < numThreads) {
            LOG.error("ERROR: The file should contain at least " + numThreads +
                    " HopsFS file path(s); however, it contains just " + paths.size() + " HopsFS file path(s).");
            LOG.error("Aborting operation.");
            return null;
        }

        Random rng = new Random(); // TODO: Optionally seed this?

        final String[][] fileBatches = new String[numThreads][filesPerThread];
        int counter = 0;
        for (int i = 0; i < numThreads; i++) {
            for (int j = 0; j < filesPerThread; j++) {
                int filePathIndex;
                if (shuffle)
                    filePathIndex = rng.nextInt(paths.size());
                else
                    filePathIndex = counter++;
                fileBatches[i][j] = paths.get(filePathIndex);
            }
            counter = 0;

            if (shuffle)
                Collections.shuffle(paths);
        }

        return executeBenchmark(numThreads, fileBatches, 1,
                opCode, FSOperation.READ_FILE);
    }

    public static DistributedBenchmarkResult listDirectoryWeakScaling(int numThreads, int readsPerFile,
                                                                      String inputPath, boolean shuffle, int opCode)
            throws InterruptedException, FileNotFoundException {
        List<String> paths = Utils.getFilePathsFromFile(inputPath);

        if (shuffle) {
            LOG.debug("Shuffling paths.");
            Collections.shuffle(paths);
        }

        if (paths.size() < numThreads) {
            LOG.error("ERROR: The file should contain at least " + numThreads +
                    " HopsFS directory path(s); however, it contains just " + paths.size() +
                    " HopsFS directory path(s).");
            LOG.error("Aborting operation.");
            return null;
        }

        String[][] pathsPerThread = new String[numThreads][1];
        for (int i = 0; i < numThreads; i++) {
            pathsPerThread[i][0] = paths.get(i);
        }

        return executeBenchmark(numThreads, pathsPerThread, readsPerFile, opCode,
                FSOperation.LIST_DIR_NO_PRINT);
    }

    /**
     * Read N files using N threads (so, each thread reads 1 file). Each file is read a specified number of times.
     *
     * This is the WEAK SCALING (read) benchmark.
     *
     * @param numThreads Number of threads & number of files to read, as each thread reads one file.
     * @param readsPerFile How many times each file should be read.
     * @param inputPath Path to local file containing HopsFS filepaths (of the files to read).
     */
    public static DistributedBenchmarkResult weakScalingReadsV1(int numThreads, int readsPerFile, String inputPath,
                                                                boolean shuffle, int opCode)
            throws InterruptedException, FileNotFoundException {
        List<String> paths = Utils.getFilePathsFromFile(inputPath);

        if (shuffle) {
            LOG.debug("Shuffling paths.");
            Collections.shuffle(paths);
        }

        if (paths.size() < numThreads) {
            LOG.error("ERROR: The file should contain at least " + numThreads +
                    " HopsFS file path(s); however, it contains just " + paths.size() + " HopsFS file path(s).");
            LOG.error("Aborting operation.");
            return null;
        }

        String[][] pathsPerThread = new String[numThreads][1];
        for (int i = 0; i < numThreads; i++) {
            pathsPerThread[i][0] = paths.get(i);
        }

        return executeBenchmark(numThreads, pathsPerThread, readsPerFile, opCode,
                FSOperation.READ_FILE);
    }

    public static DistributedBenchmarkResult getFileStatusOperation() throws InterruptedException {
        System.out.print("Path to local file containing HopsFS/HDFS paths:\n> ");
        String localFilePath = scanner.nextLine();

        System.out.print("Stat calls per file/directory:\n> ");
        int opsPerFile = Integer.parseInt(scanner.nextLine());

        System.out.print("Number of threads:\n> ");
        int numThreads = Integer.parseInt(scanner.nextLine());

        List<String> paths;
        try {
            paths = Utils.getFilePathsFromFile(localFilePath);
        } catch (FileNotFoundException ex) {
            LOG.error("Could not find file: '" + localFilePath + "'");
            return null;
        }
        int numPaths = paths.size();

        int filesPerArray = (int)Math.floor((double)numPaths/numThreads);
        int remainder = numPaths % numThreads;

        if (remainder != 0) {
            LOG.info("Assigning all but last thread " + filesPerArray +
                    " files. The last thread will be assigned " + remainder + " files.");
        } else {
            LOG.info("Assigning each thread " + filesPerArray + " files.");
        }

        String[][] pathsPerThread = Utils.splitArray(paths.toArray(new String[0]), filesPerArray);

        assert pathsPerThread != null;
        LOG.info("pathsPerThread.length: " + pathsPerThread.length);

        return executeBenchmark(numThreads, pathsPerThread, opsPerFile, OP_GET_FILE_STATUS,
                FSOperation.FILE_INFO);
    }

    public static void readFilesOperation()
            throws InterruptedException {
        System.out.print("Path to local file containing HopsFS/HDFS paths:\n> ");
        String localFilePath = scanner.nextLine();

        System.out.print("Reads per file:\n> ");
        int readsPerFile = Integer.parseInt(scanner.nextLine());

        System.out.print("Number of threads:\n> ");
        int numThreads = Integer.parseInt(scanner.nextLine());

        readFiles(localFilePath, readsPerFile, numThreads, OP_READ_FILES);
    }

    public static DistributedBenchmarkResult deleteFilesOperation(DistributedFileSystem sharedHdfs) {
        System.out.print("Path to file containing HopsFS paths? \n> ");
        String input = scanner.nextLine();
        return deleteFiles(input, sharedHdfs);
    }

    /**
     * Delete the files listed in the file specified by the path argument.
     * @param localPath Text file containing HopsFS file paths to delete.
     */
    public static DistributedBenchmarkResult deleteFiles(String localPath, DistributedFileSystem sharedHdfs) {
        List<String> paths;
        try {
            paths = Utils.getFilePathsFromFile(localPath);
        } catch (FileNotFoundException ex) {
            LOG.error("Could not find file: '" + localPath + "'");
            return null;
        }

        int numSuccessfulDeletes = 0;
        long s = System.currentTimeMillis();
        SynchronizedDescriptiveStatistics latencies = new SynchronizedDescriptiveStatistics();
        for (String path : paths) {
            try {
                Path filePath = new Path(Commander.NAME_NODE_ENDPOINT + path);
                long start = System.currentTimeMillis();
                boolean success = sharedHdfs.delete(filePath, true);
                long end = System.currentTimeMillis();

                if (success) {
                    LOG.debug("\t Successfully deleted '" + path + "' in " + (end - start) + " ms.");
                    numSuccessfulDeletes++;
                }
                else
                    LOG.error("\t Failed to delete '" + path + "' after " + (end - start) + " ms.");

                latencies.addValue(end - start);
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

        LOG.info("Latencies (ms): ");
        for (double latency : latencies.getValues())
            System.out.println(latency);

        return new DistributedBenchmarkResult(null, OP_DELETE_FILES, paths.size(), durationSeconds, s, t, latencies);
    }

    /**
     * Specify a file on the local filesystem containing a bunch of HopsFS file paths. Read the local file in order
     * to get all the HopsFS file paths, then read those files a configurable number of times.
     * @param path Path to file containing a bunch of HopsFS files.
     * @param readsPerFile Number of times each file should be read.
     * @param numThreads Number of threads to use when performing the reads concurrently.
     */
    public static DistributedBenchmarkResult readFiles(String path, int readsPerFile, int numThreads, int opCode)
            throws InterruptedException {
        List<String> paths;
        try {
            paths = Utils.getFilePathsFromFile(path);
        } catch (FileNotFoundException ex) {
            LOG.error("Could not find file: '" + path + "'");
            return null;
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

        return executeBenchmark(numThreads, pathsPerThread, readsPerFile, opCode,
                FSOperation.READ_FILE);
    }

    /**
     * Write a bunch of files to a target directory.
     */
    public static void writeFilesToDirectory()
            throws InterruptedException, IOException {
        System.out.print("Target directory:\n> ");
        String targetDirectory = scanner.nextLine();

        System.out.print("Number of files:\n> ");
        int numFiles = Integer.parseInt(scanner.nextLine());

        // If 'y', create the files one-by-one. If 'n', we'll use a configurable number of threads.
        System.out.print("Sequentially create files? [Y/n]\n>");
        String resp = scanner.nextLine();

        int numThreads = 1;
        // If they answered anything other than 'y', then abort.
        if (resp.equalsIgnoreCase("n")) {
            System.out.print("Number of threads:\n> ");
            numThreads = Integer.parseInt(scanner.nextLine());
        }

        writeFilesInternal(numFiles, numThreads, Collections.singletonList(targetDirectory),
                OP_WEAK_SCALING_WRITES, false, "N/A", false);
    }

    public static DistributedBenchmarkResult mkdirWeakScaling(int mkdirsPerDirectory, int numThreads,
                                                              List<String> targetDirectories, int opCode,
                                                              boolean randomWrites,String operationId,
                                                              boolean writePathsToFile)
            throws IOException, InterruptedException {
        // Generate the file contents and file names. targetDirectories has length equal to numThreads
        // except when randomWrites is true (in which case, in may vary). But either way, each thread
        // will be reading n files, so this works.
        int totalNumberOfDirectories = mkdirsPerDirectory * numThreads;
        LOG.info("Generating " + mkdirsPerDirectory + " directories for each directory (total of " +
                totalNumberOfDirectories + " files).");
        final String[] targetPaths = new String[totalNumberOfDirectories];

        // The standard way of generating files. Just generate a bunch of files for each provided directory.
        if (!randomWrites) {
            int counter = 0;
            // Generate file names and subsequently the full file paths.
            for (String targetDirectory : targetDirectories) {
                // File names.
                String[] targetDirs = Utils.getFixedLengthRandomStrings(mkdirsPerDirectory, 15);

                // Create the full paths.
                for (String targetDir : targetDirs) {
                    targetPaths[counter++] = targetDirectory + "/" + "dir_" + targetDir + "/";
                }
            }
        } else {
            // Generate truly random reads using the `targetDirectories` list as the sample space from which
            // we draw random directories to write to. We generate random writes with replacement from the
            // targetDirectories list.
            Random rng = new Random();

            // Generate the filenames. These will be appended to the end of the directories.
            Utils.getFixedLengthRandomStrings(totalNumberOfDirectories, 20, targetPaths);

            for (int i = 0; i < totalNumberOfDirectories; i++) {
                // Randomly select a directory from the list of all target directories.
                String directory = targetDirectories.get(rng.nextInt(targetDirectories.size()));

                // We initially put all the randomly-generated filenames in 'targetPaths'. Now, we prepend each
                // randomly-generated filename with the randomly-selected directory. We used targetPaths to store
                // the filenames first just to avoid allocating too many arrays for large read tests.
                targetPaths[i] = directory + "/" + "dir_" + targetPaths[i] + "/";
            }
        }

        LOG.info("Generated a total of " + totalNumberOfDirectories + " file(s).");

        Utils.write("./output/mkdirInDirectoryPaths-" + Instant.now().toEpochMilli() + ".txt", targetPaths);

        // TODO: Are we splitting this correctly?
        int numMkdirsPerThread = targetPaths.length / numThreads;
        final String[][] targetPathsPerThread = Utils.splitArray(targetPaths, numMkdirsPerThread);
        assert (targetPathsPerThread != null);
        assert (targetPathsPerThread.length == numThreads);

        DistributedBenchmarkResult result = executeBenchmark(numThreads, targetPathsPerThread, 1, opCode,
                FSOperation.MKDIRS);

        if (writePathsToFile) {
            String dirPath = "./weakScalingMkdirDirectoriesCreated/";
            String filePath = dirPath.concat(operationId + "-" + result.jvmId + ".txt");

            LOG.debug("Writing HopsFS directory paths to file: '" + filePath + "'");

            File dir = new File(dirPath);
            if (!dir.exists())
                dir.mkdir();

            File file = new File(filePath);
            try {
                FileWriter fw = new FileWriter(file.getAbsoluteFile());
                BufferedWriter bw = new BufferedWriter(fw);
                for (String path : targetPaths) {
                    bw.write(path + "\n");
                }
                bw.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        return result;
    }

    /**
     * Write a bunch of files to a bunch of directories.
     *
     * The {@code targetDirectories} list is expected to have size equal to {@code numThreads}, unless
     * {@code randomWrites} is true. When {@code randomWrites} is true, we just generate a bunch of random writes
     * using all provided directories as part of the sample space.
     *
     * @param filesPerDirectory Number of files per directory (or per thread for random writes).
     * @param numThreads The number of threads to use when performing the operation.
     * @param targetDirectories The target directories.
     * @param randomWrites Generate a bunch of random writes across all directories,
     *                     rather than doing the writes per-directory.
     */
    public static DistributedBenchmarkResult writeFilesInternal(int filesPerDirectory, int numThreads,
                                                                List<String> targetDirectories, int opCode,
                                                                boolean randomWrites, String operationId,
                                                                boolean writePathsToFile)
            throws IOException, InterruptedException {
        // Generate the file contents and file names. targetDirectories has length equal to numThreads
        // except when randomWrites is true (in which case, in may vary). But either way, each thread
        // will be reading n files, so this works.
        int totalNumberOfFiles = filesPerDirectory * numThreads;
        LOG.info("Generating " + filesPerDirectory + " files for each directory (total of " + totalNumberOfFiles + " files).");
        final String[] targetPaths = new String[totalNumberOfFiles];

        // The standard way of generating files. Just generate a bunch of files for each provided directory.
        if (!randomWrites) {
            int counter = 0;
            // Generate file names and subsequently the full file paths.
            for (String targetDirectory : targetDirectories) {
                // File names.
                String[] targetFiles = Utils.getFixedLengthRandomStrings(filesPerDirectory, 15);

                // Create the full paths.
                for (String targetFile : targetFiles) {
                    targetPaths[counter++] = targetDirectory + "/" + targetFile;
                }
            }
        } else {
            // Generate truly random reads using the `targetDirectories` list as the sample space from which
            // we draw random directories to write to. We generate random writes with replacement from the
            // targetDirectories list.
            Random rng = new Random();

            // Generate the filenames. These will be appended to the end of the directories.
            Utils.getFixedLengthRandomStrings(totalNumberOfFiles, 20, targetPaths);

            for (int i = 0; i < totalNumberOfFiles; i++) {
                // Randomly select a directory from the list of all target directories.
                String directory = targetDirectories.get(rng.nextInt(targetDirectories.size()));

                // We initially put all the randomly-generated filenames in 'targetPaths'. Now, we prepend each
                // randomly-generated filename with the randomly-selected directory. We used targetPaths to store
                // the filenames first just to avoid allocating too many arrays for large read tests.
                targetPaths[i] = directory + "/" + targetPaths[i];
            }
        }

        LOG.info("Generated a total of " + totalNumberOfFiles + " file(s).");

        Utils.write("./output/writeToDirectoryPaths-" + Instant.now().toEpochMilli()+ ".txt", targetPaths);

        // TODO: Are we splitting this correctly?
        int numWritesPerThread = targetPaths.length / numThreads;
        final String[][] targetPathsPerThread = Utils.splitArray(targetPaths, numWritesPerThread);
        assert(targetPathsPerThread != null);
        assert(targetPathsPerThread.length == numThreads);

        DistributedBenchmarkResult result = executeBenchmark(numThreads, targetPathsPerThread, 1, opCode,
                FSOperation.CREATE_FILE);

        if (writePathsToFile) {
            String dirPath = "./weakScalingWriteFilesCreated/";
            String filePath = dirPath.concat(operationId + "-" + result.jvmId + ".txt");

            LOG.debug("Writing HopsFS file paths to file: '" + filePath + "'");

            File dir = new File(dirPath);
            if (!dir.exists())
                dir.mkdir();

            File file = new File(filePath);
            try {
                FileWriter fw = new FileWriter(file.getAbsoluteFile());
                BufferedWriter bw = new BufferedWriter(fw);
                for (String path : targetPaths) {
                    bw.write(path + "\n");
                }
                bw.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        return result;
    }

    /**
     * Check if the user is trying to cancel the current operation.
     *
     * @param input The user's input.
     */
    private static void checkForCancel(String input) {
        if (input.equalsIgnoreCase("abort") || input.equalsIgnoreCase("cancel") ||
                input.equalsIgnoreCase("exit"))
            throw new IllegalArgumentException("User specified '" + input + "'. Aborting operation.");
    }

    public static boolean getFileStatus(String path, DistributedFileSystem hdfs) {
        Path filePath = new Path(Commander.NAME_NODE_ENDPOINT + path);
        try {
            FileStatus status = hdfs.getFileStatus(filePath);
            if (status == null) {
                LOG.warn("Received null result from getFileInfo('" + filePath + "')...");
                return false;
            }
            return true;
        } catch (Exception ex) {
            ex.printStackTrace();
        }

        return false;
    }

    public static void createDirectories(DistributedFileSystem hdfs) {
        System.out.print("Base name for the directories:\n> ");
        String baseDirName = scanner.nextLine();

        int numDirectoriesToCreate = getIntFromUser("How many directories should be created?");

        long s = System.currentTimeMillis();
        for (int i = 0; i < numDirectoriesToCreate; i++) {
            mkdir(baseDirName + i + "/", hdfs);
        }
        long t = System.currentTimeMillis();

        double durationSeconds = (t - s) / 1000.0;
        double throughput = (double)numDirectoriesToCreate / durationSeconds;

        LOG.info("Created " + numDirectoriesToCreate + " directories in " + durationSeconds + " seconds.");
        LOG.info("Throughput: " + throughput + " ops/sec.");
    }

    public static void createSubtree(DistributedFileSystem hdfs) throws IOException {
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

        int numDirectoriesCreated = 0;
        int filesCreated = 0;

        Instant start = Instant.now();

        int currentDepth = 0;

        mkdir(subtreeRootPath, hdfs);
        numDirectoriesCreated++;

        Set<String> directoriesCreated = new HashSet<>();
        Stack<TreeNode> directoryStack = new Stack<>();
        TreeNode subtreeRoot = new TreeNode(subtreeRootPath, new ArrayList<>());
        directoryStack.push(subtreeRoot);

        while (currentDepth <= subtreeDepth) {
            LOG.info("=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=");
            LOG.info("CURRENT DEPTH: " + currentDepth);
            LOG.info("DIRECTORIES CREATED: " + numDirectoriesCreated);
            LOG.info("=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=");
            List<Stack<TreeNode>> currentDepthStacks = new ArrayList<>();
            while (!directoryStack.empty()) {
                TreeNode directory = directoryStack.pop();
                //directoriesCreated.add(directory.getPath());

                String basePath = directory.getPath() + "/dir";

                Stack<TreeNode> stack = createChildDirectories(basePath, maxSubDirs, hdfs, directoriesCreated);
                directory.addChildren(stack);
                numDirectoriesCreated += stack.size();
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
        LOG.info("Directories created: " + numDirectoriesCreated);
        LOG.info("Files created: " + filesCreated + "\n");

        LOG.info("subtreeRoot children: " + subtreeRoot.getChildren().size());
        LOG.info("directoriesCreated.size(): " + directoriesCreated.size());
        LOG.info(subtreeRoot.toString());

//        for (String path : directoriesCreated) {
//            System.out.println(path);
//        }

        Utils.write("./output/createSubtree-" + Instant.now().toEpochMilli()+ ".txt", directoriesCreated.toArray(new String[0]));

        LOG.info("==================================");
    }

    public static Stack<TreeNode> createChildDirectories(String basePath, int subDirs,
                                                         DistributedFileSystem hdfs,
                                                         Collection<String> directoriesCreated) {
        Stack<TreeNode> directoryStack = new Stack<>();
        for (int i = 0; i < subDirs; i++) {
            String path = basePath + i;
            mkdir(path, hdfs);
            directoriesCreated.add(path);
            TreeNode node = new TreeNode(path, new ArrayList<>());
            directoryStack.push(node);
        }

        return directoryStack;
    }

    public static void createFileOperation(DistributedFileSystem hdfs) {
        System.out.print("File path:\n> ");
        String fileName = scanner.nextLine();

        checkForCancel(fileName);

        System.out.print("File contents:\n> ");
        String fileContents = scanner.nextLine().trim();

        checkForCancel(fileName);

        createFile(fileName, fileContents, hdfs);
    }

    /**
     * Create a new file with the given name and contents.
     * @param name The name of the file.
     * @param contents The content to be written to the file.
     *
     * @return True if the create operation succeeds, otherwise false.
     */
    public static boolean createFile(String name,
                                     String contents,
                                     DistributedFileSystem hdfs) {
        Path filePath = new Path(Commander.NAME_NODE_ENDPOINT + name);

        try {
            FSDataOutputStream outputStream = hdfs.create(filePath);

            if (!contents.isEmpty()) {
                BufferedWriter br = new BufferedWriter(new OutputStreamWriter(outputStream, StandardCharsets.UTF_8));
                br.write(contents);
                br.close();
                // LOG.info("\t Successfully created non-empty file '" + filePath + "'");
            } else {
                // LOG.info("\t Successfully created empty file '" + filePath + "'");
                outputStream.close();
            }

            return true;
        } catch (IOException ex) {
            ex.printStackTrace();
        }

        return false;
    }

    /**
     * Check if the user is trying to cancel the current operation.
     *
     * @param input The user's input.
     */
    private static void checkForExit(String input) {
        if (input.equalsIgnoreCase("abort") || input.equalsIgnoreCase("cancel") ||
                input.equalsIgnoreCase("exit"))
            throw new IllegalArgumentException("User specified '" + input + "'. Aborting operation.");
    }

    public static void renameOperation(DistributedFileSystem hdfs) {
        System.out.print("Original file path:\n> ");
        String originalFileName = scanner.nextLine();
        checkForExit(originalFileName);

        System.out.print("Renamed file path:\n> ");
        String renamedFileName = scanner.nextLine();
        checkForExit(renamedFileName);

        Path filePath = new Path(Commander.NAME_NODE_ENDPOINT + originalFileName);
        Path filePathRename = new Path(Commander.NAME_NODE_ENDPOINT + renamedFileName);

        try {
            LOG.info("\t Original file path: \"" + originalFileName + "\"");
            LOG.info("\t New file path: \"" + renamedFileName + "\"");
            hdfs.rename(filePath, filePathRename);
            LOG.info("\t Finished rename operation.");
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    public static boolean listDirectory(DistributedFileSystem hdfs, String targetDirectory) {
        try {
            FileStatus[] fileStatus = hdfs.listStatus(new Path(Commander.NAME_NODE_ENDPOINT + targetDirectory));
            LOG.info("Directory '" + targetDirectory + "' contains " + fileStatus.length + " files.");
            for (FileStatus status : fileStatus)
                LOG.info(status.getPath().toString());
            LOG.info("Directory '" + targetDirectory + "' contains " + fileStatus.length + " files.");
            return true;
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        return false;
    }

    public static boolean listDirectoryNoPrint(DistributedFileSystem hdfs, String targetDirectory) {
        try {
            hdfs.listStatus(new Path(Commander.NAME_NODE_ENDPOINT + targetDirectory));
            return true;
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        return false;
    }

    public static boolean listOperation(DistributedFileSystem hdfs) {
        System.out.print("Target directory:\n> ");
        String targetDirectory = scanner.nextLine();

        return listDirectory(hdfs, targetDirectory);
    }

    /**
     * Create a new directory with the given path.
     * @param path The path of the new directory.
     */
    public static boolean mkdir(String path, DistributedFileSystem hdfs) {
        Path filePath = new Path(Commander.NAME_NODE_ENDPOINT + path);

        try {
            // LOG.info("\t Attempting to create new directory: \"" + path + "\"");
            // LOG.info("\t Directory created successfully: " + directoryCreated);

            return hdfs.mkdirs(filePath);
        } catch (IOException ex) {
            ex.printStackTrace();
        }

        return false;
    }

    public static boolean exists(DistributedFileSystem hdfs, String targetPath) {
        Path filePath = new Path(Commander.NAME_NODE_ENDPOINT + targetPath);

        try {
            return hdfs.exists(filePath);
        } catch (IOException ex) {
            ex.printStackTrace();
            return false;
        }
    }

    public static void mkdirOperation(DistributedFileSystem hdfs) {
        System.out.print("New directory path:\n> ");
        String newDirectoryName = scanner.nextLine();

        mkdir(newDirectoryName, hdfs);
    }

    public static void appendOperation(DistributedFileSystem hdfs) {
        System.out.print("File path:\n> ");
        String fileName = scanner.nextLine();
        System.out.print("Content to append:\n> ");
        String fileContents = scanner.nextLine();

        Path filePath = new Path(Commander.NAME_NODE_ENDPOINT + fileName);

        try {
            FSDataOutputStream outputStream = hdfs.append(filePath);
            LOG.info("\t Called append() successfully.");
            BufferedWriter br = new BufferedWriter(new OutputStreamWriter(outputStream, StandardCharsets.UTF_8));
            LOG.info("\t Created BufferedWriter object.");
            br.write(fileContents);
            LOG.info("\t Appended \"" + fileContents + "\" to file using BufferedWriter.");
            br.close();
            LOG.info("\t Closed BufferedWriter.");
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    public static void readOperation(DistributedFileSystem hdfs) {
        System.out.print("File path:\n> ");
        String fileName = scanner.nextLine();
        readFile(fileName, hdfs);
    }

    /**
     * Read the HopsFS/HDFS file at the given path.
     * @param fileName The path to the file to read.
     * @return The latency of the operation in nanoseconds.
     */
    public static boolean readFile(String fileName, DistributedFileSystem hdfs) {
        Path filePath = new Path(Commander.NAME_NODE_ENDPOINT + fileName);

        try {
            FSDataInputStream inputStream = hdfs.open(filePath);

            BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
            String line;

            while ((line = br.readLine()) != null)
                LOG.info(line);
            inputStream.close();
            br.close();
            return true;
        } catch (IOException ex) {
            ex.printStackTrace();
            return false;
        }
    }

    private static int getIntFromUser(String prompt) {
        System.out.print(prompt + "\n> ");
        String input = scanner.nextLine();
        checkForCancel(input);
        return Integer.parseInt(input);
    }

    public static boolean delete(String targetPath, DistributedFileSystem hdfs) {
        Path filePath = new Path(Commander.NAME_NODE_ENDPOINT + targetPath);

        try {
            return hdfs.delete(filePath, true);
        } catch (IOException ex) {
            ex.printStackTrace();
        }

        return false;
    }

    public static void deleteOperation(DistributedFileSystem hdfs) {
        System.out.print("File or directory path:\n> ");
        String targetPath = scanner.nextLine();

        long start = System.currentTimeMillis();
        delete(targetPath, hdfs);
        LOG.info("Completed DELETE on target '" + targetPath + "' in " +
                (System.currentTimeMillis() - start) + " ms.");
    }
}
