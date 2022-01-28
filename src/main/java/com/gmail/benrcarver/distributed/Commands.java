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
import java.util.concurrent.TimeUnit;

import io.hops.metrics.TransactionEvent;
import io.hops.metrics.TransactionAttempt;
import io.hops.transaction.context.TransactionsStats;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.fs.FileStatus;
import io.hops.metrics.OperationPerformed;

import static com.gmail.benrcarver.distributed.Constants.OP_STRONG_SCALING;

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

        int minLength = 5;
        System.out.print("Min string length (default " + minLength + "):\n> ");
        try {
            minLength = Integer.parseInt(scanner.nextLine());
        } catch (NumberFormatException ex) {
            LOG.info("Defaulting to " + minLength + ".");
        }

        int maxLength = 10;
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
        final CountDownLatch latch = new CountDownLatch(numThreads);

        final BlockingQueue<List<OperationPerformed>> operationsPerformed =
                new java.util.concurrent.ArrayBlockingQueue<>(numThreads);
        final BlockingQueue<HashMap<String, TransactionsStats.ServerlessStatisticsPackage>> statisticsPackages
                = new ArrayBlockingQueue<>(numThreads);
        final BlockingQueue<HashMap<String, List<TransactionEvent>>> transactionEvents
                = new ArrayBlockingQueue<>(numThreads);

        for (int i = 0; i < numThreads; i++) {
            Thread thread = new Thread(() -> {
                DistributedFileSystem hdfs = new DistributedFileSystem();

                try {
                    hdfs.initialize(new URI(nameNodeEndpoint), configuration);
                } catch (URISyntaxException | IOException ex) {
                    LOG.error("ERROR: Encountered exception while initializing DistributedFileSystem object.");
                    ex.printStackTrace();
                    System.exit(1);
                }

                latch.countDown();

                for (String filePath : selectedPaths) {
                    for (int j = 0; j < readsPerFile; j++)
                        readFile(filePath, hdfs, nameNodeEndpoint);
                }

                operationsPerformed.add(hdfs.getOperationsPerformed());
                statisticsPackages.add(hdfs.getStatisticsPackages());
                transactionEvents.add(hdfs.getTransactionEvents());

                try {
                    hdfs.close();
                } catch (IOException ex) {
                    LOG.error("Encountered IOException while closing DistributedFileSystem object:", ex);
                }
            });
            threads[i] = thread;
        }

        LOG.info("Starting threads.");
        Instant start = Instant.now();
        for (Thread thread : threads) {
            thread.start();
        }

        LOG.info("Joining threads.");
        for (Thread thread : threads) {
            thread.join();
        }
        Instant end = Instant.now();
        Duration duration = Duration.between(start, end);

        for (List<OperationPerformed> opsPerformed : operationsPerformed) {
            LOG.info("Adding list of " + opsPerformed.size() +
                    " operations performed to master/shared HDFS object.");
            sharedHdfs.addOperationPerformeds(opsPerformed);
        }

        for (HashMap<String, TransactionsStats.ServerlessStatisticsPackage> statPackages : statisticsPackages) {
            LOG.info("Adding list of " + statPackages.size() +
                    " statistics packages to master/shared HDFS object.");
            sharedHdfs.mergeStatisticsPackages(statPackages, true);
        }

        for (HashMap<String, List<TransactionEvent>> txEvents : transactionEvents) {
            LOG.info("Merging " + txEvents.size() + " new transaction event(s) into master/shared HDFS object.");
            sharedHdfs.mergeTransactionEvents(txEvents, true);
        }

        double durationSeconds = duration.getSeconds() + (duration.getNano() / 1e9);
        double totalReads = (double)n * (double)readsPerFile * (double)numThreads;
        double throughput = (totalReads / durationSeconds);
        LOG.info("Finished performing all " + totalReads + " file reads in " + duration);
        LOG.info("Throughput: " + throughput + " ops/sec.");

        return new DistributedBenchmarkResult(null, OP_STRONG_SCALING, (int)totalReads, durationSeconds,
                start.toEpochMilli(), end.toEpochMilli());
    }

    /**
     * Read N files using N threads (so, each thread reads 1 file). Each file is read a specified number of times.
     *
     * @param n Number of files to read.
     * @param readsPerFile How many times each file should be read.
     * @param inputPath Path to local file containing HopsFS filepaths (of the files to read).
     */
    public static DistributedBenchmarkResult readNFiles(final Configuration configuration,
                                  final DistributedFileSystem sharedHdfs,
                                  final String nameNodeEndpoint,
                                  int n,
                                  int readsPerFile,
                                  String inputPath) throws InterruptedException, FileNotFoundException {
        List<String> paths = Utils.getFilePathsFromFile(inputPath);

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

        final java.util.concurrent.BlockingQueue<List<OperationPerformed>> operationsPerformed =
                new java.util.concurrent.ArrayBlockingQueue<>(n);
        final BlockingQueue<HashMap<String, TransactionsStats.ServerlessStatisticsPackage>> statisticsPackages
                = new ArrayBlockingQueue<>(n);
        final BlockingQueue<HashMap<String, List<TransactionEvent>>> transactionEvents
                = new ArrayBlockingQueue<>(n);

        for (int i = 0; i < n; i++) {
            final String filePath = paths.get(i);
            Thread thread = new Thread(() -> {
                DistributedFileSystem hdfs = new DistributedFileSystem();

                try {
                    hdfs.initialize(new URI(nameNodeEndpoint), configuration);
                } catch (URISyntaxException | IOException ex) {
                    LOG.error("ERROR: Encountered exception while initializing DistributedFileSystem object.");
                    ex.printStackTrace();
                    System.exit(1);
                }

                latch.countDown();

                for (int j = 0; j < readsPerFile; j++)
                    readFile(filePath, hdfs, nameNodeEndpoint);

                operationsPerformed.add(hdfs.getOperationsPerformed());
                statisticsPackages.add(hdfs.getStatisticsPackages());
                transactionEvents.add(hdfs.getTransactionEvents());

                try {
                    hdfs.close();
                } catch (IOException ex) {
                    LOG.error("Encountered IOException while closing DistributedFileSystem object:", ex);
                }
            });
            threads[i] = thread;
        }

        LOG.info("Starting threads.");
        Instant start = Instant.now();
        for (Thread thread : threads) {
            thread.start();
        }

        LOG.info("Joining threads.");
        for (Thread thread : threads) {
            thread.join();
        }
        Instant end = Instant.now();
        Duration duration = Duration.between(start, end);

        for (List<OperationPerformed> opsPerformed : operationsPerformed) {
            LOG.info("Adding list of " + opsPerformed.size() +
                    " operations performed to master/shared HDFS object.");
            sharedHdfs.addOperationPerformeds(opsPerformed);
        }

        for (HashMap<String, TransactionsStats.ServerlessStatisticsPackage> statPackages : statisticsPackages) {
            LOG.info("Adding list of " + statPackages.size() +
                    " statistics packages to master/shared HDFS object.");
            sharedHdfs.mergeStatisticsPackages(statPackages, true);
        }

        for (HashMap<String, List<TransactionEvent>> txEvents : transactionEvents) {
            LOG.info("Merging " + txEvents.size() + " new transaction event(s) into master/shared HDFS object.");
            sharedHdfs.mergeTransactionEvents(txEvents, true);
        }

        double durationSeconds = duration.getSeconds() + (duration.getNano() / 1e9);
        double totalReads = (double)n * (double)readsPerFile;
        double throughput = (totalReads / durationSeconds);
        LOG.info("Finished performing all " + totalReads + " file reads in " + duration);
        LOG.info("Throughput: " + throughput + " ops/sec.");

        return new DistributedBenchmarkResult(null, OP_STRONG_SCALING, (int)totalReads, durationSeconds,
                start.toEpochMilli(), end.toEpochMilli());
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

            // LOG.info("Writing " + transactionEvents.size() + " transaction event lists to CSV.");

            txEventsWriter.write(TransactionEvent.getHeader());
            txEventsWriter.newLine();

            for (Map.Entry<String, List<TransactionEvent>> entry : transactionEvents.entrySet()) {
                String requestId = entry.getKey();
                List<TransactionEvent> txEvents = entry.getValue();

                // LOG.info("Adding " + txEvents.size() + " transaction events to CSV.");
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

    public static void deleteFilesOperation(DistributedFileSystem sharedHdfs, final String nameNodeEndpoint) {
        System.out.print("Path to file containing HopsFS paths? \n> ");
        String input = scanner.nextLine();
        deleteFiles(input, sharedHdfs, nameNodeEndpoint);
    }

    /**
     * Delete the files listed in the file specified by the path argument.
     * @param localPath Text file containing HopsFS file paths to delete.
     */
    public static void deleteFiles(String localPath, DistributedFileSystem sharedHdfs, final String nameNodeEndpoint) {
        List<String> paths;
        try {
            paths = Utils.getFilePathsFromFile(localPath);
        } catch (FileNotFoundException ex) {
            LOG.error("Could not find file: '" + localPath + "'");
            return;
        }

        for (String path : paths) {
            try {
                Path filePath = new Path(nameNodeEndpoint + path);
                boolean success = sharedHdfs.delete(filePath, true);
                LOG.info("\t Delete was successful: " + success);
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
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

        final java.util.concurrent.BlockingQueue<List<OperationPerformed>> operationsPerformed =
                new java.util.concurrent.ArrayBlockingQueue<>(numThreads);
        final BlockingQueue<HashMap<String, TransactionsStats.ServerlessStatisticsPackage>> statisticsPackages
                = new ArrayBlockingQueue<>(numThreads);
        final BlockingQueue<HashMap<String, List<TransactionEvent>>> transactionEvents
                = new ArrayBlockingQueue<>(numThreads);

        for (int i = 0; i < numThreads; i++) {
            final String[] pathsForThread = pathsPerThread[i];
            Thread thread = new Thread(() -> {
                DistributedFileSystem hdfs = new DistributedFileSystem();

                try {
                    hdfs.initialize(new URI(nameNodeEndpoint), configuration);
                } catch (URISyntaxException | IOException ex) {
                    LOG.error("ERROR: Encountered exception while initializing DistributedFileSystem object.");
                    ex.printStackTrace();
                    System.exit(1);
                }

                latch.countDown();

                for (String filePath : pathsForThread) {
                    for (int j = 0; j < readsPerFile; j++)
                        readFile(filePath, hdfs, nameNodeEndpoint);
                }

                operationsPerformed.add(hdfs.getOperationsPerformed());
                statisticsPackages.add(hdfs.getStatisticsPackages());
                transactionEvents.add(hdfs.getTransactionEvents());

                try {
                    hdfs.close();
                } catch (IOException ex) {
                    LOG.error("Encountered IOException while closing DistributedFileSystem object:", ex);
                }
            });
            threads[i] = thread;
        }

        LOG.info("Starting threads.");
        Instant start = Instant.now();
        for (Thread thread : threads) {
            thread.start();
        }

        LOG.info("Joining threads.");
        for (Thread thread : threads) {
            thread.join();
        }
        Instant end = Instant.now();
        Duration duration = Duration.between(start, end);

        double durationSeconds = duration.getSeconds() + (duration.getNano() / 1e9);
        LOG.info("Finished performing all " + (readsPerFile * paths.size()) + " file reads in " + duration);
        double totalReads = (double)n * (double)readsPerFile;
        double throughput = (totalReads / durationSeconds);
        LOG.info("Throughput: " + throughput + " ops/sec.");

        for (List<OperationPerformed> opsPerformed : operationsPerformed) {
            LOG.info("Adding list of " + opsPerformed.size() +
                    " operations performed to master/shared HDFS object.");
            sharedHdfs.addOperationPerformeds(opsPerformed);
        }

        for (HashMap<String, TransactionsStats.ServerlessStatisticsPackage> statPackages : statisticsPackages) {
            LOG.info("Adding list of " + statPackages.size() +
                    " statistics packages to master/shared HDFS object.");
            sharedHdfs.mergeStatisticsPackages(statPackages, true);
        }

        for (HashMap<String, List<TransactionEvent>> txEvents : transactionEvents) {
            LOG.info("Merging " + txEvents.size() + " new transaction event(s) into master/shared HDFS object.");
            sharedHdfs.mergeTransactionEvents(txEvents, true);
        }
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
    public static void writeFilesInternal(int n, int minLength, int maxLength, int numThreads,
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

            for (int i = 0; i < targetFiles.length; i++) {
                targetPaths[counter++] = targetDirectory + "/" + targetFiles[i];
            }
        }

        LOG.info("Generated a total of " + totalNumberOfFiles + " file(s).");

        Utils.write("./output/writeToDirectoryPaths-" + Instant.now().toEpochMilli()+ ".txt", targetPaths);

        Instant start;
        Instant end;
        if (numThreads == 1) {
            start = Instant.now();

            createFiles(targetPaths, content, sharedHdfs, nameNodeEndpoint);

            end = Instant.now();
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

            Thread[] threads = new Thread[numThreads];

            final java.util.concurrent.BlockingQueue<List<OperationPerformed>> operationsPerformed =
                    new java.util.concurrent.ArrayBlockingQueue<>(numThreads);
            final BlockingQueue<HashMap<String, TransactionsStats.ServerlessStatisticsPackage>> statisticsPackages
                    = new ArrayBlockingQueue<>(numThreads);
            final BlockingQueue<HashMap<String, List<TransactionEvent>>> transactionEvents
                    = new ArrayBlockingQueue<>(numThreads);

            for (int i = 0; i < numThreads; i++) {
                final int idx = i;
                Thread thread = new Thread(() -> {
                    DistributedFileSystem hdfs = new DistributedFileSystem();

                    try {
                        hdfs.initialize(new URI(nameNodeEndpoint), configuration);
                    } catch (URISyntaxException | IOException ex) {
                        LOG.error("ERROR: Encountered exception while initializing DistributedFileSystem object.");
                        ex.printStackTrace();
                        System.exit(1);
                    }

                    latch.countDown();
                    createFiles(targetPathsPerArray[idx], contentPerArray[idx], hdfs, nameNodeEndpoint);

                    operationsPerformed.add(hdfs.getOperationsPerformed());
                    statisticsPackages.add(hdfs.getStatisticsPackages());
                    transactionEvents.add(hdfs.getTransactionEvents());

                    try {
                        hdfs.close();
                    } catch (IOException ex) {
                        LOG.error("Encountered IOException while closing DistributedFileSystem object:", ex);
                    }
                });
                threads[i] = thread;
            }

            LOG.info("Starting threads.");
            start = Instant.now();
            for (Thread thread : threads) {
                thread.start();
            }

            LOG.info("Joining threads.");
            for (Thread thread : threads) {
                thread.join();
            }
            end = Instant.now();

            for (List<OperationPerformed> opsPerformed : operationsPerformed) {
                LOG.info("Adding list of " + opsPerformed.size() +
                        " operations performed to master/shared HDFS object.");
                sharedHdfs.addOperationPerformeds(opsPerformed);
            }

            for (HashMap<String, TransactionsStats.ServerlessStatisticsPackage> statPackages : statisticsPackages) {
                LOG.info("Adding list of " + statPackages.size() +
                        " statistics packages to master/shared HDFS object.");
                sharedHdfs.mergeStatisticsPackages(statPackages, true);
            }

            for (HashMap<String, List<TransactionEvent>> txEvents : transactionEvents) {
                LOG.info("Merging " + txEvents.size() + " new transaction event(s) into master/shared HDFS object.");
                sharedHdfs.mergeTransactionEvents(txEvents, true);
            }
        }

        Duration duration = Duration.between(start, end);
        float durationSeconds = duration.getSeconds() + TimeUnit.NANOSECONDS.toSeconds(duration.getNano());
        filesPerSec = totalNumberOfFiles / durationSeconds;
        LOG.info("");
        LOG.info("");
        LOG.info("===============================");
        LOG.info("Time elapsed: " + duration.toString());
        LOG.info("Aggregate throughput: " + filesPerSec + " ops/sec.");
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
     */
    public static void createFiles(String[] names, String[] content, DistributedFileSystem hdfs, String nameNodeEndpoint) {
        assert(names.length == content.length);

        for (int i = 0; i < names.length; i++) {
            LOG.info("Writing file " + i + "/" + names.length);
            createFile(names[i], content[i], hdfs, nameNodeEndpoint);
        }
    }

    /**
     * Create a new file with the given name and contents.
     * @param name The name of the file.
     * @param contents The content to be written to the file.
     */
    public static void createFile(String name, String contents,
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
        } catch (IOException ex) {
            ex.printStackTrace();
        }
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
            long readStart = System.currentTimeMillis();

            LOG.info("");
            LOG.info("CONTENTS OF FILE '" + fileName + "': ");
            while ((line = br.readLine()) != null)
                LOG.info(line);
            LOG.info("");
            long readEnd = System.currentTimeMillis();
            inputStream.close();
            br.close();
            long readDuration = readEnd - readStart;

            LOG.info("Read contents of file \"" + fileName + "\" from DataNode in " + readDuration + " milliseconds.");

//            OperationPerformed operationPerformed = new OperationPerformed(
//                    "ReadBlocksFromDataNode", UUID.randomUUID().toString(), readStart, readEnd,
//                    readStart, readEnd, readStart, readEnd, 999, false, true, "TCP", 0L, 0, 0);
//            hdfs.addOperationPerformed(operationPerformed);
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    public static void deleteOperation(DistributedFileSystem hdfs, final String nameNodeEndpoint) {
        System.out.print("File or directory path:\n> ");
        String targetPath = scanner.nextLine();

        Path filePath = new Path(nameNodeEndpoint + targetPath);

        try {
            boolean success = hdfs.delete(filePath, true);
            LOG.info("\t Delete was successful: " + success);
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }
}
