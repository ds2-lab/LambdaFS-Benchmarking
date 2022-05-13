package com.gmail.benrcarver.distributed;

import com.gmail.benrcarver.distributed.util.TreeNode;
import com.gmail.benrcarver.distributed.util.Utils;
import org.apache.commons.cli.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import io.hops.metrics.TransactionEvent;
import io.hops.metrics.TransactionAttempt;
import io.hops.transaction.context.TransactionsStats;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.fs.FileStatus;
import io.hops.metrics.OperationPerformed;

import java.util.concurrent.ArrayBlockingQueue;
import java.io.*;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * This is the class that is executed when running the application. It starts the Commander or Follower.
 */
public class InteractiveTest {
    public static final Log LOG = LogFactory.getLog(InteractiveTest.class);

    private final static String namenodeEndpoint = "hdfs://10.150.0.17:9000/";

    private static final Scanner scanner = new Scanner(System.in);
    //private static DistributedFileSystem hdfs;

    /////////////
    // OPTIONS //
    /////////////

    /**
     * If true, then all clients will issue a PING operation first, followed by whatever FS operation we're trying to
     * do. The purpose of this is to potentially establish a TCP connection with the target NameNode before performing
     * multiple operations in a row. There isn't really a point to doing this for a one-off operation, but if we're
     * about to read/write a bunch of files, then it would make sense to establish a TCP connection right away so
     * ALL subsequent operations are issued via TCP.
     */
    private static boolean pingFirst = false;

    public static void main(String[] args) throws InterruptedException, IOException {
        Options cmdLineOpts = new Options();
        Option workerOpt = new Option("w", "worker", false, "If true, run this program as a worker, listening to commands from a remote leader.");
        Option leaderIpOpt = new Option("l", "leader_ip", true, "The IP address of the Leader. Only used when this process is designated as a worker.");
        Option leaderPort = new Option("p", "leader_port", true, "The port of the Leader. Only used when this process is designated as a worker.");
        Option localOption = new Option("n", "nondistributed", false, "Run in non-distributed mode, meaning we don't launch any followers.");
        //Option logLevelOption = new Option("ll", "loglevel", true, "The log4j log level to pass to the NameNodes.");
        Option consistencyProtocolOption = new Option("c", "disable_consistency", false, "If passed, then we default to disabling the consistency protocol.");
        Option numFollowersOpt = new Option("f", "num_followers", true, "Start only the first 'f' followers listed in the config.");

        Option yamlPath = new Option("y", "yaml_path", true, "Path to YAML configuration file.");

        cmdLineOpts.addOption(workerOpt);
        cmdLineOpts.addOption(leaderIpOpt);
        cmdLineOpts.addOption(leaderPort);
        cmdLineOpts.addOption(yamlPath);
        cmdLineOpts.addOption(localOption);
        cmdLineOpts.addOption(numFollowersOpt);
        //cmdLineOpts.addOption(logLevelOption);
        cmdLineOpts.addOption(consistencyProtocolOption);

        CommandLineParser parser = new GnuParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd = null;

        try {
            cmd = parser.parse(cmdLineOpts, args);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            formatter.printHelp("utility-name", cmdLineOpts);

            System.exit(1);
        }

        int numFollowers = -1;
        if (cmd.hasOption("num_followers"))
            numFollowers = Integer.parseInt(cmd.getOptionValue("num_followers"));

        if (cmd.hasOption("worker")) {
            LOG.info("Beginning execution as FOLLOWER now.");
            Follower follower = new Follower(
                    cmd.getOptionValue("leader_ip"),
                    Integer.parseInt(cmd.getOptionValue("leader_port")),
                    //cmd.hasOption("loglevel") ? cmd.getOptionValue("loglevel") : "DEBUG",
                    cmd.hasOption("disable_consistency") /* If it has this option, then it is true */);
            follower.connect();
            follower.waitUntilDone();
        } else {
            Commander commander = Commander.getOrCreateCommander(
                    cmd.getOptionValue("leader_ip"),
                    Integer.parseInt(cmd.getOptionValue("leader_port")),
                    cmd.getOptionValue("yaml_path"),
                    cmd.hasOption("nondistributed"), /* If it has this option, then it is true */
                    cmd.hasOption("disable_consistency"), /* If it has this option, then it is true */
                    numFollowers);
            commander.start();
        }
    }

    private static void showOptionsMenu() {
        LOG.info("====== OPTIONS ======");
        LOG.info("(1) PING FIRST: " + pingFirst);
        LOG.info("=====================");
    }

    private static void optionsOperation() {
        LOG.info("Welcome to the Options Menu.");
        LOG.info("Enter the integer corresponding to a given option to modify the value of that option.");
        LOG.info("For boolean options, entering their associated integer simply flips the value of the option.");
        LOG.info("Specify an integer <= 0 to exit the Options Menu.\n");

        while (true) {
            showOptionsMenu();
            System.out.print("Select an option (or enter <= 0 to return to standard menu):\n> ");

            String userInput = scanner.nextLine();
            int selectedOption = 0;

            try {
                selectedOption = Integer.parseInt(userInput);

                if (selectedOption > 1)
                    throw new IllegalArgumentException("Invalid integer entered by user: " + selectedOption);
            } catch (Exception ex) {
                LOG.error("Invalid input: '" + userInput +
                        "'. Please enter a valid integer associated with an option or an integer <= 0 to exit.");
            }

            if (selectedOption <= 0) {
                LOG.info("Returning to standard menu.");
                return;
            } else if (selectedOption == 1) {
                LOG.info("Setting 'PING FIRST' to " + (!pingFirst));
                pingFirst = !pingFirst;
            } else {
                LOG.error("Invalid input: '" + userInput +
                        "'. Please enter a valid integer associated with an option or an integer <= 0 to exit.");
            }
        }
    }

    private static void writeFilesToDirectories(DistributedFileSystem hdfs, final Configuration configuration) throws IOException, InterruptedException {
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
        writeFilesInternal(n, minLength, maxLength, numberOfThreads, directories, hdfs, configuration);
    }

    private static void clearStatisticsPackages(DistributedFileSystem hdfs) {
        System.out.print("Are you sure? (y/N)\n> ");
        String input = scanner.nextLine();

        if (input.equalsIgnoreCase("y")) {
            hdfs.clearStatistics(true, true, true);
        } else {
            LOG.info("NOT clearing statistics packages.");
        }
    }

    private static void strongScalingBenchmark(final Configuration configuration,
                                               final DistributedFileSystem sharedHdfs)
            throws InterruptedException, FileNotFoundException {
        // User provides file containing HopsFS file paths.
        // Specifies how many files each thread should read.
        // Specifies number of threads.
        // Specifies how many times each file should be read.
        System.out.print("How many files should be read by each thread?\n> ");
        String inputN = scanner.nextLine();
        int n = Integer.parseInt(inputN);

        System.out.print("How many times should each file be read?\n> ");
        String inputReadsPerFile = scanner.nextLine();
        int readsPerFile = Integer.parseInt(inputReadsPerFile);

        System.out.print("Number of threads:\n> ");
        int numThreads = Integer.parseInt(scanner.nextLine());

        System.out.print("Please provide a path to a local file containing at least " + inputN + " HopsFS file " +
                (n == 1 ? "path.\n> " : "paths.\n> "));
        String inputPath = scanner.nextLine();

        List<String> paths = Utils.getFilePathsFromFile(inputPath);

        if (paths.size() < n) {
            LOG.error("ERROR: The file should contain at least " + n +
                    " HopsFS file path(s); however, it contains just " + paths.size() + " HopsFS file path(s).");
            LOG.error("Aborting operation.");
            return;
        }

        // Select a random subset of size n, where n is the number of files each thread should rea.d
        Collections.shuffle(paths);
        List<String> selectedPaths = paths.subList(0, n);

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
            Thread thread = new Thread(() -> {
                DistributedFileSystem hdfs = new DistributedFileSystem();

                try {
                    hdfs.initialize(new URI(namenodeEndpoint), configuration);
                } catch (URISyntaxException | IOException ex) {
                    LOG.error("ERROR: Encountered exception while initializing DistributedFileSystem object.");
                    ex.printStackTrace();
                    System.exit(1);
                }

                latch.countDown();

                for (String filePath : selectedPaths) {
                    for (int j = 0; j < readsPerFile; j++)
                        readFile(filePath, hdfs);
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
    }

    /**
     * Query the user for:
     *  - An integer `n`, the number of files to read
     *  - The path to a local file containing `n` or more HopsFS file paths.
     *  - The number of reads per file.
     *
     * This function will use `n` threads to read those `n` files.
     */
    private static void readNFilesOperation(final Configuration configuration, final DistributedFileSystem sharedHdfs)
            throws InterruptedException, FileNotFoundException {
        System.out.print("How many files should be read?\n> ");
        String inputN = scanner.nextLine();
        int n = Integer.parseInt(inputN);

        System.out.print("How many times should each file be read?\n> ");
        String inputReadsPerFile = scanner.nextLine();
        int readsPerFile = Integer.parseInt(inputReadsPerFile);

        System.out.print("Please provide a path to a local file containing at least " + inputN + " HopsFS file " +
                (n == 1 ? "path.\n> " : "paths.\n> "));
        String inputPath = scanner.nextLine();

        List<String> paths = Utils.getFilePathsFromFile(inputPath);

        if (paths.size() < n) {
            LOG.error("ERROR: The file should contain at least " + n +
                    " HopsFS file path(s); however, it contains just " + paths.size() + " HopsFS file path(s).");
            LOG.error("Aborting operation.");
            return;
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
                    hdfs.initialize(new URI(namenodeEndpoint), configuration);
                } catch (URISyntaxException | IOException ex) {
                    LOG.error("ERROR: Encountered exception while initializing DistributedFileSystem object.");
                    ex.printStackTrace();
                    System.exit(1);
                }

                latch.countDown();

                for (int j = 0; j < readsPerFile; j++)
                    readFile(filePath, hdfs);

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
    }

    /**
     * Print the operations performed. Optionally write them to a CSV.
     */
    private static void printOperationsPerformed(DistributedFileSystem hdfs) throws IOException {
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

    private static void readFilesOperation(final Configuration configuration, DistributedFileSystem sharedHdfs)
            throws InterruptedException {
        System.out.print("Path to local file containing HopsFS/HDFS paths:\n> ");
        String localFilePath = scanner.nextLine();

        System.out.print("Reads per file:\n> ");
        int readsPerFile = Integer.parseInt(scanner.nextLine());

        System.out.print("Number of threads:\n> ");
        int numThreads = Integer.parseInt(scanner.nextLine());

        readFiles(localFilePath, readsPerFile, numThreads, configuration, sharedHdfs);
    }

    private static void deleteFilesOperation(DistributedFileSystem sharedHdfs) {
        System.out.print("Path to file containing HopsFS paths? \n> ");
        String input = scanner.nextLine();
        deleteFiles(input, sharedHdfs);
    }

    /**
     * Delete the files listed in the file specified by the path argument.
     * @param localPath Text file containing HopsFS file paths to delete.
     */
    private static void deleteFiles(String localPath, DistributedFileSystem sharedHdfs) {
        List<String> paths;
        try {
            paths = Utils.getFilePathsFromFile(localPath);
        } catch (FileNotFoundException ex) {
            LOG.error("Could not find file: '" + localPath + "'");
            return;
        }

        for (String path : paths) {
            try {
                Path filePath = new Path(namenodeEndpoint + path);
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
    private static void readFiles(String path, int readsPerFile, int numThreads, final Configuration configuration,
                                  DistributedFileSystem sharedHdfs) throws InterruptedException {
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
                    hdfs.initialize(new URI(namenodeEndpoint), configuration);
                } catch (URISyntaxException | IOException ex) {
                    LOG.error("ERROR: Encountered exception while initializing DistributedFileSystem object.");
                    ex.printStackTrace();
                    System.exit(1);
                }

                latch.countDown();

                for (String filePath : pathsForThread) {
                    for (int j = 0; j < readsPerFile; j++)
                        readFile(filePath, hdfs);
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
    private static void writeFilesToDirectory(DistributedFileSystem sharedHdfs, final Configuration configuration)
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

        writeFilesInternal(n, minLength, maxLength, numThreads, Collections.singletonList(targetDirectory), sharedHdfs, configuration);
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
    private static void writeFilesInternal(int n, int minLength, int maxLength, int numThreads,
                                           List<String> targetDirectories, DistributedFileSystem sharedHdfs,
                                           Configuration configuration) throws IOException, InterruptedException {
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

            createFiles(targetPaths, content, sharedHdfs);

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
                        hdfs.initialize(new URI(namenodeEndpoint), configuration);
                    } catch (URISyntaxException | IOException ex) {
                        LOG.error("ERROR: Encountered exception while initializing DistributedFileSystem object.");
                        ex.printStackTrace();
                        System.exit(1);
                    }

                    latch.countDown();
                    createFiles(targetPathsPerArray[idx], contentPerArray[idx], hdfs);

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

    private static void createSubtree(DistributedFileSystem hdfs) {
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
        if (!resp.toLowerCase().equals("y")) {
            LOG.info("\nAborting.");
            return;
        }

        Random rng = new Random();

        int directoriesCreated = 0;
        int filesCreated = 0;

        Instant start = Instant.now();

        int currentDepth = 0;

        mkdir(subtreeRootPath, hdfs);
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

                Stack<TreeNode> stack = createChildDirectories(basePath, maxSubDirs, hdfs);
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

    private static Stack<TreeNode> createChildDirectories(String basePath, int subDirs, DistributedFileSystem hdfs) {
        Stack<TreeNode> directoryStack = new Stack<TreeNode>();
        for (int i = 0; i < subDirs; i++) {
            String path = basePath + i;
            mkdir(path, hdfs);
            TreeNode node = new TreeNode(path, new ArrayList<TreeNode>());
            directoryStack.push(node);
        }

        return directoryStack;
    }

    private static void createFileOperation(DistributedFileSystem hdfs) {
        System.out.print("File path:\n> ");
        String fileName = scanner.nextLine();
        System.out.print("File contents:\n> ");
        String fileContents = scanner.nextLine().trim();

        createFile(fileName, fileContents, hdfs);
    }

    /**
     * Create files using the names and contents provide by the two parameters.
     *
     * The two argument lists must have the same length.
     *
     * @param names File names.
     * @param content File contents.
     */
    private static void createFiles(String[] names, String[] content, DistributedFileSystem hdfs) {
        assert(names.length == content.length);

        for (int i = 0; i < names.length; i++) {
            LOG.info("Writing file " + i + "/" + names.length);
            createFile(names[i], content[i], hdfs);
        }
    }

    /**
     * Create a new file with the given name and contents.
     * @param name The name of the file.
     * @param contents The content to be written to the file.
     */
    private static void createFile(String name, String contents, DistributedFileSystem hdfs) {
        Path filePath = new Path(namenodeEndpoint + name);

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

    private static void renameOperation(DistributedFileSystem hdfs) {
        System.out.print("Original file path:\n> ");
        String originalFileName = scanner.nextLine();
        System.out.print("Renamed file path:\n> ");
        String renamedFileName = scanner.nextLine();

        Path filePath = new Path(namenodeEndpoint + originalFileName);
        Path filePathRename = new Path(namenodeEndpoint + renamedFileName);

        try {
            LOG.info("\t Original file path: \"" + originalFileName + "\"");
            LOG.info("\t New file path: \"" + renamedFileName + "\"");
            hdfs.rename(filePath, filePathRename);
            LOG.info("\t Finished rename operation.");
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    private static void listOperation(DistributedFileSystem hdfs) {
        System.out.print("Target directory:\n> ");
        String targetDirectory = scanner.nextLine();

        try {
            FileStatus[] fileStatus = hdfs.listStatus(new Path(namenodeEndpoint + targetDirectory));
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
    private static void mkdir(String path, DistributedFileSystem hdfs) {
        Path filePath = new Path(namenodeEndpoint + path);

        try {
            LOG.info("\t Attempting to create new directory: \"" + path + "\"");
            boolean directoryCreated = hdfs.mkdirs(filePath);
            LOG.info("\t Directory created successfully: " + directoryCreated);
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    private static void mkdirOperation(DistributedFileSystem hdfs) {
        System.out.print("New directory path:\n> ");
        String newDirectoryName = scanner.nextLine();

        mkdir(newDirectoryName, hdfs);
    }

    private static void appendOperation(DistributedFileSystem hdfs) {
        System.out.print("File path:\n> ");
        String fileName = scanner.nextLine();
        System.out.print("Content to append:\n> ");
        String fileContents = scanner.nextLine();

        Path filePath = new Path(namenodeEndpoint + fileName);

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

    private static int getIntFromUser(String prompt) {
        System.out.print(prompt + "\n> ");
        return Integer.parseInt(scanner.nextLine());
    }

    private static void prewarmOperation(DistributedFileSystem hdfs) {
        int threadsPerDeployment = getIntFromUser("Number of threads to use for each deployment?");
        int pingsPerThread = getIntFromUser("How many times should each thread pings its assigned deployment?");

        try {
            hdfs.prewarm(threadsPerDeployment, pingsPerThread);
        } catch (IOException ex) {
            LOG.info("Encountered IOException while pre-warming NNs.");
            ex.printStackTrace();
        }
    }

    private static void pingOperation(DistributedFileSystem hdfs) {
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

    private static void readOperation(DistributedFileSystem hdfs) {
        System.out.print("File path:\n> ");
        String fileName = scanner.nextLine();
        readFile(fileName, hdfs);
    }

    /**
     *
     *
     * Read the HopsFS/HDFS file at the given path.
     * @param fileName The path to the file to read.
     */
    private static void readFile(String fileName, DistributedFileSystem hdfs) {
        Path filePath = new Path(namenodeEndpoint + fileName);

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

    private static void deleteOperation(DistributedFileSystem hdfs) {
        System.out.print("File or directory path:\n> ");
        String targetPath = scanner.nextLine();

        Path filePath = new Path(namenodeEndpoint + targetPath);

        try {
            boolean success = hdfs.delete(filePath, true);
            LOG.info("\t Delete was successful: " + success);
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    private static int getNextOperation() {
        while (true) {
            try {
                String input = scanner.nextLine();
                return Integer.parseInt(input);
            } catch (NumberFormatException ex) {
                LOG.info("\t Invalid input! Please enter an integer.");
            } catch (Exception ex) {
                ex.printStackTrace();
                System.exit(1);
            }
        }
    }

    private static void printMenu() {
        System.out.println("");
        System.out.println("====== MENU ======");
        System.out.println("Debug Operations:");
        System.out.println("\n(-4) Clear statistics\n(-3) Output statistics packages to CSV\n" +
                "(-2) Output operations performed + write to file\n(-1) Print TCP debug information.");
        System.out.println("\nStandard Operations:");
        System.out.println("(0) Exit\n(1) Create file\n(2) Create directory\n(3) Read contents of file.\n(4) Rename" +
                "\n(5) Delete\n(6) List directory\n(7) Append\n(8) Create Subtree.\n(9) Ping\n(10) Prewarm" +
                "\n(11) Write Files to Directory\n(12) Read files\n(13) Delete files\n(14) Write Files to Directories" +
                "\n(15) Read n Files with n Threads (Weak Scaling)\n(16) Read n Files y Times with z Threads (Strong Scaling)");
        System.out.println("==================");
        System.out.println("");
        System.out.println("What would you like to do?");
        System.out.print("> ");
    }
}