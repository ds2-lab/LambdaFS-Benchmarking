package com.gmail.benrcarver;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.fs.FileStatus;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class InteractiveTest {
    public static final Log LOG = LogFactory.getLog(InteractiveTest.class);
    
    private static final String filesystemEndpoint = "hdfs://10.150.0.48:8020/";

    private static final Scanner scanner = new Scanner(System.in);
    //private static DistributedFileSystem hdfs;

    public static void main(String[] args) throws InterruptedException, IOException {
        LOG.debug("Starting HdfsTest now.");
        Configuration configuration = Utils.getConfiguration();
        try {
            configuration.addResource(new File("/home/ben/repos/hops/hadoop-dist/target/hadoop-3.2.0.2-RC0/etc/hadoop/hdfs-site.xml").toURI().toURL());
            configuration.addResource(new File("/home/ben/repos/hops/hadoop-dist/target/hadoop-3.2.0.2-RC0/etc/hadoop/core-site.xml").toURI().toURL());
        } catch (MalformedURLException ex) {
            ex.printStackTrace();
        }
        LOG.debug("Created configuration.");
        DistributedFileSystem hdfs = new DistributedFileSystem();
        LOG.debug("Created DistributedFileSystem object.");

        try {
            hdfs.initialize(new URI(filesystemEndpoint), configuration);
            LOG.debug("Called initialize() successfully.");
        } catch (URISyntaxException | IOException ex) {
            LOG.error("");
            LOG.error("");
            LOG.error("ERROR: Encountered exception while initializing DistributedFileSystem object.");
            ex.printStackTrace();
            System.exit(1);
        }

        while (true) {
            Thread.sleep(250);
            printMenu();
            int op = getNextOperation();

            switch(op) {
                case -4:
                    LOG.debug("Clearing statistics packages is not supported for Vanilla HopsFS.");
                    break;
                case -3:
                    LOG.debug("Writing statistics packages to files is not supported for Vanilla HopsFS.");
                    LOG.debug("");
                    break;
                case -2:
                    LOG.warn("Printing operations performed is not supported by Vanilla HopsFS.");
                    LOG.debug("");
                    break;
                case -1:
                    LOG.debug("Printing TCP debug information is not supported by Vanilla HopsFS.");
                    LOG.debug("");
                    break;
                case 0:
                    LOG.debug("Exiting now... goodbye!");
                    try {
                        hdfs.close();
                    } catch (IOException ex) {
                        LOG.debug("Encountered exception while closing file system...");
                        ex.printStackTrace();
                    }
                    System.exit(0);
                case 1:
                    LOG.debug("CREATE FILE selected!");
                    createFileOperation(hdfs);
                    break;
                case 2:
                    LOG.debug("MAKE DIRECTORY selected!");
                    mkdirOperation(hdfs);;
                    break;
                case 3:
                    LOG.debug("READ FILE selected!");
                    readOperation(hdfs);
                    break;
                case 4:
                    LOG.debug("RENAME selected!");
                    renameOperation(hdfs);
                    break;
                case 5:
                    LOG.debug("DELETE selected!");
                    deleteOperation(hdfs);
                    break;
                case 6:
                    LOG.debug("LIST selected!");
                    listOperation(hdfs);
                    break;
                case 7:
                    LOG.debug("APPEND selected!");
                    appendOperation(hdfs);
                    break;
                case 8:
                    LOG.debug("CREATE SUBTREE selected!");
                    createSubtree(hdfs);
                    break;
                case 9:
                    LOG.debug("PING is not supported for Vanilla HopsFS!");
                    break;
                case 10:
                    LOG.debug("PREWARM is not supported for Vanilla HopsFS!");
                    break;
                case 11:
                    LOG.debug("WRITE FILES TO DIRECTORY selected!");
                    writeFilesToDirectory(hdfs, configuration);
                    break;
                case 12:
                    LOG.debug("READ FILES selected!");
                    readFilesOperation(configuration, hdfs);
                    break;
                case 13:
                    LOG.debug("DELETE FILES selected!");
                    deleteFilesOperation(hdfs);
                    break;
                case 14:
                    LOG.debug("WRITE FILES TO DIRECTORIES selected!");
                    writeFilesToDirectories(hdfs, configuration);
                    break;
                case 15:
                    LOG.debug("'Read n Files with n Threads (Weak Scaling Read)' selected!");
                    readNFilesOperation(configuration);
                    break;
                case 16:
                    LOG.debug("'Read n Files y Times with z Threads (Strong Scaling Read)' selected!");
                    strongScalingReadBenchmark(configuration);
                    break;
                case 17:
                    LOG.debug("'Write n Files with n Threads (Weak Scaling Write)' selected!");
                    weakScalingWriteOperation(configuration, hdfs);
                    break;
                case 18:
                    LOG.debug("'Write n Files y Times with z Threads (Strong Scaling Write)' selected!");
                    strongScalingWriteOperation(configuration, hdfs);
                    break;
                case 19:
                    LOG.debug("CREATE DIRECTORIES selected!");
                    createDirectories(hdfs);
                    break;
                default:
                    LOG.debug("ERROR: Unknown or invalid operation specified: " + op);
                    break;
            }
        }
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

    private static void strongScalingReadBenchmark(final Configuration configuration)
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

        for (int i = 0; i < numThreads; i++) {
            Thread thread = new Thread(() -> {
                DistributedFileSystem hdfs = new DistributedFileSystem();

                try {
                    hdfs.initialize(new URI(filesystemEndpoint), configuration);
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

        LOG.info("Joining threads.");
        for (Thread thread : threads) {
            thread.join();
        }
        long end = System.currentTimeMillis();

        double durationSeconds = (end - start) / 1000.0;
        double totalReads = (double)n * (double)readsPerFile * (double)numThreads;
        double throughput = (totalReads / durationSeconds);
        LOG.info("Finished performing all " + totalReads + " file reads in " + durationSeconds + " seconds");
        LOG.info("Throughput: " + throughput + " ops/sec.");
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
                LOG.debug("1 directory specified.");
            else
                LOG.debug(directories.size() + " directories specified.");
        }
        else if (choice == 2) {
            System.out.print("Please provide path to file containing HopsFS directories:\n> ");
            String filePath = scanner.nextLine();
            directories = Utils.getFilePathsFromFile(filePath);

            if (directories.size() == 1)
                LOG.debug("1 directory specified in file.");
            else
                LOG.debug(directories.size() + " directories specified in file.");
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
            LOG.debug("Defaulting to " + n + ".");
        }

        int minLength = 5;
        System.out.print("Min string length (default " + minLength + "):\n> ");
        try {
            minLength = Integer.parseInt(scanner.nextLine());
        } catch (NumberFormatException ex) {
            LOG.debug("Defaulting to " + minLength + ".");
        }

        int maxLength = 10;
        System.out.print("Max string length (default " + maxLength + "):\n> ");
        try {
            maxLength = Integer.parseInt(scanner.nextLine());
        } catch (NumberFormatException ex) {
            LOG.debug("Defaulting to " + maxLength + ".");
        }

        assert directories != null;
        writeFilesInternal(n, minLength, maxLength, numberOfThreads, directories, hdfs, configuration);
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

    private static int getIntFromUser(String prompt) {
        System.out.print(prompt + "\n> ");
        return Integer.parseInt(scanner.nextLine());
    }

    public static void strongScalingWriteOperation(final Configuration configuration,
                                            final DistributedFileSystem sharedHdfs)
            throws InterruptedException, IOException {
        int totalNumberOfFiles = getIntFromUser("Total number of files to write?");
        int numberOfThreads = getIntFromUser("Number of threads to use?");

        if (totalNumberOfFiles < numberOfThreads) {
            LOG.error("The number of files to be written (" + totalNumberOfFiles +
                    ") should be less than the number of threads used to write said files (" + numberOfThreads + ").");
            return;
        }

        int writesPerThread = totalNumberOfFiles / numberOfThreads;
        int remainder = totalNumberOfFiles % numberOfThreads;

        if (remainder != 0) {
            LOG.error("Cannot cleanly divide " + totalNumberOfFiles + " writes among " + numberOfThreads + " threads.");
            return;
        }

        int directoryChoice = getIntFromUser(
                "Should threads all write to SAME DIRECTORY [1] or DIFFERENT DIRECTORIES [2]?");

        if (directoryChoice < 1 || directoryChoice > 2) {
            LOG.error("Invalid argument specified. Should be \"1\" for same directory or \"2\" for different directories. " +
                    "Instead, got \"" + directoryChoice + "\"");
            return;
        }

        int dirInputMethodChoice = getIntFromUser("Manually input (comma-separated list) [1], or specify file containing directories [2]?");

        List<String> directories = null;
        if (dirInputMethodChoice == 1) {
            System.out.print("Please enter the directories as a comma-separated list:\n> ");
            String listOfDirectories = scanner.nextLine();
            directories = Arrays.asList(listOfDirectories.split(","));

            if (directories.size() == 1)
                LOG.info("1 directory specified.");
            else
                LOG.info(directories.size() + " directories specified.");
        }
        else if (dirInputMethodChoice == 2) {
            System.out.print("Please provide path to file containing HopsFS directories:\n> ");
            String filePath = scanner.nextLine();
            directories = Utils.getFilePathsFromFile(filePath);

            if (directories.size() == 1)
                LOG.info("1 directory specified in file.");
            else
                LOG.info(directories.size() + " directories specified in file.");
        }
        else {
            LOG.error("Invalid option specified (" + dirInputMethodChoice +
                    "). Please enter \"1\" or \"2\" for this prompt.");
            return;
        }

        // IMPORTANT: Make directories the same size as the number of threads, so we have one directory per thread.
        //            This allows us to directly reuse the writeFilesInternal() function, which creates a certain
        //            number of files per directory. If number of threads is equal to number of directories, then
        //            we are essentially creating a certain number of files per thread, which is what we want.
        if (directoryChoice == 1) {
            Random rng = new Random();
            int idx = rng.nextInt(directories.size());
            String dir = directories.get(idx);
            directories = new ArrayList<>(numberOfThreads);
            for (int i = 0; i < numberOfThreads; i++)
                directories.add(dir); // This way, they'll all write to the same directory. We can reuse old code.
        } else {
            Collections.shuffle(directories);
            directories = directories.subList(0, numberOfThreads);
        }

        int minLength = 0;
        try {
            minLength = getIntFromUser("Min string length (default: " + minLength + ")?");
        } catch (NumberFormatException ex) {
            LOG.info("Defaulting to " + minLength + ".");
        }

        int maxLength = 0;
        try {
            maxLength = getIntFromUser("Max string length (default: " + maxLength + ")?");
        } catch (NumberFormatException ex) {
            LOG.info("Defaulting to " + maxLength + ".");
        }

        writeFilesInternal(writesPerThread, minLength, maxLength, numberOfThreads, directories, sharedHdfs, configuration);
    }

    /**
     * Weak scaling, writes.
     */
    public static void weakScalingWriteOperation(final Configuration configuration,
                                          final DistributedFileSystem sharedHdfs)
            throws IOException, InterruptedException {
        System.out.print("Should the threads write their files to the SAME DIRECTORY [1] or DIFFERENT DIRECTORIES [2]?\n> ");
        int directoryChoice = Integer.parseInt(scanner.nextLine());

        // Validate input.
        if (directoryChoice < 1 || directoryChoice > 2) {
            LOG.error("Invalid argument specified. Should be \"1\" for same directory or \"2\" for different directories. " +
                    "Instead, got \"" + directoryChoice + "\"");
            return;
        }

        System.out.print("Manually input (comma-separated list) [1], or specify file containing directories [2]? \n> ");
        int dirInputMethodChoice = Integer.parseInt(scanner.nextLine());

        List<String> directories = null;
        if (dirInputMethodChoice == 1) {
            System.out.print("Please enter the directories as a comma-separated list:\n> ");
            String listOfDirectories = scanner.nextLine();
            directories = Arrays.asList(listOfDirectories.split(","));

            if (directories.size() == 1)
                LOG.info("1 directory specified.");
            else
                LOG.info(directories.size() + " directories specified.");
        }
        else if (dirInputMethodChoice == 2) {
            System.out.print("Please provide path to file containing HopsFS directories:\n> ");
            String filePath = scanner.nextLine();
            directories = Utils.getFilePathsFromFile(filePath);

            if (directories.size() == 1)
                LOG.info("1 directory specified in file.");
            else
                LOG.info(directories.size() + " directories specified in file.");
        }
        else {
            LOG.error("Invalid option specified (" + dirInputMethodChoice +
                    "). Please enter \"1\" or \"2\" for this prompt.");
            return;
        }

        System.out.print("Number of threads? \n> ");
        int numberOfThreads = Integer.parseInt(scanner.nextLine());

        // IMPORTANT: Make directories the same size as the number of threads, so we have one directory per thread.
        //            This allows us to directly reuse the writeFilesInternal() function, which creates a certain
        //            number of files per directory. If number of threads is equal to number of directories, then
        //            we are essentially creating a certain number of files per thread, which is what we want.
        assert(directories != null);

        if (directoryChoice == 1) {
            Random rng = new Random();
            int idx = rng.nextInt(directories.size());
            String dir = directories.get(idx);
            directories = new ArrayList<>(numberOfThreads);
            for (int i = 0; i < numberOfThreads; i++)
                directories.add(dir); // This way, they'll all write to the same directory. We can reuse old code.
        } else {
            Collections.shuffle(directories);
            directories = directories.subList(0, numberOfThreads);
        }

        System.out.print("Number of writes per thread? \n> ");
        int writesPerThread = Integer.parseInt(scanner.nextLine());

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

        writeFilesInternal(writesPerThread, minLength, maxLength, numberOfThreads, directories, sharedHdfs, configuration);
    }

    /**
     * Delete the files listed in the file specified by the path argument.
     * @param localPath Text file containing HopsFS file paths to delete.
     */
    private static void deleteFiles(String localPath, DistributedFileSystem sharedHdfs) {
        List<String> paths = Utils.getFilePathsFromFile(localPath);

        List<Long> latencies = new ArrayList<>();
        long startTime = System.currentTimeMillis();
        int numSuccess = 0;
        for (String path : paths) {
            try {
                long start = System.currentTimeMillis();
                Path filePath = new Path(filesystemEndpoint + path);
                boolean success = sharedHdfs.delete(filePath, true);
                long end = System.currentTimeMillis();

                if (success) {
                    LOG.debug("\t Successfully deleted '" + path + "' in " + (end - start) + " ms.");
                    numSuccess++;
                }
                else
                    LOG.error("\t Failed to delete '" + path + "' after " + (end - start) + " ms.");

                latencies.add(end - start);
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
        long endTime = System.currentTimeMillis();
        double durationSeconds = (endTime - startTime) / 1000.0;
        LOG.info("Throughput: " + (numSuccess / durationSeconds) + " ops/sec.");
        LOG.info("Throughput (including failed ops): " + (paths.size() / durationSeconds) + " ops/sec.");
        LOG.info("Latencies (ms): ");
        for (Long latency : latencies)
            System.out.println(latency);
    }

    /**
     * Query the user for:
     *  - An integer `n`, the number of files to read
     *  - The path to a local file containing `n` or more HopsFS file paths.
     *  - The number of reads per file.
     *
     * This function will use `n` threads to read those `n` files.
     */
    private static void readNFilesOperation(final Configuration configuration)
            throws InterruptedException {
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

        Collections.shuffle(paths); // Randomize the order.

        System.out.print("Number of trials?\n> ");
        int numTrials = Integer.parseInt(scanner.nextLine());

        if (paths.size() < n) {
            LOG.error("ERROR: The file should contain at least " + n +
                    " HopsFS file path(s); however, it contains just " + paths.size() + " HopsFS file path(s).");
            LOG.error("Aborting operation.");
            return;
        }

        List<Double> throughputResults = new ArrayList<>();
        for (int trial = 0; trial < numTrials; trial++) {
            System.out.println("|==== TRIAL #" + trial + " ====|");
            Thread[] threads = new Thread[n];

            // Used to synchronize threads; they each connect to HopsFS and then
            // count down. So, they all cannot start until they are all connected.
            final CountDownLatch latch = new CountDownLatch(n);

            for (int i = 0; i < n; i++) {
                final String filePath = paths.get(i);
                Thread thread = new Thread(() -> {
                    DistributedFileSystem hdfs = new DistributedFileSystem();

                    try {
                        hdfs.initialize(new URI(filesystemEndpoint), configuration);
                    } catch (URISyntaxException | IOException ex) {
                        LOG.error("ERROR: Encountered exception while initializing DistributedFileSystem object.");
                        ex.printStackTrace();
                        System.exit(1);
                    }

                    latch.countDown();

                    for (int j = 0; j < readsPerFile; j++)
                        readFile(filePath, hdfs);

                    try {
                        hdfs.close();
                    } catch (IOException ex) {
                        LOG.error("Encountered IOException while closing DistributedFileSystem object:", ex);
                    }
                });
                threads[i] = thread;
            }

            LOG.debug("Starting threads.");
            long start = System.currentTimeMillis();
            for (Thread thread : threads) {
                thread.start();
            }

            LOG.debug("Joining threads.");
            for (Thread thread : threads) {
                thread.join();
            }
            long end = System.currentTimeMillis();

            double durationSeconds = (end - start) / 1000.0;
            double totalReads = (double) n * (double) readsPerFile;
            double throughput = (totalReads / durationSeconds);
            LOG.info("Finished performing all " + totalReads + " file reads in " + durationSeconds);
            LOG.info("Throughput: " + throughput + " ops/sec.");

            throughputResults.add(throughput);

            if (trial < numTrials - 1) // Do not sleep for last trial.
                Thread.sleep(500);
        }

        System.out.println("Throughput results:");
        for (double throughput : throughputResults) {
            System.out.println(throughput);
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
        List<String> paths = Utils.getFilePathsFromFile(path);
        int n = paths.size();

        int filesPerArray = (int)Math.floor((double)n/numThreads);
        int remainder = n % numThreads;

        if (remainder != 0) {
            LOG.debug("Assigning all but last thread " + filesPerArray +
                    " files. The last thread will be assigned " + remainder + " files.");
        } else {
            LOG.debug("Assigning each thread " + filesPerArray + " files.");
        }

        String[][] pathsPerThread = Utils.splitArray(paths.toArray(new String[0]), filesPerArray);

        assert(pathsPerThread != null);
        LOG.debug("pathsPerThread.length: " + pathsPerThread.length);

        Thread[] threads = new Thread[numThreads];

        // Used to synchronize threads; they each connect to HopsFS and then
        // count down. So, they all cannot start until they are all connected.
        final CountDownLatch latch = new CountDownLatch(numThreads);

        for (int i = 0; i < numThreads; i++) {
            final String[] pathsForThread = pathsPerThread[i];
            Thread thread = new Thread(() -> {
                DistributedFileSystem hdfs = new DistributedFileSystem();

                try {
                    hdfs.initialize(new URI(filesystemEndpoint), configuration);
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

                try {
                    hdfs.close();
                } catch (IOException ex) {
                    LOG.error("Encountered IOException while closing DistributedFileSystem object:", ex);
                }
            });
            threads[i] = thread;
        }

        LOG.debug("Starting threads.");
        long start = System.currentTimeMillis();
        for (Thread thread : threads) {
            thread.start();
        }

        LOG.debug("Joining threads.");
        for (Thread thread : threads) {
            thread.join();
        }
        long end = System.currentTimeMillis();

        double durationSeconds = (end - start) / 1000.0;
        LOG.debug("Finished performing all " + (readsPerFile * paths.size()) + " file reads in " +
                durationSeconds + " seconds.");
        double totalReads = (double)n * (double)readsPerFile;
        double throughput = (totalReads / durationSeconds);
        LOG.debug("Throughput: " + throughput + " ops/sec.");
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
        LOG.debug("Generating " + n + " files for each directory (total of " + totalNumberOfFiles + " files.");
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

        LOG.debug("Generated a total of " + totalNumberOfFiles + " file(s).");

        Utils.write("./output/writeToDirectoryPaths-" + Instant.now().toEpochMilli()+ ".txt", targetPaths);

        long start, end;
        if (numThreads == 1) {
            start = System.currentTimeMillis();

            createFiles(targetPaths, content, sharedHdfs);

        } else {
            int filesPerArray = (int)Math.floor((double)totalNumberOfFiles / numThreads);
            int remainder = totalNumberOfFiles % numThreads;

            if (remainder != 0) {
                LOG.debug("Assigning all but last thread " + filesPerArray +
                        " files. The last thread will be assigned " + remainder + " files.");
            } else {
                LOG.debug("Assigning each thread " + filesPerArray + " files.");
            }

            final String[][] contentPerArray = Utils.splitArray(content, filesPerArray);
            final String[][] targetPathsPerArray = Utils.splitArray(targetPaths, filesPerArray);

            assert targetPathsPerArray != null;
            assert contentPerArray != null;

            final CountDownLatch latch = new CountDownLatch(numThreads);

            Thread[] threads = new Thread[numThreads];

            for (int i = 0; i < numThreads; i++) {
                final int idx = i;
                Thread thread = new Thread(() -> {
                    DistributedFileSystem hdfs = new DistributedFileSystem();

                    try {
                        hdfs.initialize(new URI(filesystemEndpoint), configuration);
                    } catch (URISyntaxException | IOException ex) {
                        LOG.error("ERROR: Encountered exception while initializing DistributedFileSystem object.");
                        ex.printStackTrace();
                        System.exit(1);
                    }

                    latch.countDown();
                    createFiles(targetPathsPerArray[idx], contentPerArray[idx], hdfs);

                    try {
                        hdfs.close();
                    } catch (IOException ex) {
                        LOG.error("Encountered IOException while closing DistributedFileSystem object:", ex);
                    }
                });
                threads[i] = thread;
            }

            LOG.debug("Starting threads.");
            start = System.currentTimeMillis();
            for (Thread thread : threads) {
                thread.start();
            }

            LOG.debug("Joining threads.");
            for (Thread thread : threads) {
                thread.join();
            }
        }
        end = System.currentTimeMillis();

        double duration = (end - start) / 1000.0;
        filesPerSec = totalNumberOfFiles / duration;
        LOG.debug("");
        LOG.debug("");
        LOG.debug("===============================");
        LOG.debug("Time elapsed: " + duration + " seconds");
        LOG.debug("Aggregate throughput: " + filesPerSec + " ops/sec.");
    }

    private static void createSubtree(DistributedFileSystem hdfs) {
        System.out.print("Subtree root directory:\n> ");
        String subtreeRootPath = scanner.nextLine();

        System.out.print("Subtree depth:\n> ");
        int subtreeDepth = Integer.parseInt(scanner.nextLine());

        System.out.print("Max subdirs:\n> ");
        int maxSubDirs = Integer.parseInt(scanner.nextLine());

//        System.out.print("Files per directory:\n> ");
//        int filesPerDirectory = Integer.parseInt(scanner.nextLine());
//
//        System.out.print("File contents:\n> ");
//        String fileContents = scanner.nextLine();

        int height = subtreeDepth + 1;
        double totalPossibleDirectories = (Math.pow(maxSubDirs, height + 1) - 1) / (maxSubDirs - 1);
        LOG.debug("\nThis could create a maximum of " + totalPossibleDirectories + " directories.");
        System.out.print("Is this okay? [y/N]\n >");

        String resp = scanner.nextLine();

        // If they answered anything other than 'y', then abort.
        if (!resp.toLowerCase().equals("y")) {
            LOG.debug("\nAborting.");
            return;
        }

        Random rng = new Random();

        int directoriesCreated = 0;
        int filesCreated = 0;

        long start = System.currentTimeMillis();

        int currentDepth = 0;

        mkdir(subtreeRootPath, hdfs);
        directoriesCreated++;

        Stack<TreeNode> directoryStack = new Stack<TreeNode>();
        TreeNode subtreeRoot = new TreeNode(subtreeRootPath, new ArrayList<TreeNode>());
        directoryStack.push(subtreeRoot);

        while (currentDepth <= subtreeDepth) {
            LOG.debug("=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=");
            LOG.debug("CURRENT DEPTH: " + currentDepth);
            LOG.debug("DIRECTORIES CREATED: " + directoriesCreated);
            LOG.debug("=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=");
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

        long end = System.currentTimeMillis();
        double subtreeCreationDuration = (end - start) / 1000.0;

        LOG.debug("=== Subtree Creation Completed ===");
        LOG.debug("Time elapsed: " + subtreeCreationDuration + " seconds");
        LOG.debug("Directories created: " + directoriesCreated);
        LOG.debug("Files created: " + filesCreated + "\n");

        LOG.debug("subtreeRoot children: " + subtreeRoot.children.size());
        LOG.debug(subtreeRoot.toString());

        LOG.debug("==================================");
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
            LOG.debug("Writing file " + i + "/" + names.length);
            createFile(names[i], content[i], hdfs);
        }
    }

    /**
     * Create a new file with the given name and contents.
     * @param name The name of the file.
     * @param contents The content to be written to the file.
     */
    private static void createFile(String name, String contents, DistributedFileSystem hdfs) {
        Path filePath = new Path(filesystemEndpoint + name);

        try {
            FSDataOutputStream outputStream = hdfs.create(filePath);

            if (!contents.isEmpty()) {
                BufferedWriter br = new BufferedWriter(new OutputStreamWriter(outputStream, "UTF-8"));
                br.write(contents);
                br.close();
                LOG.debug("\t Successfully created non-empty file '" + filePath + "'");
            } else {
                LOG.debug("\t Successfully created empty file '" + filePath + "'");
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

        Path filePath = new Path(filesystemEndpoint + originalFileName);
        Path filePathRename = new Path(filesystemEndpoint + renamedFileName);

        try {
            LOG.debug("\t Original file path: \"" + originalFileName + "\"");
            LOG.debug("\t New file path: \"" + renamedFileName + "\"");
            hdfs.rename(filePath, filePathRename);
            LOG.debug("\t Finished rename operation.");
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    private static void listOperation(DistributedFileSystem hdfs) {
        System.out.print("Target directory:\n> ");
        String targetDirectory = scanner.nextLine();

        try {
            FileStatus[] fileStatus = hdfs.listStatus(new Path(filesystemEndpoint + targetDirectory));
            LOG.debug("Directory '" + targetDirectory + "' contains " + fileStatus.length + " files.");
            for(FileStatus status : fileStatus)
                LOG.debug(status.getPath().toString());
            LOG.debug("Directory '" + targetDirectory + "' contains " + fileStatus.length + " files.");
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    /**
     * Create a new directory with the given path.
     * @param path The path of the new directory.
     */
    private static void mkdir(String path, DistributedFileSystem hdfs) {
        Path filePath = new Path(filesystemEndpoint + path);

        try {
            LOG.debug("\t Attempting to create new directory: \"" + path + "\"");
            boolean directoryCreated = hdfs.mkdirs(filePath);
            LOG.debug("\t Directory created successfully: " + directoryCreated);
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

        Path filePath = new Path(filesystemEndpoint + fileName);

        try {
            FSDataOutputStream outputStream = hdfs.append(filePath);
            LOG.debug("\t Called append() successfully.");
            BufferedWriter br = new BufferedWriter(new OutputStreamWriter(outputStream, "UTF-8"));
            LOG.debug("\t Created BufferedWriter object.");
            br.write(fileContents);
            LOG.debug("\t Appended \"" + fileContents + "\" to file using BufferedWriter.");
            br.close();
            LOG.debug("\t Closed BufferedWriter.");
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    private static void readOperation(DistributedFileSystem hdfs) {
        System.out.print("File path:\n> ");
        String fileName = scanner.nextLine();
        readFile(fileName, hdfs);
    }

    /**
     * Read the HopsFS/HDFS file at the given path.
     * @param fileName The path to the file to read.
     */
    private static void readFile(String fileName, DistributedFileSystem hdfs) {
        Path filePath = new Path(filesystemEndpoint + fileName);

        try {
            FSDataInputStream inputStream = hdfs.open(filePath);
            BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
            String line = null;
            long readStart = System.currentTimeMillis();

            LOG.debug("");
            LOG.debug("CONTENTS OF FILE '" + fileName + "': ");
            while ((line = br.readLine()) != null)
                LOG.debug(line);
            LOG.debug("");
            long readEnd = System.currentTimeMillis();
            inputStream.close();
            br.close();
            long readDuration = readEnd - readStart;

            LOG.debug("Read contents of file \"" + fileName + "\" from DataNode in " + readDuration + " milliseconds.");
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    private static void deleteOperation(DistributedFileSystem hdfs) {
        System.out.print("File or directory path:\n> ");
        String targetPath = scanner.nextLine();

        Path filePath = new Path(filesystemEndpoint + targetPath);

        try {
            long s = System.currentTimeMillis();
            boolean success = hdfs.delete(filePath, true);
            long t = System.currentTimeMillis();

            if (success)
                LOG.info("Successfully deleted '" + targetPath + "' in " + (t - s) + " milliseconds.");
            else
                LOG.error("Failed to delete '" + targetPath + "' after " + (t - s) + " milliseconds.");
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
                LOG.debug("\t Invalid input! Please enter an integer.");
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
        System.out.println("(-4) Clear statistics [NOT SUPPORTED]\n(-3) Output statistics packages to CSV [NOT SUPPORTED]\n" +
                "(-2) Output operations performed + write to file [NOT SUPPORTED]\n(-1) Print TCP debug information. [NOT SUPPORTED]");
        System.out.println("\nStandard Operations:");
        System.out.println("(0) Exit\n(1) Create file\n(2) Create directory\n(3) Read contents of file.\n(4) Rename" +
                "\n(5) Delete\n(6) List directory\n(7) Append\n(8) Create Subtree.\n(9) Ping [NOT SUPPORTED]\n(10) Prewarm [NOT SUPPORTED]" +
                "\n(11) Write Files to Directory\n(12) Read files\n(13) Delete files\n(14) Write Files to Directories" +
                "\n(15) Read n Files with n Threads (Weak Scaling Read)\n(16) Read n Files y Times with z Threads (Strong Scaling Read)" +
                "\n(17) Write n Files with n Threads (Weak Scaling - Write)\n(18) Write n Files y Times with z Threads (Strong Scaling - Write)" +
                "\n(19) Create directories.");
        System.out.println("==================");
        System.out.println("");
        System.out.println("What would you like to do?");
        System.out.print("> ");
    }
}
