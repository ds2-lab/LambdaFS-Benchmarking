package com.gmail.benrcarver;

import com.google.gson.JsonObject;
import org.apache.commons.cli.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.fs.FileStatus;

import javax.swing.*;
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

public class InteractiveTest {
    public static final Log LOG = LogFactory.getLog(InteractiveTest.class);

    private static final Scanner scanner = new Scanner(System.in);
    //private static DistributedFileSystem hdfs;

    public static void main(String[] args) throws InterruptedException, IOException {
        LOG.debug("Starting HdfsTest now.");
        Configuration configuration = Utils.getConfiguration();
        try {
            configuration.addResource(new File("/home/ubuntu/repos/hops/hadoop-dist/target/hadoop-3.2.0.3-SNAPSHOT/etc/hadoop/hdfs-site.xml").toURI().toURL());
        } catch (MalformedURLException ex) {
            ex.printStackTrace();
        }
        LOG.debug("Created configuration.");
        DistributedFileSystem hdfs = new DistributedFileSystem();
        LOG.debug("Created DistributedFileSystem object.");

        try {
            hdfs.initialize(new URI("hdfs://10.241.64.14:9000"), configuration);
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
                    LOG.debug("Clearing statistics packages...");
                    clearStatisticsPackages(hdfs);
                    break;
                case -3:
                    LOG.debug("Writing statistics packages to files...");
                    LOG.debug("");
                    hdfs.dumpStatisticsPackages(true);
                    break;
                case -2:
                    LOG.warn("Printing operations performed not supported by Vanilla HopsFS...");
                    LOG.debug("");
                    break;
                case -1:
                    LOG.debug("Printing TCP debug information...");
                    LOG.debug("");
                    hdfs.printDebugInformation();
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
                    LOG.debug("PING selected!");
                    pingOperation(hdfs);
                    break;
                case 10:
                    LOG.debug("PREWARM selected!");
                    prewarmOperation(hdfs);
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

                default:
                    LOG.debug("ERROR: Unknown or invalid operation specified: " + op);
                    break;
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

    private static void clearStatisticsPackages(DistributedFileSystem hdfs) {
        System.out.print("Are you sure? (y/N)\n> ");
        String input = scanner.nextLine();

        if (input.equalsIgnoreCase("y")) {
            hdfs.clearStatistics(true, true, true);
        } else {
            LOG.debug("NOT clearing statistics packages.");
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
        List<String> paths = Utils.getFilePathsFromFile(localPath);

        for (String path : paths) {
            try {
                Path filePath = new Path("hdfs://10.241.64.14:9000/" + path);
                boolean success = sharedHdfs.delete(filePath, true);
                LOG.debug("\t Delete was successful: " + success);
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

        assert pathsPerThread != null;
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
                    hdfs.initialize(new URI("hdfs://10.241.64.14:9000"), configuration);
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
        Instant start = Instant.now();
        for (Thread thread : threads) {
            thread.start();
        }

        LOG.debug("Joining threads.");
        for (Thread thread : threads) {
            thread.join();
        }
        Instant end = Instant.now();
        Duration duration = Duration.between(start, end);

        double durationSeconds = duration.getSeconds() + (duration.getNano() / 1e9);
        LOG.debug("Finished performing all " + (readsPerFile * paths.size()) + " file reads in " + duration);
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
                        hdfs.initialize(new URI("hdfs://10.241.64.14:9000"), configuration);
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
            start = Instant.now();
            for (Thread thread : threads) {
                thread.start();
            }

            LOG.debug("Joining threads.");
            for (Thread thread : threads) {
                thread.join();
            }
            end = Instant.now();
        }

        Duration duration = Duration.between(start, end);
        float durationSeconds = duration.getSeconds() + TimeUnit.NANOSECONDS.toSeconds(duration.getNano());
        filesPerSec = totalNumberOfFiles / durationSeconds;
        LOG.debug("");
        LOG.debug("");
        LOG.debug("===============================");
        LOG.debug("Time elapsed: " + duration.toString());
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

        Instant start = Instant.now();

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

        Instant end = Instant.now();
        Duration subtreeCreationDuration = Duration.between(start, end);

        LOG.debug("=== Subtree Creation Completed ===");
        LOG.debug("Time elapsed: " + subtreeCreationDuration.toString());
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
        Path filePath = new Path("hdfs://10.241.64.14:9000/" + name);

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

        Path filePath = new Path("hdfs://10.241.64.14:9000/" + originalFileName);
        Path filePathRename = new Path("hdfs://10.241.64.14:9000/" + renamedFileName);

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
            FileStatus[] fileStatus = hdfs.listStatus(new Path("hdfs://10.241.64.14:9000/" + targetDirectory));
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
        Path filePath = new Path("hdfs://10.241.64.14:9000/" + path);

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

        Path filePath = new Path("hdfs://10.241.64.14:9000/" + fileName);

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

    private static void prewarmOperation(DistributedFileSystem hdfs) {
        System.out.print("Invocations per deployment:\n> ");
        int pingsPerDeployment = Integer.parseInt(scanner.nextLine());

        try {
            hdfs.prewarm(pingsPerDeployment);
        } catch (IOException ex) {
            LOG.debug("Encountered IOException while pre-warming NNs.");
            ex.printStackTrace();
        }
    }

    private static void pingOperation(DistributedFileSystem hdfs) {
        System.out.print("Target deployment:\n> ");
        int targetDeployment = Integer.parseInt(scanner.nextLine());

        try {
            hdfs.ping(targetDeployment);
        } catch (IOException ex) {
            LOG.debug("Encountered IOException while pinging NameNode deployment " +
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
     * Read the HopsFS/HDFS file at the given path.
     * @param fileName The path to the file to read.
     */
    private static void readFile(String fileName, DistributedFileSystem hdfs) {
        Path filePath = new Path("hdfs://10.241.64.14:9000/" + fileName);

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

        Path filePath = new Path("hdfs://10.241.64.14:9000/" + targetPath);

        try {
            boolean success = hdfs.delete(filePath, true);
            LOG.debug("\t Delete was successful: " + success);
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
        System.out.println("(-4) Clear statistics\n(-3) Output statistics packages to CSV\n" +
                "(-2) Output operations performed + write to file\n(-1) Print TCP debug information.");
        System.out.println("\nStandard Operations:");
        System.out.println("(0) Exit\n(1) Create file\n(2) Create directory\n(3) Read contents of file.\n(4) Rename" +
                "\n(5) Delete\n(6) List directory\n(7) Append\n(8) Create Subtree.\n(9) Ping\n(10) Prewarm" +
                "\n(11) Write Files to Directory\n(12) Read files\n(13) Delete files\n(14) Write Files to Directories");
        System.out.println("==================");
        System.out.println("");
        System.out.println("What would you like to do?");
        System.out.print("> ");
    }
}
