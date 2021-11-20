package com.gmail.benrcarver;

import com.google.gson.JsonObject;
import org.apache.commons.cli.*;
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
import java.nio.file.Files;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class InteractiveTest {
    private static final Scanner scanner = new Scanner(System.in);
    private static DistributedFileSystem hdfs;

    public static void main(String[] args) throws InterruptedException, IOException {
        System.out.println("Starting HdfsTest now.");
        Configuration configuration = Utils.getConfiguration();
        try {
            configuration.addResource(new File("/home/ubuntu/repos/hops/hadoop-dist/target/hadoop-3.2.0.3-SNAPSHOT/etc/hadoop/hdfs-site.xml").toURI().toURL());
        } catch (MalformedURLException ex) {
            ex.printStackTrace();
        }
        System.out.println("Created configuration.");
        hdfs = new DistributedFileSystem();
        System.out.println("Created DistributedFileSystem object.");

        try {
            hdfs.initialize(new URI("hdfs://10.241.64.14:9000"), configuration);
            System.out.println("Called initialize() successfully.");
        } catch (URISyntaxException | IOException ex) {
            System.out.println("\n\nERROR: Encountered exception while initializing DistributedFileSystem object.");
            ex.printStackTrace();
            System.exit(1);
        }

        while (true) {
            Thread.sleep(250);
            printMenu();
            int op = getNextOperation();

            switch(op) {
                case -2:
                    hdfs.printOperationsPerformed();
                    break;
                case -1:
                    hdfs.printDebugInformation();
                    break;
                case 0:
                    System.out.println("Exiting now... goodbye!");
                    try {
                        hdfs.close();
                    } catch (IOException ex) {
                        System.out.println("Encountered exception while closing file system...");
                        ex.printStackTrace();
                    }
                    System.exit(0);
                case 1:
                    System.out.println("CREATE FILE selected!");
                    createFileOperation();
                    break;
                case 2:
                    System.out.println("MAKE DIRECTORY selected!");
                    mkdirOperation();;
                    break;
                case 3:
                    System.out.println("READ FILE selected!");
                    readOperation();
                    break;
                case 4:
                    System.out.println("RENAME selected!");
                    renameOperation();
                    break;
                case 5:
                    System.out.println("DELETE selected!");
                    deleteOperation();
                    break;
                case 6:
                    System.out.println("LIST selected!");
                    listOperation();
                    break;
                case 7:
                    System.out.println("APPEND selected!");
                    appendOperation();
                    break;
                case 8:
                    System.out.println("CREATE SUBTREE selected!");
                    createSubtree();
                    break;
                case 9:
                    System.out.println("PING selected!");
                    pingOperation();
                    break;
                case 10:
                    System.out.println("PREWARM selected!");
                    prewarmOperation();
                    break;
                case 11:
                    System.out.println("WRITE FILES TO DIRECTORY selected!");
                    writeFilesToDirectory();
                    break;
                case 12:
                    System.out.println("READ FILES selected!");
                    readFilesOperation();
                    break;
                default:
                    System.out.println("ERROR: Unknown or invalid operation specified: " + op);
                    break;
            }
        }
    }

    private static void readFilesOperation() throws InterruptedException {
        System.out.print("Path to local file containing HopsFS/HDFS paths:\n> ");
        String localFilePath = scanner.nextLine();

        System.out.print("Reads per file:\n> ");
        int readsPerFile = Integer.parseInt(scanner.nextLine());

        System.out.print("Number of threads:\n> ");
        int numThreads = Integer.parseInt(scanner.nextLine());

        readFiles(localFilePath, readsPerFile, numThreads);
    }

    /**
     * Specify a file on the local filesystem containing a bunch of HopsFS file paths. Read the local file in order
     * to get all the HopsFS file paths, then read those files a configurable number of times.
     * @param path Path to file containing a bunch of HopsFS files.
     * @param readsPerFile Number of times each file should be read.
     * @param numThreads Number of threads to use when performing the reads concurrently.
     */
    private static void readFiles(String path, int readsPerFile, int numThreads) throws InterruptedException {
        List<String> paths = Utils.getFilePathsFromFile(path);
        int n = paths.size();

        int filesPerArray = (int)Math.ceil((double)n/numThreads);

        System.out.println("Assigning each thread " + filesPerArray + " files (plus remainder for last thread.");

        String[][] pathsPerThread = Utils.splitArray(paths.toArray(new String[0]), filesPerArray);

        Thread[] threads = new Thread[numThreads];

        for (int i = 0; i < numThreads; i++) {
            final int idx = i;
            final String[] pathsForThread = pathsPerThread[i];
            Thread thread = new Thread(() -> {
                for (String filePath : pathsForThread) {
                    for (int j = 0; j < readsPerFile; j++)
                        readFile(filePath);
                }
            });
            threads[i] = thread;
        }

        System.out.println("Starting threads.");
        Instant start = Instant.now();
        for (Thread thread : threads) {
            thread.start();
        }

        System.out.println("Joining threads.");
        for (Thread thread : threads) {
            thread.join();
        }
        Instant end = Instant.now();

        System.out.println("Finished performing all " + (readsPerFile * paths.size()) + " file reads in " +
                Duration.between(start, end).toString());
    }

    /**
     * Write a bunch of files to a target directory.
     */
    private static void writeFilesToDirectory() throws InterruptedException, IOException {
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

        // Generate the file contents and file names.
        final String[] content = Utils.getVariableLengthRandomStrings(n, minLength, maxLength);
        final String[] targetPaths = Utils.getFixedLengthRandomStrings(n, 15);
        for (int i = 0; i < targetPaths.length; i++) {
            targetPaths[i] = targetDirectory + "/" + targetPaths[i];
        }

        Utils.write("./output/writeToDirectoryPaths-" + Instant.now().toEpochMilli()+ ".txt", targetPaths);

        Instant start;
        Instant end;
        if (numThreads == 1) {
            start = Instant.now();

            createFiles(targetPaths, content);

            end = Instant.now();
        } else {
            int filesPerArray = (int)Math.ceil((double)n/numThreads);

            System.out.println("Assigning each thread " + filesPerArray + " files (plus remainder for last thread.");

            final String[][] contentPerArray = Utils.splitArray(content, filesPerArray);
            final String[][] targetPathsPerArray = Utils.splitArray(targetPaths, filesPerArray);

            assert targetPathsPerArray != null;
            assert contentPerArray != null;

            Thread[] threads = new Thread[numThreads];

            for (int i = 0; i < numThreads; i++) {
                final int idx = i;
                Thread thread = new Thread(() -> createFiles(targetPathsPerArray[idx], contentPerArray[idx]));
                threads[i] = thread;
            }

            System.out.println("Starting threads.");
            start = Instant.now();
            for (Thread thread : threads) {
                thread.start();
            }

            System.out.println("Joining threads.");
            for (Thread thread : threads) {
                thread.join();
            }
            end = Instant.now();
        }

        Duration duration = Duration.between(start, end);
        System.out.println("\n\n===============================");
        System.out.println("Time elapsed: " + duration.toString());
    }

    private static void createSubtree() {
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
        System.out.println("\nThis could create a maximum of " + totalPossibleDirectories + " directories.");
        System.out.print("Is this okay? [y/N]\n >");

        String resp = scanner.nextLine();

        // If they answered anything other than 'y', then abort.
        if (!resp.toLowerCase().equals("y")) {
            System.out.println("\nAborting.");
            return;
        }

        Random rng = new Random();

        int directoriesCreated = 0;
        int filesCreated = 0;

        Instant start = Instant.now();

        int currentDepth = 0;

        mkdir(subtreeRootPath);
        directoriesCreated++;

        Stack<TreeNode> directoryStack = new Stack<TreeNode>();
        TreeNode subtreeRoot = new TreeNode(subtreeRootPath, new ArrayList<TreeNode>());
        directoryStack.push(subtreeRoot);

        while (currentDepth <= subtreeDepth) {
            System.out.println("=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=");
            System.out.println("CURRENT DEPTH: " + currentDepth);
            System.out.println("DIRECTORIES CREATED: " + directoriesCreated);
            System.out.println("=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=");
            List<Stack<TreeNode>> currentDepthStacks = new ArrayList<>();
            while (!directoryStack.empty()) {
                TreeNode directory = directoryStack.pop();

                String basePath = directory.getPath() + "/dir";

                Stack<TreeNode> stack = createChildDirectories(basePath, maxSubDirs);
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

        System.out.println("=== Subtree Creation Completed ===");
        System.out.println("Time elapsed: " + subtreeCreationDuration.toString());
        System.out.println("Directories created: " + directoriesCreated);
        System.out.println("Files created: " + filesCreated + "\n");

        System.out.println("subtreeRoot children: " + subtreeRoot.children.size());
        System.out.println(subtreeRoot.toString());

        System.out.println("==================================");
    }

    private static Stack<TreeNode> createChildDirectories(String basePath, int subDirs) {
        Stack<TreeNode> directoryStack = new Stack<TreeNode>();
        for (int i = 0; i < subDirs; i++) {
            String path = basePath + i;
            mkdir(path);
            TreeNode node = new TreeNode(path, new ArrayList<TreeNode>());
            directoryStack.push(node);
        }

        return directoryStack;
    }

    private static void createFileOperation() {
        System.out.print("File path:\n> ");
        String fileName = scanner.nextLine();
        System.out.print("File contents:\n> ");
        String fileContents = scanner.nextLine();

        createFile(fileName, fileContents);
    }

    /**
     * Create files using the names and contents provide by the two parameters.
     *
     * The two argument lists must have the same length.
     *
     * @param names File names.
     * @param content File contents.
     */
    private static void createFiles(String[] names, String[] content) {
        assert(names.length == content.length);

        for (int i = 0; i < names.length; i++) {
            System.out.println("Writing file " + i + "/" + names.length);
            createFile(names[i], content[i]);
        }
    }

    /**
     * Create a new file with the given name and contents.
     * @param name The name of the file.
     * @param contents The content to be written to the file.
     */
    private static void createFile(String name, String contents) {
        Path filePath = new Path("hdfs://10.241.64.14:9000/" + name);

        try {
            FSDataOutputStream outputStream = hdfs.create(filePath);
            System.out.println("\t Called create() successfully.");
            BufferedWriter br = new BufferedWriter(new OutputStreamWriter(outputStream, "UTF-8"));
            System.out.println("\t Created BufferedWriter object.");
            br.write(contents);
            System.out.println("\t Wrote \"" + contents + "\" using BufferedWriter.");
            br.close();
            System.out.println("\t Closed BufferedWriter.");
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    private static void renameOperation() {
        System.out.print("Original file path:\n> ");
        String originalFileName = scanner.nextLine();
        System.out.print("Renamed file path:\n> ");
        String renamedFileName = scanner.nextLine();

        Path filePath = new Path("hdfs://10.241.64.14:9000/" + originalFileName);
        Path filePathRename = new Path("hdfs://10.241.64.14:9000/" + renamedFileName);

        try {
            System.out.println("\t Original file path: \"" + originalFileName + "\"");
            System.out.println("\t New file path: \"" + renamedFileName + "\"");
            hdfs.rename(filePath, filePathRename);
            System.out.println("\t Finished rename operation.");
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    private static void listOperation() {
        System.out.print("Target directory:\n> ");
        String targetDirectory = scanner.nextLine();

        try {
            FileStatus[] fileStatus = hdfs.listStatus(new Path("hdfs://10.241.64.14:9000/" + targetDirectory));
            for(FileStatus status : fileStatus){
                System.out.println(status.getPath().toString());
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    /**
     * Create a new directory with the given path.
     * @param path The path of the new directory.
     */
    private static void mkdir(String path) {
        Path filePath = new Path("hdfs://10.241.64.14:9000/" + path);

        try {
            System.out.println("\t Attempting to create new directory: \"" + path + "\"");
            boolean directoryCreated = hdfs.mkdirs(filePath);
            System.out.println("\t Directory created successfully: " + directoryCreated);
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    private static void mkdirOperation() {
        System.out.print("New directory path:\n> ");
        String newDirectoryName = scanner.nextLine();

        mkdir(newDirectoryName);
    }

    private static void appendOperation() {
        System.out.print("File path:\n> ");
        String fileName = scanner.nextLine();
        System.out.print("Content to append:\n> ");
        String fileContents = scanner.nextLine();

        Path filePath = new Path("hdfs://10.241.64.14:9000/" + fileName);

        try {
            FSDataOutputStream outputStream = hdfs.append(filePath);
            System.out.println("\t Called append() successfully.");
            BufferedWriter br = new BufferedWriter(new OutputStreamWriter(outputStream, "UTF-8"));
            System.out.println("\t Created BufferedWriter object.");
            br.write(fileContents);
            System.out.println("\t Appended \"" + fileContents + "\" to file using BufferedWriter.");
            br.close();
            System.out.println("\t Closed BufferedWriter.");
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    private static void prewarmOperation() {
        System.out.print("Invocations per deployment:\n> ");
        int pingsPerDeployment = Integer.parseInt(scanner.nextLine());

        try {
            hdfs.prewarm(pingsPerDeployment);
        } catch (IOException ex) {
            System.out.println("Encountered IOException while pre-warming NNs.");
            ex.printStackTrace();
        }
    }

    private static void pingOperation() {
        System.out.print("Target deployment:\n> ");
        int targetDeployment = Integer.parseInt(scanner.nextLine());

        try {
            hdfs.ping(targetDeployment);
        } catch (IOException ex) {
            System.out.println("Encountered IOException while pinging NameNode deployment " +
                    targetDeployment + ".");
            ex.printStackTrace();
        }
    }

    private static void readOperation() {
        System.out.print("File path:\n> ");
        String fileName = scanner.nextLine();
        readFile(fileName);
    }

    /**
     * Read the HopsFS/HDFS file at the given path.
     * @param fileName The path to the file to read.
     */
    private static void readFile(String fileName) {
        Path filePath = new Path("hdfs://10.241.64.14:9000/" + fileName);

        try {
            FSDataInputStream inputStream = hdfs.open(filePath);
            BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
            String line = null;
            while ((line = br.readLine()) != null)
                System.out.println(line);

            inputStream.close();
            br.close();
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    private static void deleteOperation() {
        System.out.print("File or directory path:\n> ");
        String targetPath = scanner.nextLine();

        Path filePath = new Path("hdfs://10.241.64.14:9000/" + targetPath);

        try {
            boolean success = hdfs.delete(filePath, true);
            System.out.println("\t Delete was successful: " + success);
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
                System.out.println("\t Invalid input! Please enter an integer.");
            } catch (Exception ex) {
                ex.printStackTrace();
                System.exit(1);
            }
        }
    }

    private static void printMenu() {
        System.out.println("\n\n====== MENU ======");
        System.out.println("Operations:");
        System.out.println("(0) Exit\n(1) Create file\n(2) Create directory\n(3) Read contents of file.\n(4) Rename" +
                "\n(5) Delete\n(6) List directory\n(7) Append\n(8) Create Subtree.\n(9) Ping\n(10) Prewarm" +
                "\n(11) Write Files to Directory\n(12) Read files");
        System.out.println("==================");
        System.out.println("\nWhat would you like to do?");
        System.out.print("> ");
    }
}
