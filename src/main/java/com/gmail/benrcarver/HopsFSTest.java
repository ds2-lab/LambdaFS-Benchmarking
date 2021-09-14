package com.gmail.benrcarver;

import com.google.gson.JsonObject;
import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

/**
 * This is the OLD HopsFS test/benchmark.
 */
public class HopsFSTest {

    private static final String DATA_SOURCE = "DATA_SOURCE";
    private static final String FROM_CACHE = "FROM_CACHE";
    private static final String FROM_NDB = "FROM_NDB";
    private static final String RESULT = "RESULT";
    private static final String NUM_QUERIES = "NUM_QUERIES";
    private static final String CONNECTION_URL = "CONNECTION_URL";
    private static final String OPERATION_TYPE = "OPERATION_TYPE";
    private static final String QUERY = "QUERY";
    private static final String CLUSTERJ = "CLUSTERJ";
    private static final String ID = "ID";
    private static final String FIRST_NAME = "FIRST_NAME";
    private static final String LAST_NAME = "LAST_NAME";
    private static final String POSITION = "POSITION";
    private static final String DEPARTMENT = "DEPARTMENT";
    private static final String CONFIG_PATH = "ndb-config.properties";
    private static final String DEFAULT_DATA_SOURCE = "FROM_NDB";

    private static final String DEFAULT_FILENAME = "helloWorld.txt";
    private static final String DEFAULT_FILE_CONTENTS = "Hello, world!";

    private static final String DEFAULT_OPERATION = "create";
    private static final String DEFAULT_NEW_NAME = "rename_test.txt";
    private static final String LIST_OPERATIONS_STRING = "create, delete, rename, mkdir";

    public static void main(String[] args) {
        testFileSystemOperation(args);
    }

    private static void testLatencyBenchmark(String connectionUrl, String dataSource, String query, int id, int numQueries) {
        System.out.println("Starting HdfsTest now.");
        Configuration configuration = new Configuration();
        System.out.println("Created configuration.");
        DistributedFileSystem dfs = new DistributedFileSystem();
        System.out.println("Created DistributedFileSystem object.");

        try {
            dfs.initialize(new URI("hdfs://10.241.64.14:9000"), configuration);
            System.out.println("Called initialize() successfully.");
        } catch (URISyntaxException | IOException ex) {
            ex.printStackTrace();
        }

        System.out.println("Attempting to call Latency Benchmark now...");

        ArrayList<Double> results = new ArrayList<Double>();

        try {
            for (int i = 0; i < numQueries; i++) {
                long startTime = System.nanoTime();
                JsonObject result = dfs.latencyBenchmark(connectionUrl, dataSource, query, id);
                long endTime = System.nanoTime();
                long durationInNano = (endTime - startTime);
                double durationInMillis = (double)TimeUnit.NANOSECONDS.toMillis(durationInNano);
                results.add(durationInMillis / 1000);
                System.out.println("Finished latency benchmark #" + i + " in " + (durationInMillis / 1000) + " seconds.");
                System.out.println(result);
            }
        } catch (SQLException | IOException ex) {
            System.out.println("\n[ERROR] Encountered exception while attempting latency benchmark...");
            ex.printStackTrace();
        }

        double sum = results.stream().mapToDouble(Double::doubleValue).sum();
        double average = (double)sum / results.size();
        double max = Collections.max(results);
        double min = Collections.min(results);
        System.out.println("Average/Min/Max/All:\n" + average + "\n" + min + "\n" + max + "\n" + results.toString());
    }

    private static void testFileSystemOperation(String[] args) {
        // Used by all FS operations.
        Options options = new Options();
        Option fileNameOption = new Option("f", "fileName", true, "The name of the file to create.");
        fileNameOption.setRequired(false);

        // Just used during `create`.
        Option fileCreateContentsOption = new Option("c", "contents", true, "String to write as the file contents.");
        fileCreateContentsOption.setRequired(false);

        // Used to specify which operation is being performed.
        Option operationOption = new Option("o", "operation", true, "Specify the operation to perform by name. Valid operations include: " + LIST_OPERATIONS_STRING);
        fileCreateContentsOption.setRequired(false);

        Option newNameOption = new Option("n", "new_name", true, "The new name for the file (used during rename).");
        newNameOption.setRequired(false);

        options.addOption(fileNameOption);
        options.addOption(fileCreateContentsOption);
        options.addOption(operationOption);
        options.addOption(newNameOption);

        CommandLineParser parser = new GnuParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd = null;

        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            formatter.printHelp("utility-name", options);

            System.exit(1);
        }

        String fileName;
        if (cmd.hasOption("fileName"))
            fileName = cmd.getOptionValue("fileName");
        else
            fileName = DEFAULT_FILENAME;

        String fileContents;
        if (cmd.hasOption("contents"))
            fileContents = cmd.getOptionValue("contents");
        else
            fileContents = DEFAULT_FILE_CONTENTS;

        String operation;
        if (cmd.hasOption("operation"))
            operation = cmd.getOptionValue("operation");
        else
            operation = DEFAULT_OPERATION;

        String newFileName;
        if (cmd.hasOption("new_name"))
            newFileName = cmd.getOptionValue("new_name");
        else
            newFileName = DEFAULT_NEW_NAME;

        System.out.println("=-=-==-=-==-=-==-=-==-=-==-=-==-=-==-=-==-=-=");
        System.out.println("File name: \"" + fileName + "\"");
        System.out.println("File contents: \"" + fileContents + "\"");
        System.out.println("New file name: \"" + newFileName + "\"");
        System.out.println("- - - - - - - - - - - - - - - - - - - - - - -");
        System.out.println("Performing operation: \"" + operation + "\"");
        System.out.println("=-=-==-=-==-=-==-=-==-=-==-=-==-=-==-=-==-=-=");

        System.out.println("Starting HdfsTest now.");
        Configuration configuration = new Configuration();
        System.out.println("Created configuration.");
        DistributedFileSystem hdfs = new DistributedFileSystem();
        System.out.println("Created DistributedFileSystem object.");

        try {
            hdfs.initialize(new URI("hdfs://10.241.64.14:9000"), configuration);
            System.out.println("Called initialize() successfully.");
        } catch (URISyntaxException | IOException ex) {
            ex.printStackTrace();
        }

        Path filePath = new Path("hdfs://10.241.64.14:9000/" + fileName);
        Path filePathRename = new Path("hdfs://10.241.64.14:9000/" + newFileName);
        System.out.println("Created Path object.");

        switch (operation) {
            case "create":
                try {
                    FSDataOutputStream outputStream = hdfs.create(filePath);
                    System.out.println("Called create() successfully.");
                    BufferedWriter br = new BufferedWriter(new OutputStreamWriter(outputStream, "UTF-8"));
                    System.out.println("Created BufferedWriter object.");
                    br.write(fileContents);
                    System.out.println("Wrote \"" + fileContents + "\" using BufferedWriter.");
                    br.close();
                    System.out.println("Closed BufferedWriter.");
                    hdfs.close();
                    System.out.println("Closed DistributedFileSystem object.");
                } catch (IOException ex) {
                    ex.printStackTrace();
                }
                break;
            case "delete":
                try {
                    boolean success = hdfs.delete(filePath, true);
                    System.out.println("Delete was successful: " + success);
                    hdfs.close();
                    System.out.println("Closed DistributedFileSystem object.");
                } catch (IOException ex) {
                    ex.printStackTrace();
                }
                break;
            case "rename":
                try {
                    System.out.println("Original file path: \"" + filePath + "\"");
                    System.out.println("New file path: \"" + filePathRename + "\"");
                    hdfs.rename(filePath, filePathRename);
                    System.out.println("Finished rename operation.");
                    hdfs.close();
                    System.out.println("Closed DistributedFileSystem object.");
                } catch (IOException ex) {
                    ex.printStackTrace();
                }
                break;
            case "mkdir":
                try {
                    System.out.println("Attempting to create new directory: \"" + filePath + "\"");
                    boolean directoryCreated = hdfs.mkdirs(filePath);
                    System.out.println("Directory created successfully: " + directoryCreated);
                    hdfs.close();
                    System.out.println("Closed DistributedFileSystem object.");
                } catch (IOException ex) {
                    ex.printStackTrace();
                }
                break;
            default:
                throw new IllegalArgumentException("Unknown file system operation: \"" + operation + "\"");
        }
    }
}