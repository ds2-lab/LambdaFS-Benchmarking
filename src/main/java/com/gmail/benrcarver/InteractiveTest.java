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
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.InputMismatchException;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;

public class InteractiveTest {
    private static final Scanner scanner = new Scanner(System.in);
    private static DistributedFileSystem hdfs;
    private static Configuration configuration;

    public static void main(String[] args) {
        System.out.println("Starting HdfsTest now.");
        configuration = new Configuration();
        System.out.println("Created configuration.");
        hdfs = new DistributedFileSystem();
        System.out.println("Created DistributedFileSystem object.");

        try {
            hdfs.initialize(new URI("hdfs://10.241.64.14:9000"), configuration);
            System.out.println("Called initialize() successfully.");
        } catch (URISyntaxException | IOException ex) {
            ex.printStackTrace();
        }

        while (true) {
            printMenu();
            int op = getNextOperation();

            switch(op) {
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
                    System.out.println("Exiting now... goodbye!");
                    try {
                        hdfs.close();
                    } catch (IOException ex) {
                        System.out.println("Encountered exception while closing file system...");
                        ex.printStackTrace();
                    }
                    System.exit(0);
                default:
                    System.out.println("ERROR: Unknown or invalid operation specified: " + op);
                    break;
            }
        }
    }

    private static void createFileOperation() {
        System.out.println("File path:\n> ");
        String fileName = scanner.nextLine();
        System.out.println("File contents:\n> ");
        String fileContents = scanner.nextLine();

        Path filePath = new Path("hdfs://10.241.64.14:9000/" + fileName);

        try {
            FSDataOutputStream outputStream = hdfs.create(filePath);
            System.out.println("\t Called create() successfully.");
            BufferedWriter br = new BufferedWriter(new OutputStreamWriter(outputStream, "UTF-8"));
            System.out.println("\t Created BufferedWriter object.");
            br.write(fileContents);
            System.out.println("\t Wrote \"" + fileContents + "\" using BufferedWriter.");
            br.close();
            System.out.println("\t Closed BufferedWriter.");
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    private static void renameOperation() {
        System.out.println("Original file path:\n> ");
        String originalFileName = scanner.nextLine();
        System.out.println("Renamed file path:\n> ");
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
        System.out.println("Target directory:\n> ");
        String targetDirectory = scanner.nextLine();

        try {
            FileStatus[] fileStatus = hdfs.listStatus(new Path("hdfs://localhost:9000/" + targetDirectory));
            for(FileStatus status : fileStatus){
                System.out.println(status.getPath().toString());
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    private static void mkdirOperation() {
        System.out.println("New directory path:\n> ");
        String newDirectoryName = scanner.nextLine();

        Path filePath = new Path("hdfs://10.241.64.14:9000/" + newDirectoryName);

        try {
            System.out.println("\t Attempting to create new directory: \"" + newDirectoryName + "\"");
            boolean directoryCreated = hdfs.mkdirs(filePath);
            System.out.println("\t Directory created successfully: " + directoryCreated);
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    private static void readOperation() {
        System.out.println("File path:\n> ");
        String fileName = scanner.nextLine();

        Path filePath = new Path("hdfs://10.241.64.14:9000/" + fileName);

        try {
            BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
            FSDataInputStream inputStream = hdfs.open(filePath);
            System.out.println("File contents:\n" + inputStream.available());
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    private static void deleteOperation() {
        System.out.println("File or directory path:\n> ");
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
                int op = Integer.parseInt(input);
                return op;
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
        System.out.println("(1) Create file\n(2) Create directory\n(3) Read contents of file.\n(4) Rename\n(5) Delete\n(6) List directory\n(7) Exit.");
        System.out.println("==================");
        System.out.println("\tWhat would you like to do?");
        System.out.print("> ");
    }
}
