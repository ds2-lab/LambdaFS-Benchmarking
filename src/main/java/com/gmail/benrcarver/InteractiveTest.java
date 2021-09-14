package com.gmail.benrcarver;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.InputMismatchException;
import java.util.Scanner;
import com.google.gson.JsonObject;
import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;

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
            hdfs.initialize(new URI("hdfs://10.150.0.6:9000"), configuration);
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
                    System.out.println("Exiting now... goodbye!");
                    hdfs.close();
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

        try {
            FSDataOutputStream outputStream = hdfs.create(fileName);
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

        try {
            System.out.println("\t Original file path: \"" + originalFileName + "\"");
            System.out.println("\t New file path: \"" + renamedFileName + "\"");
            hdfs.rename(originalFileName, renamedFileName);
            System.out.println("\t Finished rename operation.");
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    private static void mkdirOperation() {
        System.out.println("New directory path:\n> ");
        String newDirectoryName = scanner.nextLine();

        try {
            System.out.println("\t Attempting to create new directory: \"" + newDirectoryName + "\"");
            boolean directoryCreated = hdfs.mkdirs(newDirectoryName);
            System.out.println("\t Directory created successfully: " + directoryCreated);
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    private static void readOperation() {
        System.out.println("File path:\n> ");
        String fileName = scanner.nextLine();

        try {
            BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
            Path path = new Path(filePath);
            FSDataInputStream inputStream = hdfs.open(path);
            System.out.println(inputStream.available());
            fs.close();
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    private static int getNextOperation() {
        while (true) {
            try {
                int op = scanner.nextInt();
                return op;
            } catch (InputMismatchException ex) {
                System.out.println("Invalid input! Please enter an integer.");
            } catch (Exception ex) {
                ex.printStackTrace();
                System.exit(1);
            }
        }
    }

    private static void printMenu() {
        System.out.println("\n\n====== MENU ======");
        System.out.println("Operations:");
        System.out.println("(1) Create file\n(2) Create directory\n(3) Read contents of file.\n(4) Rename\n(5) Exit.");
        System.out.println("==================");
        System.out.println("\tWhat would you like to do?");
        System.out.print("> ");
    }
}
