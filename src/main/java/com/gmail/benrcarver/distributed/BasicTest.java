package com.gmail.benrcarver.distributed;

import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.*;

public class BasicTest {
    public static void main(String[] args) throws IOException, URISyntaxException {
        Options cmdLineOpts = new Options();
        Option configFilePathOption = new Option("c", "config_path", false, "Path to hdfs-site.xml configuration file.");
        cmdLineOpts.addOption(configFilePathOption);

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

        String configFilePath;

        if (cmd.hasOption("config_path")) {
            configFilePath = cmd.getOptionValue("config_path");
        } else {
            configFilePath = "/home/ubuntu/repos/LambdaFS/hadoop-dist/target/hadoop-3.2.0.3-SNAPSHOT/etc/hadoop/hdfs-site.xml";
        }

        DistributedFileSystem hdfs = new DistributedFileSystem();

        Configuration hdfsConfiguration = new Configuration();

        File configFile = new File(configFilePath);
        URL configFileURL = configFile.toURI().toURL();
        hdfsConfiguration.addResource(configFileURL);

        String privateIpv4;
        try(final DatagramSocket socket = new DatagramSocket()) {
            socket.connect(InetAddress.getByName("8.8.8.8"), 10002);
            privateIpv4 = socket.getLocalAddress().getHostAddress();
        }

        String nameNodeEndpoint = "hdfs://" + privateIpv4 + ":9000/";
        Commander.NAME_NODE_ENDPOINT = nameNodeEndpoint;
        hdfs.initialize(new URI(nameNodeEndpoint), hdfsConfiguration);

        System.out.println("Creating file \"test.txt\" now.\n");

        Commands.createFile("test.txt", "", hdfs);

        System.out.println("Created file \"test.txt\". Let's see if it shows up when we list the contents of the root directory \"/\"...\n");

        System.out.println("Directory contents:\n");
        FileStatus[] fileStatus = hdfs.listStatus(new Path(nameNodeEndpoint + "/"));
        for (FileStatus status : fileStatus)
            System.out.println(status.getPath().toString());
        System.out.println("\nDirectory \"/\" contains " + fileStatus.length + " files.\n");

        if (fileStatus.length != 1) {
            System.out.println("[ERROR] The directory should contain 1 file, not " + fileStatus.length + ".");
            System.out.println("Something is going wrong!");
        } else {
            System.out.println("[SUCCESS] Everything appears to be working!");
        }

        System.out.println("\nCreating subdirectory \"/my_directory\" now.");

        boolean success = Commands.mkdir("/my_directory", hdfs);

        if (!success) {
            System.out.println("Something went wrong when creating directory \"/my_directory\"...");
            System.exit(1);
        }

        System.out.println("Created directory \"/my_directory\". Let's see if it shows up when we list the contents of the root directory \"/\"...\n");

        System.out.println("Contents of directory \"/\":\n");
        fileStatus = hdfs.listStatus(new Path(nameNodeEndpoint + "/"));
        for (FileStatus status : fileStatus)
            System.out.println(status.getPath().toString());
        System.out.println("\nDirectory \"/\" contains " + fileStatus.length + " files.\n");

        if (fileStatus.length != 2) {
            System.out.println("[ERROR] The directory should contain 2 files now, not " + fileStatus.length + ".");
            System.out.println("Something is going wrong!");
        } else {
            System.out.println("[SUCCESS] Everything appears to be working!");
        }

        System.out.println("\nLet's create some files in the /my_directory folder.");

        int numFiles;
        while (true) {
            numFiles = Commands.getIntFromUser("How many files should we create? [1 - 100]");

            if (numFiles <= 0) {
                System.out.println("\nPlease enter a positive value between 1 and 100. You entered: \"" + numFiles +"\"");
            }
            else if (numFiles > 100) {
                System.out.println("Please enter a value between 1 and 100. You entered: \"" + numFiles + "\"");
            }
            else {
                System.out.println("\nGot it! We'll create " + numFiles + " file(s) in the \"/my_directory\" directory.");
                break;
            }
        }

        for (int i = 0; i < numFiles; i++) {
            String filePath = "/my_directory/my_file" + i + ".txt";
            System.out.println("Creating file: \"" + filePath + "\"");
            success = Commands.createFile(filePath, "", hdfs);

            if (!success) {
                System.out.println("Something went wrong when creating file \"" + filePath + "\"");
                System.exit(1);
            }
        }

        System.out.println("\nListing the contents of the \"/my_directory\" directory now.");
        System.out.println("Contents of directory \"/my_directory/\":\n");
        fileStatus = hdfs.listStatus(new Path(nameNodeEndpoint + "/my_directory"));
        for (FileStatus status : fileStatus)
            System.out.println(status.getPath().toString());
        System.out.println("\nDirectory \"/my_directory\" contains " + fileStatus.length + " files.\n");

        if (fileStatus.length != numFiles) {
            System.out.println("[ERROR] The directory should contain " + numFiles + " file(s), not " + fileStatus.length + ".");
            System.out.println("Something is going wrong!");
        } else {
            System.out.println("[SUCCESS] Everything appears to be working!");
        }

        System.out.println("Now let's try to delete everything that we just created, starting with \"test.txt\"");

        success = Commands.delete("test.txt", hdfs);

        if (!success) {
            System.out.println("Error while deleting \"test.txt\"...");
            System.exit(1);
        }

        System.out.println("Successfully deleted \"test.txt\". Next, let's delete \"/my_directory\"");

        success = Commands.delete("/my_directory", hdfs);

        if (!success) {
            System.out.println("Error while deleting \"/my_directory\"...");
            System.exit(1);
        }

        System.out.println("Let's list the contents of the root directory \"/\" again and see if it's empty.");

        System.out.println("Contents of directory \"/\":\n");
        fileStatus = hdfs.listStatus(new Path(nameNodeEndpoint + "/"));
        for (FileStatus status : fileStatus)
            System.out.println(status.getPath().toString());
        System.out.println("\nDirectory \"/\" contains " + fileStatus.length + " files.\n");

        if (fileStatus.length != 0) {
            System.out.println("[ERROR] The directory should contain no files now, not " + fileStatus.length + ".");
            System.out.println("Something is going wrong!");
        } else {
            System.out.println("[SUCCESS] Everything appears to be working!");
        }

        System.out.println("Exiting.");

        hdfs.close();

        System.exit(0);
    }
}
