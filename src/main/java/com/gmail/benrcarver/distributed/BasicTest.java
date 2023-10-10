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

        hdfs.close();

        System.out.println("Exiting.");

        System.exit(0);
    }
}
