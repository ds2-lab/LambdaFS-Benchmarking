package com.gmail.benrcarver.distributed;

import org.apache.commons.cli.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.fs.FileStatus;

import java.io.*;

/**
 * This is the class that is executed when running the application. It starts the Commander or Follower.
 */
public class InteractiveTest {
    public static final Log LOG = LogFactory.getLog(InteractiveTest.class);

    public static void main(String[] args) throws InterruptedException, IOException {
        Options cmdLineOpts = new Options();
        Option workerOpt = new Option("w", "worker", false, "If true, run this program as a worker, listening to commands from a remote leader.");
        Option leaderIpOpt = new Option("l", "leader_ip", true, "The IP address of the Leader. Only used when this process is designated as a worker.");
        Option leaderPort = new Option("p", "leader_port", true, "The port of the Leader. Only used when this process is designated as a worker.");
        Option localOption = new Option("n", "nondistributed", false, "Run in non-distributed mode, meaning we don't launch any followers.");

        Option yamlPath = new Option("y", "yaml_path", true, "Path to YAML configuration file.");

        cmdLineOpts.addOption(workerOpt);
        cmdLineOpts.addOption(leaderIpOpt);
        cmdLineOpts.addOption(leaderPort);
        cmdLineOpts.addOption(yamlPath);
        cmdLineOpts.addOption(localOption);

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

        // If the user specified the worker argument, then we launch as a Follower.
        // Otherwise, we launch as a Commander. The Commander takes input from the user
        // and directs itself and Followers to execute particular file system operations.
        if (cmd.hasOption("worker")) {
            LOG.info("Beginning execution as FOLLOWER now.");
            Follower follower = new Follower(
                    cmd.getOptionValue("leader_ip"),
                    Integer.parseInt(cmd.getOptionValue("leader_port")));
            follower.connect(); // Connect to the Commander.
            follower.waitUntilDone(); // Basically run forever.
        } else {
            Commander commander = Commander.getOrCreateCommander(
                    cmd.getOptionValue("leader_ip"),
                    Integer.parseInt(cmd.getOptionValue("leader_port")),
                    cmd.getOptionValue("yaml_path"),
                    cmd.hasOption("nondistributed"), /* If it has this option, then it is true */
                    cmd.hasOption("loglevel") ? cmd.getOptionValue("loglevel") : "DEBUG",
                    cmd.hasOption("disable_consistency") /* If it has this option, then it is true */);
            commander.start();
        }
    }
}