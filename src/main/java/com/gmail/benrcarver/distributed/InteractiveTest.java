package com.gmail.benrcarver.distributed;

import com.gmail.benrcarver.distributed.util.Utils;
import com.jcraft.jsch.JSchException;
import io.grpc.LoadBalancerRegistry;
import io.grpc.internal.PickFirstLoadBalancerProvider;
import org.apache.commons.cli.*;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.*;

/**
 * Application Driver.
 *
 * This is the class that is executed when running the application. It starts the Commander or Follower.
 */
public class InteractiveTest {
    public static final Logger LOG = LoggerFactory.getLogger(InteractiveTest.class);

    public static void main(String[] args) throws InterruptedException, IOException, JSchException {
        LoadBalancerRegistry.getDefaultRegistry().register(new PickFirstLoadBalancerProvider());

        Options cmdLineOpts = new Options();
        Option workerOpt = new Option("w", "worker", false, "If true, run this program as a worker, listening to commands from a remote leader.");
        Option leaderIpOpt = new Option("l", "leader_ip", true, "The IP address of the Leader. Only used when this process is designated as a worker.");
        Option leaderPort = new Option("p", "leader_port", true, "The port of the Leader. Only used when this process is designated as a worker.");
        Option localOption = new Option("n", "nondistributed", false, "Run in non-distributed mode, meaning we don't launch any followers.");

        Option constructBenchmarkFS = new Option("s", "setup_filesystem", false, "Run a hard-coded script to create a bunch of files and directories for benchmarking.");

        //Option logLevelOption = new Option("ll", "loglevel", true, "The log4j log level to pass to the NameNodes.");
        Option numFollowersOpt = new Option("f", "num_followers", true, "Start only the first 'f' followers listed in the config.");
        Option scpJarsOpt = new Option("j", "scp_jars", false, "The commander should SCP the JAR files to each follower.");
        Option scpConfigOpt = new Option("c", "scp_config", false, "The command should SCP the config file to each follower.");
        Option manualLaunchFollowersOpt = new Option("m", "manually_launch_followers", false, "When passed, the Commander will not automatically start the Followers. It will still copy files, however.");
        Option yamlPath = new Option("y", "yaml_path", true, "Path to YAML configuration file.");

        cmdLineOpts.addOption(workerOpt);
        cmdLineOpts.addOption(leaderIpOpt);
        cmdLineOpts.addOption(leaderPort);
        cmdLineOpts.addOption(yamlPath);
        cmdLineOpts.addOption(localOption);
        cmdLineOpts.addOption(numFollowersOpt);
        cmdLineOpts.addOption(scpJarsOpt);
        cmdLineOpts.addOption(scpConfigOpt);
        cmdLineOpts.addOption(manualLaunchFollowersOpt);
        cmdLineOpts.addOption(constructBenchmarkFS);

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

        if (cmd.hasOption("setup_filesystem")) {
            LOG.info("Will create benchmark-ready file system state...");

            // TODO: Using the tree10920 file, create all of the required directories.
            // TODO: Also create a directory for the 50 and 186 files.
            // TODO: Using the 109200, 50, and 186 files, create all of the files.

            Commander commander = Commander.getOrCreateCommander(
                    cmd.getOptionValue("leader_ip"),
                    Integer.parseInt(cmd.getOptionValue("leader_port")),
                    cmd.getOptionValue("yaml_path"),
                    true, /* If it has this option, then it is true */
                    0, false, false,false);
            commander.startNoLoop();
            //DistributedBenchmarkResult dirsResult = commander.createDirectoriesFromFile(
            //        "./fs_data/dirs4writes_serverless.txt", 16);
            DistributedBenchmarkResult filesResult = commander.createEmptyFilesFromFile(
                    "./fs_data/files_alt.txt", 16);

//            LOG.info("========== DIRS RESULT ==========");
//            LOG.info("Num Ops Performed   : " + dirsResult.numOpsPerformed);
//            LOG.info("Duration (sec)      : " + dirsResult.durationSeconds);
//            LOG.info("Throughput          : " + dirsResult.getOpsPerSecond());
//
//            if (dirsResult.latencyStatistics != null) {
//                DescriptiveStatistics latency = dirsResult.latencyStatistics;
//
//                LOG.info("Latency (ms) [min: " + latency.getMin() + ", max: " + latency.getMax() +
//                        ", avg: " + latency.getMean() + ", std dev: " + latency.getStandardDeviation() +
//                        ", N: " + latency.getN() + "]");
//            }

            LOG.info("========== FILES RESULT ==========");
            LOG.info("Num Ops Performed   : " + filesResult.numOpsPerformed);
            LOG.info("Duration (sec)      : " + filesResult.durationSeconds);
            LOG.info("Throughput          : " + filesResult.getOpsPerSecond());

            if (filesResult.latencyStatistics != null) {
                DescriptiveStatistics latency = new DescriptiveStatistics(filesResult.latencyStatistics);

                LOG.info("Latency (ms) [min: " + latency.getMin() + ", max: " + latency.getMax() +
                        ", avg: " + latency.getMean() + ", std dev: " + latency.getStandardDeviation() +
                        ", N: " + latency.getN() + "]");
            }
        }

        // If the user specified the worker argument, then we launch as a Follower.
        // Otherwise, we launch as a Commander. The Commander takes input from the user
        // and directs itself and Followers to execute particular file system operations.
        int numFollowers = -1;
        if (cmd.hasOption("num_followers"))
            numFollowers = Integer.parseInt(cmd.getOptionValue("num_followers"));

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
                    numFollowers,
                    cmd.hasOption("scp_jars"),
                    cmd.hasOption("scp_config"),
                    cmd.hasOption("manually_launch_followers"));
            commander.start();
        }
    }
}