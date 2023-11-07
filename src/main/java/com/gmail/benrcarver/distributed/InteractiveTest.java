package com.gmail.benrcarver.distributed;

import com.jcraft.jsch.JSchException;
import io.grpc.LoadBalancerRegistry;
import io.grpc.internal.PickFirstLoadBalancerProvider;
import org.apache.commons.cli.*;
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
        // Create the "./output/" directory if it does not already exist.
        File directory = new File("./output/");
        if (! directory.exists()){
            directory.mkdir();
        }

        LoadBalancerRegistry.getDefaultRegistry().register(new PickFirstLoadBalancerProvider());

        Options cmdLineOpts = new Options();
        Option workerOpt = new Option("w", "worker", false, "If true, run this program as a worker, listening to commands from a remote leader.");
        Option leaderIpOpt = new Option("l", "leader_ip", true, "The IP address of the Leader. Only used when this process is designated as a worker.");
        Option leaderPort = new Option("p", "leader_port", true, "The port of the Leader. Only used when this process is designated as a worker.");
        Option localOption = new Option("n", "nondistributed", false, "Run in non-distributed mode, meaning we don't launch any followers.");
        //Option logLevelOption = new Option("ll", "loglevel", true, "The log4j log level to pass to the NameNodes.");
        Option consistencyProtocolOption = new Option("d", "disable_consistency", false, "If passed, then we default to disabling the consistency protocol.");
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
        //cmdLineOpts.addOption(logLevelOption);
        cmdLineOpts.addOption(consistencyProtocolOption);
        cmdLineOpts.addOption(scpJarsOpt);
        cmdLineOpts.addOption(scpConfigOpt);
        cmdLineOpts.addOption(manualLaunchFollowersOpt);

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

        int numFollowers = -1;
        if (cmd.hasOption("num_followers"))
            numFollowers = Integer.parseInt(cmd.getOptionValue("num_followers"));

        if (cmd.hasOption("worker")) {
            LOG.info("Beginning execution as FOLLOWER now.");
            Follower follower = new Follower(
                    cmd.getOptionValue("leader_ip"),
                    Integer.parseInt(cmd.getOptionValue("leader_port")),
                    //cmd.hasOption("loglevel") ? cmd.getOptionValue("loglevel") : "DEBUG",
                    cmd.hasOption("disable_consistency") /* If it has this option, then it is true */);
            follower.connect();
            follower.waitUntilDone();
        } else {
            Commander commander = Commander.getOrCreateCommander(
                    cmd.getOptionValue("leader_ip"),
                    Integer.parseInt(cmd.getOptionValue("leader_port")),
                    cmd.getOptionValue("yaml_path"),
                    cmd.hasOption("nondistributed"), /* If it has this option, then it is true */
                    cmd.hasOption("disable_consistency"), /* If it has this option, then it is true */
                    numFollowers,
                    cmd.hasOption("scp_jars"),
                    cmd.hasOption("scp_config"),
                    cmd.hasOption("manually_launch_followers"));
            commander.start();
        }
    }
}