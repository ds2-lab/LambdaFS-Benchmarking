package com.gmail.benrcarver.distributed;

import com.esotericsoftware.kryonet.Connection;
import com.esotericsoftware.kryonet.FrameworkMessage;
import com.esotericsoftware.kryonet.Listener;
import com.esotericsoftware.kryonet.Server;
import com.gmail.benrcarver.distributed.coin.BMConfiguration;
import com.gmail.benrcarver.distributed.util.Utils;
import com.gmail.benrcarver.distributed.workload.RandomlyGeneratedWorkload;
import com.gmail.benrcarver.distributed.workload.WorkloadResponse;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.jcraft.jsch.*;
import io.hops.metrics.OperationPerformed;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.math3.stat.descriptive.SynchronizedDescriptiveStatistics;
import org.apache.commons.math3.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.yaml.snakeyaml.Yaml;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.DecimalFormat;
import java.util.*;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static com.gmail.benrcarver.distributed.Commands.*;
import static com.gmail.benrcarver.distributed.Constants.*;
import static com.gmail.benrcarver.distributed.Constants.TRACK_OP_PERFORMED;

/**
 * Controls a fleet of distributed machines. Executes HopsFS benchmarks based on user input/commands.
 */
public class Commander {
    public static final Logger LOG = LoggerFactory.getLogger(Commander.class);

    private final List<FollowerConnection> followers;

    private static final int COMMANDER_TCP_BUFFER_SIZES = Follower.FOLLOWER_TCP_BUFFER_SIZES;

    private static final String LEADER_PREFIX = "[LEADER TCP SERVER]";

    /**
     * Use with String.format(LAUNCH_FOLLOWER_CMD, leader_ip, leader_port)
     */
    private static final String LAUNCH_FOLLOWER_CMD = "source ~/.bashrc; cd /home/ubuntu/repos/HopsFS-Benchmarking-Utility; java -Dlog4j.configuration=file:/home/ubuntu/repos/HopsFS-Benchmarking-Utility/src/main/resources/log4j.properties -Dsun.io.serialization.extendedDebugInfo=true -Xmx%sg -Xms%sg -XX:+UseConcMarkSweepGC -XX:+UnlockDiagnosticVMOptions -XX:ParGCCardsPerStrideChunk=32768 -XX:+CMSScavengeBeforeRemark -XX:MaxGCPauseMillis=350 -XX:MaxTenuringThreshold=2 -XX:MaxNewSize=%sm -XX:+CMSClassUnloadingEnabled -XX:+UseCMSInitiatingOccupancyOnly -XX:CMSInitiatingOccupancyFraction=75 -XX:+ScavengeBeforeFullGC -verbose:gc -XX:+PrintGCTimeStamps -XX:+PrintGCDetails -cp \".:target/HopsFSBenchmark-1.0-jar-with-dependencies.jar:/home/ubuntu/repos/hops/hadoop-dist/target/hadoop-3.2.0.3-SNAPSHOT/share/hadoop/hdfs/lib/*:/home/ubuntu/repos/hops/hadoop-dist/target/hadoop-3.2.0-SNAPSHOT/share/hadoop/common/lib/*:/home/ubuntu/repos/hops/hadoop-hdfs-project/hadoop-hdfs-client/target/hadoop-hdfs-client-3.2.0.3-SNAPSHOT.jar:/home/ubuntu/repos/hops/hops-leader-election/target/hops-leader-election-3.2.0.3-SNAPSHOT.jar:/home/ubuntu/openwhisk-runtime-java/core/java8/libs/*:/home/ubuntu/repos/hops/hadoop-hdfs-project/hadoop-hdfs/target/hadoop-hdfs-3.2.0.3-SNAPSHOT.jar:/home/ubuntu/repos/hops/hadoop-common-project/hadoop-common/target/hadoop-common-3.2.0.3-SNAPSHOT.jar\" com.gmail.benrcarver.distributed.InteractiveTest --leader_ip %s --leader_port %d --yaml_path /home/ubuntu/repos/HopsFS-Benchmarking-Utility/config.yaml --worker";

    private static final String BENCHMARK_JAR_PATH = "/home/ubuntu/repos/HopsFS-Benchmarking-Utility/target/HopsFSBenchmark-1.0-jar-with-dependencies.jar";

    private static final String HADOOP_HDFS_JAR_PATH = "/home/ubuntu/repos/hops/hadoop-hdfs-project/hadoop-hdfs/target/hadoop-hdfs-3.2.0.3-SNAPSHOT.jar";

    /**
     * Has a default value.
     */
    public static String NAME_NODE_ENDPOINT = "hdfs://10.150.0.17:9000/";

    /**
     * Used to obtain input from the user.
     */
    private final Scanner scanner = new Scanner(System.in);

    /**
     * Used to communicate with followers.
     */
    private final Server tcpServer;

    /**
     * Indicates whether followers are currently set to track operations performed.
     */
    private boolean followersTrackOpsPerformed = false;

    /**
     * The leader's IP address.
     */
    private final String ip;

    /**
     * Port that we are listening on.
     */
    private final int port;

    /**
     * Configurations of the followers. Used when launching them via SSH.
     */
    private List<FollowerConfig> followerConfigs;

    /**
     * Time (in milliseconds) to sleep after each trial.
     * This gives NameNodes a chance to perform any clean-up (e.g., garbage collection).
     */
    private int postTrialSleepInterval = 5000;

    /**
     * Fully-qualified path of hdfs-site.xml configuration file.
     */
    public static String hdfsConfigFilePath;

    /**
     * Map from operation ID to the queue in which distributed results should be placed by the TCP server.
     */
    private final ConcurrentHashMap<String, BlockingQueue<DistributedBenchmarkResult>> resultQueues;

    private final BlockingQueue<WorkloadResponse> workloadResponseQueue;

    /**
     * Indicates whether we're running in the so-called non-distributed mode or not.
     *
     * In non-distributed mode, we assume that there is only one client VM.
     * The Commander operates as if there are no other client VMs to connect to.
     */
    private final boolean nonDistributed;

    /**
     * Indicates whether the consistency protocol is enabled.
     * This is passed into the client-facing Serverless HopsFS API.
     */
    public static boolean consistencyEnabled = true;

    /**
     * The {@link Commander} class uses a singleton pattern.
     */
    private static Commander instance;

    /**
     * The main DistributedFileSystem instance. Used by the main thread and also to keep track of metrics.
     */
    private DistributedFileSystem primaryHdfs;

    /**
     * The approximate number of collections that occurred.
     */
    private long numGarbageCollections = 0L;

    /**
     * The approximate time, in milliseconds, that has elapsed during GCs
     */
    private long garbageCollectionTime = 0L;

    /**
     * Indicates whether the target filesystem is Serverless HopsFS or Vanilla HopsFS.
     *
     * If true, then the target filesystem is Serverless HopsFS.
     * If false, then the target filesystem is Vanilla HopsFS.
     */
    private boolean isServerless = true;

    /**
     * If true, then the Commander will execute tasks/jobs too.
     * If false, then the Commander will only direct Followers to perform tasks and aggregate their results.
     */
    private boolean commanderExecutesToo = true;

    /**
     * Start the first 'numFollowersFromConfigToStart' followers listed in the config.
     */
    private int numFollowersFromConfigToStart;

    /**
     * If true, then we SCP the JAR files to each follower before starting them.
     */
    private final boolean scpJars;

    /**
     * Used by the Commander to SSH into {@link Follower} VMs.
     *
     * The Commander copies over configuration files and the latest .JAR file(s) before launching the followers.
     * (This feature is toggled by command-line arguments. The Commander does not copy anything over by default.)
     */
    private final JSch jsch;

    /**
     * If true, then we SCP the config file to each follower before starting them.
     */
    private final boolean scpConfig;

    /**
     * When true, Commander does not automatically launch followers. The user is expected to do it manually.
     * The commander will still copy over any specified files.
     */
    private final boolean manuallyLaunchFollowers;

    /**
     * Tracks if Followers disconnect during a benchmark, as we don't need to wait for as many results
     * if Followers disconnect in the middle of the benchmark (i.e., one less result per disconnected Follower).
     */
    private final AtomicInteger numDisconnections = new AtomicInteger(0);

    /**
     * The Follower VMs whom we are still waiting on for results.
     *
     * This is reset at the beginning/end of each trial of a particular benchmark.
     */
    private final Set<String> waitingOn = ConcurrentHashMap.newKeySet();

    private int maxNewSizeMb = 64000;
    private int maxHeapSizeGb = 96;
    private int minHeapSizeGb = 96;

    public static Commander getOrCreateCommander(String ip, int port, String yamlPath, boolean nondistributed,
                                                 boolean disableConsistency, int numFollowers,
                                                 boolean scpJars, boolean scpConfig,
                                                 boolean manuallyLaunchFollowers) throws IOException, JSchException {
        if (instance == null) {
            // serverlessLogLevel = logLevel;
            consistencyEnabled = !disableConsistency;
            instance = new Commander(ip, port, yamlPath, nondistributed,
                    numFollowers, scpJars, scpConfig, manuallyLaunchFollowers);
        }

        return instance;
    }

    private Commander(String ip, int port, String yamlPath, boolean nondistributed, int numFollowersFromConfigToStart,
                      boolean scpJars, boolean scpConfig, boolean manuallyLaunchFollowers)
            throws IOException, JSchException {
        this.ip = ip;
        this.port = port;
        this.nonDistributed = nondistributed;
        // TODO: Maybe do book-keeping or fault-tolerance here.
        this.followers = new ArrayList<>();
        this.resultQueues = new ConcurrentHashMap<>();
        this.workloadResponseQueue = new ArrayBlockingQueue<>(16);
        this.numFollowersFromConfigToStart = numFollowersFromConfigToStart;
        this.scpJars = scpJars;
        this.scpConfig = scpConfig;
        this.manuallyLaunchFollowers = manuallyLaunchFollowers;

        tcpServer = new Server(COMMANDER_TCP_BUFFER_SIZES, COMMANDER_TCP_BUFFER_SIZES) {
            @Override
            protected Connection newConnection() {
                LOG.debug(LEADER_PREFIX + " Creating new FollowerConnection.");
                return new FollowerConnection();
            }
        };

        tcpServer.addListener(new Listener.ThreadedListener(new ServerListener()));

        jsch = new JSch();

        // Only bother with identity file if we're running as Commander.
        // That keyfile isn't on any of the Follower VMs.
        if (!nondistributed) {
            jsch.addIdentity("/home/ubuntu/.ssh/id_rsa");
        }

        processConfiguration(yamlPath);
    }

    public static int log(int x, int base)
    {
        return (int) (Math.log(x) / Math.log(base));
    }

    /**
     * Process the configuration file for the benchmarking utility.
     */
    private void processConfiguration(String yamlPath) throws IOException {
        Yaml yaml = new Yaml();
        try (InputStream in = Files.newInputStream(Paths.get(yamlPath))) {
            LocalConfiguration config = yaml.loadAs(in, LocalConfiguration.class);

            LOG.info("Loaded configuration: " + config.toString());

            NAME_NODE_ENDPOINT = config.getNamenodeEndpoint();
            followerConfigs = config.getFollowers();
            hdfsConfigFilePath = config.getHdfsConfigFile();
            isServerless = config.getIsServerless();
            commanderExecutesToo = config.getCommanderExecutesToo();
            maxHeapSizeGb = config.getMaxHeapSizeGb();
            minHeapSizeGb = config.getMinHeapSizeGb();
            maxNewSizeMb = config.getMaxNewSizeMb();

            Commands.IS_SERVERLESS = isServerless;

            LOG.info("Loaded configuration!");
            LOG.info(String.valueOf(config));
        }
    }

    public void start() throws IOException, InterruptedException {
        if (!nonDistributed) {
            LOG.info("Commander is operating in DISTRIBUTED mode.");
            startServer();

            try {
                launchFollowers();
            } catch (IOException ex) {
                LOG.error("Encountered IOException while starting followers:", ex);
            } catch (JSchException ex) {
                LOG.error("Encountered JSchException while starting followers:", ex);
            }
        } else {
            LOG.info("Commander is operating in NON-DISTRIBUTED mode.");
        }
        Commands.TRACK_OP_PERFORMED = true;
        interactiveLoop();
    }

    private void executeCommand(String user, String host, String launchCommand, Session session, boolean disconnectSessionAfterCommand) {
        java.util.Properties sshConfig = new java.util.Properties();
        sshConfig.put("StrictHostKeyChecking", "no");

        try {
            if (session == null) {
                session = jsch.getSession(user, host, 22);
                session.setConfig(sshConfig);
                session.connect();
            }

            Channel channel = session.openChannel("exec");
            ((ChannelExec) channel).setCommand(launchCommand);
            channel.setInputStream(null);
            ((ChannelExec) channel).setErrStream(System.err);

            channel.connect();
            channel.disconnect();

            if (disconnectSessionAfterCommand)
                session.disconnect();
        } catch (JSchException e) {
            e.printStackTrace();
        }
    }

    private void launchFollower(String user, String host, String launchCommand) {
        java.util.Properties sshConfig = new java.util.Properties();
        sshConfig.put("StrictHostKeyChecking", "no");

        Session session;
        try {
            session = jsch.getSession(user, host, 22);
            session.setConfig(sshConfig);
            session.connect();

            ChannelSftp sftpChannel = (ChannelSftp) session.openChannel("sftp");
            sftpChannel.connect();

            if (scpJars) {
                LOG.info("SFTP-ing hadoop-hdfs-3.2.0.3-SNAPSHOT.jar to Follower " + host + ".");

                sftpChannel.put(HADOOP_HDFS_JAR_PATH, HADOOP_HDFS_JAR_PATH);
                LOG.info("SFTP-ing HopsFSBenchmark-1.0-jar-with-dependencies.jar to Follower " + host + ".");
                sftpChannel.put(BENCHMARK_JAR_PATH, BENCHMARK_JAR_PATH);
            }

            if (scpConfig) {
                LOG.info("SFTP-ing hdfs-site.xml to Follower " + host + ".");

                File f = new File(hdfsConfigFilePath);
                if (f.exists()) {
                    sftpChannel.put(hdfsConfigFilePath, hdfsConfigFilePath);
                    LOG.info("SFTP-ing mkdirWeakScaling to Follower " + host + ".");
                }
                else
                    LOG.error("Cannot SFTP '" + hdfsConfigFilePath + "' as it does not exist.");

                f = new File("/home/ubuntu/repos/HopsFS-Benchmarking-Utility/mkdirWeakScaling");
                if (f.exists()) {
                    sftpChannel.put("/home/ubuntu/repos/HopsFS-Benchmarking-Utility/mkdirWeakScaling", "/home/ubuntu/repos/HopsFS-Benchmarking-Utility/mkdirWeakScaling");
                    LOG.info("SFTP-ing log4j.properties to Follower " + host + " now.");
                }
                else
                    LOG.error("Cannot SFTP '/home/ubuntu/repos/HopsFS-Benchmarking-Utility/mkdirWeakScaling' as it does not exist.");

                String log4jPath = "/home/ubuntu/repos/HopsFS-Benchmarking-Utility/src/main/resources/log4j.properties";
                f = new File(log4jPath);
                if (f.exists()) {
                    sftpChannel.put(log4jPath, log4jPath);
                    LOG.debug("SFTP'd log4j.properties to Follower " + host + ".");
                }
                else
                    LOG.error("Cannot SFTP '" + log4jPath + "' as it does not exist.");

                LOG.info("SFTP-ing logback.xml to Follower " + host + " now.");
                String logbackPath = "/home/ubuntu/repos/HopsFS-Benchmarking-Utility/src/main/resources/logback.xml";
                f = new File(logbackPath);
                if (f.exists()) {
                    sftpChannel.put(logbackPath, logbackPath);
                    LOG.info("SFTP'd logback.xml to Follower " + host + ".");
                }
                else {
                    LOG.error("Cannot SFTP '" + logbackPath + "' as it does not exist.");
                }

                LOG.info("SFTP-ing workload.yaml to Follower " + host + " now.");
                String workloadYamlPath = "/home/ubuntu/repos/HopsFS-Benchmarking-Utility/workload.yaml";
                f = new File(workloadYamlPath);
                if (f.exists()) {
                    sftpChannel.put(workloadYamlPath, workloadYamlPath);
                    LOG.info("SFTP'd workload.yaml to Follower " + host + ".");
                }
                else {
                    LOG.error("Cannot SFTP '" + workloadYamlPath + "' as it does not exist.");
                }
            }

            if (!manuallyLaunchFollowers) {
                LOG.info("Explicitly starting follower on " + user + "@" + host + " with command: " + launchCommand);
                executeCommand(user, host, launchCommand, session, true);
            }
            else
                LOG.info("'Manually Launch Followers' is set to TRUE. Commander will not auto-launch Follower.");

            sftpChannel.disconnect();
        } catch (JSchException | SftpException e) {
            e.printStackTrace();
        }
    }

    /**
     * Using SSH, launch the follower processes.
     */
    private void launchFollowers() throws IOException, JSchException {
        final String launchCommand = String.format(LAUNCH_FOLLOWER_CMD, maxHeapSizeGb, minHeapSizeGb, maxNewSizeMb, ip, port);

        // If 'numFollowersFromConfigToStart' is negative, then use all followers.
        if (numFollowersFromConfigToStart < 0)
            numFollowersFromConfigToStart = followerConfigs.size();

        if (!scpJars && !scpConfig && manuallyLaunchFollowers) {
            LOG.info("No files to copy and we're launching followers manually.");
            return;
        }

        LOG.info("Starting and/or copying files to " + numFollowersFromConfigToStart + " follower(s) now...");

        for (int i = 0; i < numFollowersFromConfigToStart; i++) {
            FollowerConfig config = followerConfigs.get(i);

            // Don't kill Java processes if we're not auto-launching Followers. We might kill the user's process.
            if (!manuallyLaunchFollowers)
                executeCommand(config.getUser(), config.getIp(), "pkill -9 java", null, true);

            launchFollower(config.getUser(), config.getIp(), launchCommand);
        }
    }

    private void startServer() throws IOException {
        tcpServer.start();
        Network.register(tcpServer);
        tcpServer.bind(port, port+1);
    }

    /**
     * Stop the TCP server. Also sends 'STOP' commands to all the followers.
     */
    private void stopServer() {
        // TODO: Send 'STOP' commands to each follower.
        tcpServer.stop();
    }

    private void interactiveLoop() {
        LOG.info("Beginning execution as LEADER now.");

        primaryHdfs = initDfsClient(null, true);

        while (true) {
            updateGCMetrics();
            long startingGCs = numGarbageCollections;
            long startingGCTime = garbageCollectionTime;

            try {
                printMenu();
                int op = getNextOperation();

                switch (op) {
                    case OP_SET_COMMANDER_EXECUTES_TOO:
                        setCommanderExecutesToo();
                        break;
                    case OP_SET_HTTP_TCP_REPLACEMENT_CHANCE:
                        setHttpTcpReplacementChance();
                        break;
                    case OP_ESTABLISH_CONNECTIONS:
                        establishConnections();
                        break;
                    case OP_SAVE_LATENCIES_TO_FILE:
                        saveLatenciesToFile();
                        break;
                    case OP_TOGGLE_BENCHMARK_MODE:
                        toggleBenchmarkMode();
                        break;
                    case OP_TOGGLE_OPS_PERFORMED_FOLLOWERS:
                        toggleOperationsPerformedInFollowers();
                        break;
                    case OP_TRIGGER_CLIENT_GC:
                        performClientVMGarbageCollection();
                        break;
                    case OP_CHANGE_POST_TRIAL_SLEEP:
                        printAndModifyPostTrialSleep();
                        break;
                    case OP_GET_ACTIVE_NAMENODES:
                        Commands.getActiveNameNodesOperation(primaryHdfs);
                        break;
                    case OP_SET_CONSISTENCY_PROTOCOL_ENABLED:
                        handleSetConsistencyProtocolEnabled();
                        break;
                    case OP_SET_LOG_LEVEL:
                        handleSetLogLevel();
                        break;
                    case OP_CLEAR_METRIC_DATA:
                        LOG.info("Clearing metric data (including latencies) now...");
                        Commands.clearMetricData(primaryHdfs);

                        if (!nonDistributed) {
                            JsonObject payload = new JsonObject();
                            String operationId = UUID.randomUUID().toString();
                            payload.addProperty(OPERATION, OP_CLEAR_METRIC_DATA);
                            payload.addProperty(OPERATION_ID, operationId);

                            issueCommandToFollowers("Clear Metric Data", operationId, payload, false);
                        }

                        break;
                    case OP_WRITE_STATISTICS:
                        if (!isServerless) {
                            LOG.error("Writing statistics packages is not supported by Vanilla HopsFS!");
                            continue;
                        }

                        LOG.info("Writing statistics packages to files...");
                        LOG.info("");
                        primaryHdfs.dumpStatisticsPackages(true);
                        break;
                    case OP_PRINT_OPS_PERFORMED:
                        LOG.info("Printing operations performed...");
                        LOG.info("");
                        Commands.printOperationsPerformed(primaryHdfs);
                        break;
                    case OP_PRINT_TCP_DEBUG:
                        if (!isServerless) {
                            LOG.error("Printing TCP debug information operation is not supported by Vanilla HopsFS!");
                            return;
                        }

                        LOG.info("Printing TCP debug information...");
                        LOG.info("");
                        int numConnections = primaryHdfs.printDebugInformation();

                        LOG.info("Total number of active TCP/UDP connections: " + numConnections);

                        break;
                    case OP_EXIT:
                        LOG.info("Exiting now... goodbye!");
                        try {
                            primaryHdfs.close();
                        } catch (IOException ex) {
                            LOG.info("Encountered exception while closing file system...");
                            ex.printStackTrace();
                        }
                        stopServer();
                        System.exit(0);
                    case OP_CREATE_FILE:
                        LOG.info("CREATE FILE selected!");
                        Commands.createFileOperation(primaryHdfs);
                        break;
                    case OP_MKDIR:
                        LOG.info("MAKE DIRECTORY selected!");
                        Commands.mkdirOperation(primaryHdfs);
                        break;
                    case OP_READ_FILE:
                        LOG.info("READ FILE selected!");
                        Commands.readOperation(primaryHdfs);
                        break;
                    case OP_RENAME:
                        LOG.info("RENAME selected!");
                        Commands.renameOperation(primaryHdfs);
                        break;
                    case OP_DELETE:
                        LOG.info("DELETE selected!");
                        Commands.deleteOperation(primaryHdfs);
                        break;
                    case OP_LIST:
                        LOG.info("LIST selected!");
                        Commands.listOperation(primaryHdfs);
                        break;
                    case OP_APPEND:
                        LOG.info("APPEND selected!");
                        Commands.appendOperation(primaryHdfs);
                        break;
                    case OP_CREATE_SUBTREE:
                        LOG.info("CREATE SUBTREE selected!");
                        Commands.createSubtree(primaryHdfs);
                        break;
                    case OP_PING:
                        LOG.info("PING selected!");
                        Commands.pingOperation(primaryHdfs);
                        break;
                    case OP_PREWARM:
                        LOG.info("PREWARM selected!");
                        Commands.prewarmOperation(primaryHdfs);
                        break;
                    case OP_WRITE_FILES_TO_DIR:
                        LOG.info("WRITE FILES TO DIRECTORY selected!");
                        Commands.writeFilesToDirectory(primaryHdfs);
                        break;
                    case OP_READ_FILES:
                        LOG.info("READ FILES selected!");
                        Commands.readFilesOperation(primaryHdfs, OP_READ_FILES);
                        break;
                    case OP_DELETE_FILES:
                        LOG.info("DELETE FILES selected!");
                        Commands.deleteFilesOperation(primaryHdfs);
                        break;
                    case OP_WRITE_FILES_TO_DIRS:
                        LOG.info("WRITE FILES TO DIRECTORIES selected!");
                        Commands.writeFilesToDirectories(primaryHdfs);
                        break;
                    case OP_WEAK_SCALING_READS:
                        LOG.info("'Read n Files with n Threads (Weak Scaling - Read)' selected!");
                        weakScalingReadOperation(primaryHdfs);
                        break;
                    case OP_STRONG_SCALING_READS:
                        LOG.info("'Read n Files y Times with z Threads (Strong Scaling - Read)' selected!");
                        strongScalingReadOperation(primaryHdfs);
                        break;
                    case OP_WEAK_SCALING_WRITES:
                        LOG.info("'Write n Files with n Threads (Weak Scaling - Write)' selected!");
                        weakScalingWriteOperation(primaryHdfs);
                        break;
                    case OP_STRONG_SCALING_WRITES:
                        LOG.info("'Write n Files y Times with z Threads (Strong Scaling - Write)' selected!");
                        strongScalingWriteOperation(primaryHdfs);
                        break;
                    case OP_CREATE_DIRECTORIES:
                        LOG.info("CREATE N DIRECTORIES ONE-AFTER-ANOTHER selected!");
                        Commands.createNDirectories(primaryHdfs);
                        break;
                    case OP_WEAK_SCALING_READS_V2:
                        LOG.info("WeakScalingReadsV2 Selected!");
                        weakScalingReadOperationV2(primaryHdfs);
                        break;
                    case OP_GET_FILE_STATUS:
                        LOG.info("OP_GET_FILE_STATUS selected!");
                        Commands.getFileStatusOperation(primaryHdfs, NAME_NODE_ENDPOINT);
                        break;
                    case OP_LIST_DIRECTORIES_FROM_FILE:
                        LOG.info("LIST DIRECTORIES FROM FILE selected!");
                        listDirectoriesFromFile(primaryHdfs);
                        break;
                    case OP_STAT_FILES_WEAK_SCALING:
                        LOG.info("STAT FILES WEAK SCALING selected!");
                        statFilesWeakScaling(primaryHdfs);
                        break;
                    case OP_MKDIR_WEAK_SCALING:
                        LOG.info("MKDIR WEAK SCALING selected!");
                        mkdirWeakScaling(primaryHdfs);
                        break;
                    case OP_PREPARE_GENERATED_WORKLOAD:
                        LOG.info("Randomly-Generated Workload selected!");
                        randomlyGeneratedWorkload(primaryHdfs);
                        break;
                    case OP_CREATE_FROM_FILE:
                        LOG.info("CREATE FROM FILE selected!");
                        createFromFile();
                        break;
                    case OP_READER_WRITER_TEST_1:
                        LOG.info("READER WRITER TEST #1 selected!");
                        readerWriterTest1();
                        break;
                    default:
                        LOG.info("ERROR: Unknown or invalid operation specified: " + op);
                        break;
                }
            } catch (Exception ex) {
                LOG.error("Exception encountered:", ex);
            }

            updateGCMetrics();
            long numGCsPerformedDuringLastOp = numGarbageCollections - startingGCs;
            long timeSpentInGCDuringLastOp = garbageCollectionTime - startingGCTime;

            LOG.debug("Performed " + numGCsPerformedDuringLastOp + " garbage collection(s) during last operation.");
            if (numGCsPerformedDuringLastOp > 0)
                LOG.debug("Spent " + timeSpentInGCDuringLastOp + " ms garbage collecting during the last operation.");
        }
    }

    private void setCommanderExecutesToo() {
        if (commanderExecutesToo) {
            System.out.println("Currently, the Commander DOES execute tasks/jobs too.");
        } else {
            System.out.println("Currently, the Commander does NOT execute tasks/jobs too.");
        }

        System.out.print("Should the Commander execute tasks/jobs? Enter nothing to retain the current value.\n> ");
        String input = scanner.nextLine().trim();

        if (input.length() == 0) {
            System.out.println("Retaining current value (" + commanderExecutesToo + ").");
            return;
        }

        commanderExecutesToo = input.equalsIgnoreCase("y") || input.equalsIgnoreCase("yes") ||
                input.equalsIgnoreCase("t") || input.equalsIgnoreCase("true") ||
                input.equalsIgnoreCase("1");

        System.out.println("Updated value: " + commanderExecutesToo);
    }

    private void readerWriterTest1() throws InterruptedException, FileNotFoundException {
        int numReaders = getIntFromUser("How many reader threads?");
        int numWriters = getIntFromUser("How many writer threads?");
        int numThreads = numReaders + numWriters;

        boolean startReadersFirst = getBooleanFromUser("Start readers first?");

        int choice = getIntFromUser("(1) Create new files for reading? (2) Use existing files?");
        if (choice < 1 || choice > 2)
            throw new IllegalStateException("Choice must be 1 or 2.");

        List<String> readerFiles;
        if (choice == 1) {
            int numFilesToRead = getIntFromUser("How many files should be in the pool from which the readers read?");
            String readPath = getStringFromUser("What directory should the readers target?");

            boolean readPathExists = Commands.exists(primaryHdfs, readPath);

            if (!readPathExists) {
                LOG.info("Creating read directory '" + readPath + "' now...");
                Commands.mkdir(readPath, primaryHdfs);
            }

            LOG.info("Creating files to be read by readers now...");
            String[] fileNames = Utils.getFixedLengthRandomStrings(numFilesToRead, 8);
            readerFiles = new ArrayList<>(); // Successfully-created files.
            long createStart = System.currentTimeMillis();
            for (String fileName : fileNames) {
                String fullPath = readPath + "/" + fileName;
                boolean created = FSOperation.CREATE_FILE.call(primaryHdfs, fullPath, "");

                if (created)
                    readerFiles.add(fullPath);
            }
            LOG.info("Created " + readerFiles.size() + " files in " + (System.currentTimeMillis() - createStart) + " ms.");
        } else {
            String path = getStringFromUser("Please enter path to existing file.");
            LOG.info("Reading files from path '" + path + "' now...");
            readerFiles = Utils.getFilePathsFromFile(path);

            if (readerFiles.size() == 0)
                throw new IllegalStateException("Read 0 paths from file '" + path + "'");
        }

        String writePath = getStringFromUser("What directory should the writes target?");

        int durSec = getIntFromUser("How long should operations be performed (in seconds)?");
        int durationMilliseconds = durSec * 1000;

        boolean writePathExists = Commands.exists(primaryHdfs, writePath);

        if (!writePathExists) {
            LOG.info("Creating write directory '" + writePath + "' now...");
            Commands.mkdir(writePath, primaryHdfs);
        }

        String[] writerBaseFileNames = Utils.getFixedLengthRandomStrings(numWriters, 8);
        for (int i = 0; i < writerBaseFileNames.length; i++) {
            writerBaseFileNames[i] = writePath + "/" + writerBaseFileNames[i];
        }

        List<Thread> readers = new ArrayList<>();
        List<Thread> writers = new ArrayList<>();

        // Used to synchronize threads; they each connect to HopsFS and then
        // count down. So, they all cannot start until they are all connected.
        final CountDownLatch startLatch = new CountDownLatch(numThreads + 1);

        // Used to synchronize threads; they block when they finish executing to avoid using CPU cycles
        // by aggregating their results. Once all the threads have finished, they aggregate their results.
        final CountDownLatch endLatch = new CountDownLatch(numThreads);
        final Semaphore readerReadySemaphore = new Semaphore((numReaders * -1) + 1);
        final Semaphore readySemaphore = new Semaphore((numThreads * -1) + 1);
        final Semaphore endSemaphore = new Semaphore((numThreads * -1) + 1);

        final BlockingQueue<List<OperationPerformed>> operationsPerformed =
                new java.util.concurrent.ArrayBlockingQueue<>(numThreads);
        final SynchronizedDescriptiveStatistics latencyHttp = new SynchronizedDescriptiveStatistics();
        final SynchronizedDescriptiveStatistics latencyTcp = new SynchronizedDescriptiveStatistics();
        final SynchronizedDescriptiveStatistics latencyBoth = new SynchronizedDescriptiveStatistics();

        // Keep track of number of successful operations.
        AtomicInteger numSuccessfulOps = new AtomicInteger(0);
        AtomicInteger numOps = new AtomicInteger(0);

        for (int i = 0; i < numReaders; i++) {
            int threadId = i;
            Thread readerThread = new Thread(() -> {
                LOG.info("Reader thread " + threadId + " started.");
                DistributedFileSystem hdfs = Commands.getHdfsClient(primaryHdfs, false);

                if (startReadersFirst)
                    readerReadySemaphore.release();

                readySemaphore.release(); // Ready to start. Once all threads have done this, the timer begins.

                startLatch.countDown(); // Wait for the main thread's signal to actually begin.

                try {
                    startLatch.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                int numSuccessfulOpsCurrentThread = 0;
                int numOpsCurrentThread = 0;

                Random random = new Random();
                long start = System.currentTimeMillis();
                while (System.currentTimeMillis() - start < durationMilliseconds) {
                    for (int k = 0; k < 5000; k++) {
                        int idx = random.nextInt(readerFiles.size());
                        boolean success = FSOperation.READ_FILE.call(hdfs, readerFiles.get(idx), null);
                        if (success)
                            numSuccessfulOpsCurrentThread++;
                        numOpsCurrentThread++;

                        if (System.currentTimeMillis() - start >= durationMilliseconds)
                            break;
                    }

                    LOG.info("Reader " + threadId + " has completed " + numOpsCurrentThread + " ops.");
                }

                // This way, we don't have to wait for all the statistics to be added to lists and whatnot.
                // As soon as the threads finish, they call release() on the endSemaphore. Once all threads have
                // done this, we designate the benchmark as ended and record the stop time. Then we join the threads
                // so that all the statistics are placed into the appropriate collections where we can aggregate them.
                endSemaphore.release();

                endLatch.countDown();

                try {
                    endLatch.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                numSuccessfulOps.addAndGet(numSuccessfulOpsCurrentThread);
                numOps.addAndGet(numOpsCurrentThread);

                if (!BENCHMARKING_MODE) {
                    operationsPerformed.add(hdfs.getOperationsPerformed());
                }

                for (double latency : hdfs.getLatencyHttpStatistics().getValues()) {
                    latencyHttp.addValue(latency);
                    latencyBoth.addValue(latency);
                }

                for (double latency : hdfs.getLatencyTcpStatistics().getValues()) {
                    latencyTcp.addValue(latency);
                    latencyBoth.addValue(latency);
                }

                // First clear the metric data associated with the client.
                Commands.clearMetricDataNoPrompt(hdfs);

                try {
                    // Now return the client to the pool so that it can be used again in the future.
                    Commands.returnHdfsClient(hdfs);
                } catch (InterruptedException e) {
                    LOG.error("Encountered error when trying to return HDFS client. Closing it instead.");
                    e.printStackTrace();

                    try {
                        hdfs.close();

                    } catch (IOException ex) {
                        LOG.error("Encountered IOException while closing DistributedFileSystem object:", ex);
                    }
                }
            });
            readers.add(readerThread);
        }

        if (startReadersFirst) {
            for (Thread reader : readers)
                reader.start();

            LOG.info("Starting READER threads first.");
            readerReadySemaphore.acquire();
            LOG.info("All READER threads have started (first). Next, starting the WRITER threads.");
        }

        for (int i = 0; i < numWriters; i++) {
            int threadId = i;
            String baseFileName = writerBaseFileNames[i];
            Thread writerThread = new Thread(() -> {
                LOG.info("Writer thread " + threadId + " started.");
                int numFilesCreated = 0;
                DistributedFileSystem hdfs = Commands.getHdfsClient(primaryHdfs, false);

                readySemaphore.release(); // Ready to start. Once all threads have done this, the timer begins.

                startLatch.countDown(); // Wait for the main thread's signal to actually begin.

                try {
                    startLatch.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                int numSuccessfulOpsCurrentThread = 0;
                int numOpsCurrentThread = 0;

                long start = System.currentTimeMillis();
                while (System.currentTimeMillis() - start < durationMilliseconds) {
                    for (int k = 0; k < 1000; k++) {
                        String filePath = baseFileName + "-" + numFilesCreated++;
                        boolean success = FSOperation.CREATE_FILE.call(hdfs, filePath, "");
                        if (success)
                            numSuccessfulOpsCurrentThread++;
                        numOpsCurrentThread++;

                        if (System.currentTimeMillis() - start >= durationMilliseconds)
                            break;
                    }

                    LOG.info("Writer " + threadId + " has completed " + numOpsCurrentThread + " ops.");
                }

                // This way, we don't have to wait for all the statistics to be added to lists and whatnot.
                // As soon as the threads finish, they call release() on the endSemaphore. Once all threads have
                // done this, we designate the benchmark as ended and record the stop time. Then we join the threads
                // so that all the statistics are placed into the appropriate collections where we can aggregate them.
                endSemaphore.release();

                endLatch.countDown();

                try {
                    endLatch.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                numSuccessfulOps.addAndGet(numSuccessfulOpsCurrentThread);
                numOps.addAndGet(numOpsCurrentThread);

                if (!BENCHMARKING_MODE) {
                    operationsPerformed.add(hdfs.getOperationsPerformed());
                }

                for (double latency : hdfs.getLatencyHttpStatistics().getValues()) {
                    latencyHttp.addValue(latency);
                    latencyBoth.addValue(latency);
                }

                for (double latency : hdfs.getLatencyTcpStatistics().getValues()) {
                    latencyTcp.addValue(latency);
                    latencyBoth.addValue(latency);
                }

                // First clear the metric data associated with the client.
                Commands.clearMetricDataNoPrompt(hdfs);

                try {
                    // Now return the client to the pool so that it can be used again in the future.
                    Commands.returnHdfsClient(hdfs);
                } catch (InterruptedException e) {
                    LOG.error("Encountered error when trying to return HDFS client. Closing it instead.");
                    e.printStackTrace();

                    try {
                        hdfs.close();

                    } catch (IOException ex) {
                        LOG.error("Encountered IOException while closing DistributedFileSystem object:", ex);
                    }
                }
            });
            writers.add(writerThread);
        }

        if (startReadersFirst) {
            // If we started the readers first, then just start the writers.
            for (Thread writer : writers)
                writer.start();

            LOG.info("Started all WRITER threads (after READER threads).");
        } else {
            // Otherwise, start them in an interleaved fashion (reader, writer, reader, writer).
            // If there are an unequal number of readers and writers, then they are started in an
            // interleaved fashion until only one type of thread remains to be started, at which
            // point all remaining threads of that type are started.
            int readerIdx = 0;
            int writerIdx = 0;
            while (readerIdx < numReaders || writerIdx < numWriters) {
                if (readerIdx < numReaders) {
                    readers.get(readerIdx).start();
                }
                if (writerIdx < numWriters) {
                    writers.get(writerIdx).start();
                }

                readerIdx++;
                writerIdx++;
            }

            LOG.info("Started READER and WRITER threads in an interleaved fashion.");
        }
        LOG.info("Starting Reader-Writer test now...");
        readySemaphore.acquire();                   // Will block until all client threads are ready to go.
        long start = System.currentTimeMillis();    // Start the clock.
        startLatch.countDown();                     // Let the threads start.

        // This way, we don't have to wait for all the statistics to be added to lists and whatnot.
        // As soon as the threads finish, they call release() on the endSemaphore. Once all threads have
        // done this, we designate the benchmark as ended and record the stop time. Then we join the threads
        // so that all the statistics are placed into the appropriate collections where we can aggregate them.
        endSemaphore.acquire();
        long end = System.currentTimeMillis();

        LOG.info("Benchmark completed in " + (end - start) + "ms. Joining the " +
                (readers.size() + writers.size()) + " threads now...");
        for (Thread thread : readers)
            thread.join();
        for (Thread thread : writers)
            thread.join();

        List<OperationPerformed> allOperationsPerformed = new ArrayList<>();
        Pair<Integer, Integer> cacheHitsAndMisses = Commands.mergeMetricInformation(primaryHdfs, operationsPerformed, allOperationsPerformed);
        // statisticsPackages, transactionEvents, allOperationsPerformed);
        int totalCacheHits = cacheHitsAndMisses.getFirst();
        int totalCacheMisses = cacheHitsAndMisses.getSecond();

        double durationSeconds = (end - start) / 1.0e3;

        // TODO: Verify that I've calculated the total number of operations correctly.
        int numSuccess = numSuccessfulOps.get();
        double totalOperations = numOps.get();
        LOG.info("Finished performing all " + totalOperations + " operations in " + durationSeconds + " sec.");
        double totalThroughput = totalOperations / durationSeconds;
        double successThroughput = numSuccess / durationSeconds;
        LOG.info("Number of successful operations: " + numSuccess);
        LOG.info("Number of failed operations: " + (totalOperations - numSuccess));
        LOG.info("Cache Hits: " + totalCacheHits + ", Cache Misses: " + totalCacheMisses);
        if (totalCacheHits > 0 || totalCacheMisses > 0)
            LOG.info("Cache Hit Rate: " + ((double)totalCacheHits / (totalCacheHits + totalCacheMisses)));

        printLatencyStatistics(latencyBoth, latencyTcp, latencyHttp);
        primaryHdfs.addLatencies(latencyTcp.getValues(), latencyHttp.getValues());
        LOG.info("Total Throughput: " + totalThroughput + " ops/sec.");
        LOG.info("Successful Throughput: " + successThroughput + " ops/sec.");

        if (LOG.isDebugEnabled())
            LOG.debug("At end of benchmark, the HDFS Clients Cache has " + hdfsClients.size() + " clients.");
    }

    /**
     * Create files and directories that are listed in a file.
     *
     * Directories should end with '/' in the specified file; otherwise, they will be treated as files.
     *
     * The directories along the path of a file should probably already be created before the file is created,
     * so make sure that the order of the paths in the specified file satisfies this requirement.
     */
    private void createFromFile() throws FileNotFoundException {
        System.out.print("Please specify path to file containing fully-qualified paths of dirs to be created:\n> ");
        String path = scanner.nextLine();

        checkForExit(path);

        List<String> filesAndDirectories = Utils.getFilePathsFromFile(path);

        int numDirsCreated = 0;
        long start = System.currentTimeMillis();

        for (String fileOrDir : filesAndDirectories) {
            boolean success;
            if (fileOrDir.endsWith("/"))
                success = Commands.mkdir(fileOrDir, primaryHdfs);
            else
                success = Commands.createFile(fileOrDir, "", primaryHdfs);

            if (success)
                numDirsCreated++;
        }

        LOG.debug("Successfully created " + numDirsCreated + "/" + filesAndDirectories.size() +
                " files/directories in " + (System.currentTimeMillis() - start) + " ms.");
    }

    private void randomlyGeneratedWorkload(final DistributedFileSystem sharedHdfs)
            throws IOException, InterruptedException, ExecutionException {
        System.out.print("Please enter location of workload config file (enter nothing to default to ./workload.yaml):\n> ");
        String workloadConfigFile = scanner.nextLine();

        // Default to "./workload.yaml"
        if (workloadConfigFile.equals(""))
            workloadConfigFile = "./workload.yaml";

        System.out.println("Attempting to load workload from file: '" + workloadConfigFile + "'");

        BMConfiguration configuration = new BMConfiguration(workloadConfigFile);

        String operationId = UUID.randomUUID().toString();
        JsonObject payload = new JsonObject();
        payload.addProperty(OPERATION, OP_PREPARE_GENERATED_WORKLOAD);
        payload.addProperty(OPERATION_ID, operationId);
        payload.addProperty("NUM_FOLLOWERS", followers.size() + 1); // Add one to include self.

        try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
            ObjectOutputStream out;
            out = new ObjectOutputStream(bos);
            out.writeObject(configuration);
            out.flush();
            byte[] configAsBytes = bos.toByteArray();
            String base64 = Base64.getEncoder().encodeToString(configAsBytes);
            payload.addProperty("configuration", base64);
        }

        waitingOn.clear();
        workloadResponseQueue.clear();

        int expectedNumResponses = followers.size();

        LOG.info("Telling Followers to prepare for Random Workload " + operationId);
        issueCommandToFollowers("Prepare for Random Workload", operationId, payload, true);

        RandomlyGeneratedWorkload workload =
                new RandomlyGeneratedWorkload(configuration,
                        sharedHdfs, followers.size() + 1); // Add one to include self.

        int counter = 0;
        long time = System.currentTimeMillis();

        while (waitingOn.size() > 0) {
            LOG.debug("Still waiting on the following Followers: " + StringUtils.join(waitingOn, ", "));
            Thread.sleep(1000);

            int numDisconnects = numDisconnections.getAndSet(0);

            if (numDisconnects > 0) {
                LOG.warn("There has been at least one disconnection.");
            }

            counter += (System.currentTimeMillis() - time);
            time = System.currentTimeMillis();
            if (counter >= 60000) {
                boolean decision = getBooleanFromUser("Stop waiting early?");

                if (decision) {
                    // We won't be waiting on these anymore.
                    expectedNumResponses -= waitingOn.size();
                    break;
                }

                counter = 0;
            }
        }

        if (workloadResponseQueue.size() < expectedNumResponses)
            LOG.error("Expected " + expectedNumResponses + " response(s). Got " + workloadResponseQueue.size());

        boolean errorOccurred = false;
        for (WorkloadResponse resp : workloadResponseQueue) {
            if (resp.erred) {
                LOG.error("Follower encountered error while creating workload...");
                errorOccurred = true;
                break;
            }
        }

        if (errorOccurred) {
            payload = new JsonObject();
            payload.addProperty(OPERATION_ID, operationId);
            payload.addProperty(OPERATION, OP_ABORT_RANDOM_WORKLOAD);
            LOG.error("Aborting workload.");
            issueCommandToFollowers("Abort Random Workload", operationId, payload, false);
            return;
        }

        LOG.info("Continuing to warm-up stage for random workload " + operationId + " now...");
        waitingOn.clear();
        workloadResponseQueue.clear();

        payload = new JsonObject();
        payload.addProperty(OPERATION_ID, operationId);
        payload.addProperty(OPERATION, OP_DO_WARMUP_FOR_PREPARED_WORKLOAD);
        issueCommandToFollowers("Abort Random Workload", operationId, payload, true);

        workload.doWarmup();

        counter = 0;
        time = System.currentTimeMillis();
        while (waitingOn.size() > 0) {
            LOG.debug("Still waiting on the following Followers: " + StringUtils.join(waitingOn, ", "));
            Thread.sleep(1000);

            int numDisconnects = numDisconnections.getAndSet(0);

            if (numDisconnects > 0) {
                LOG.warn("There has been at least one disconnection.");
            }

            counter += (System.currentTimeMillis() - time);
            time = System.currentTimeMillis();
            if (counter >= 60000) {
                boolean decision = getBooleanFromUser("Stop waiting early?");

                if (decision) {
                    // We won't be waiting on these anymore.
                    expectedNumResponses -= waitingOn.size();
                    break;
                }

                counter = 0;
            }
        }

        if (workloadResponseQueue.size() < expectedNumResponses)
            LOG.error("Expected " + expectedNumResponses + " response(s). Got " + workloadResponseQueue.size());

        for (WorkloadResponse resp : workloadResponseQueue) {
            if (resp.erred) {
                LOG.error("Follower encountered error while warming-up workload...");
                errorOccurred = true;
                break;
            }
        }

        if (errorOccurred) {
            payload = new JsonObject();
            payload.addProperty(OPERATION_ID, operationId);
            payload.addProperty(OPERATION, OP_ABORT_RANDOM_WORKLOAD);
            LOG.error("Aborting workload.");
            issueCommandToFollowers("Abort Random Workload", operationId, payload, false);
            return;
        }

        // Do the actual workload.
        LOG.info("Preparing to do random workload " + operationId + " now...");
        waitingOn.clear();
        workloadResponseQueue.clear();

        BlockingQueue<DistributedBenchmarkResult> resultQueue = null;
        if (expectedNumResponses > 0) {
            resultQueue = new ArrayBlockingQueue<>(expectedNumResponses);
            resultQueues.put(operationId, resultQueue);
        }

        payload = new JsonObject();
        payload.addProperty(OPERATION_ID, operationId);
        payload.addProperty(OPERATION, OP_DO_RANDOM_WORKLOAD);
        issueCommandToFollowers("Execute Random Workload", operationId, payload, true);

        DistributedBenchmarkResult localResult = workload.doWorkload(operationId);

        counter = 0;
        time = System.currentTimeMillis();
        while (waitingOn.size() > 0) {
            LOG.debug("Still waiting on the following Followers: " + StringUtils.join(waitingOn, ", "));
            Thread.sleep(1000);

            int numDisconnects = numDisconnections.getAndSet(0);

            if (numDisconnects > 0) {
                LOG.warn("There has been at least one disconnection.");
            }

            counter += (System.currentTimeMillis() - time);
            time = System.currentTimeMillis();
            if (counter >= 60000) {
                boolean decision = getBooleanFromUser("Stop waiting early?");

                if (decision) {
                    // We won't be waiting on these anymore.
                    expectedNumResponses -= waitingOn.size();
                    break;
                }

                counter = 0;
            }
        }

        if (localResult.opsPerformed != null)
            primaryHdfs.addOperationPerformeds(localResult.opsPerformed);

        primaryHdfs.addLatencies(localResult.tcpLatencyStatistics.getValues(),
                localResult.httpLatencyStatistics.getValues());

        if (localResult.txEvents != null) {
            primaryHdfs.mergeTransactionEvents(localResult.txEvents, true);
        }

        // In theory, calling `extractDistributedResultFromQueue` when there's only a local result should work.
        // We'll use the data from the local result, then skip the loop where we extract distributed results
        // from the queue, and so we'll ultimately just return an AggregatedResult created from only the local result.
        AggregatedResult aggregatedResult = extractDistributedResultFromQueue(
                resultQueues.getOrDefault(operationId, new LinkedBlockingQueue<>()), localResult);
//        if (expectedNumResponses < 1) {
//            LOG.info("The number of distributed results is 1. We have nothing to wait for.");
//            String metricsString = "";
//
//            DecimalFormat df = new DecimalFormat("#.####");
//            double avgTcpLatency;
//            if (localResult.tcpLatencyStatistics != null)
//                avgTcpLatency = localResult.tcpLatencyStatistics.getMean();
//            else
//                avgTcpLatency = -1;
//
//            double avgHttpLatency;
//            if (localResult.httpLatencyStatistics != null)
//                avgHttpLatency = (localResult.httpLatencyStatistics.getN() > 0) ? localResult.httpLatencyStatistics.getMean() : 0;
//            else
//                avgHttpLatency = -1;
//
//            double avgLatency = avgTcpLatency + avgHttpLatency / 2.0;
//            double cacheHitRate = ((double)localResult.cacheHits / (double)(localResult.cacheHits + localResult.cacheMisses));
//            // throughput (ops/sec), cache hits, cache misses, cache hit rate, avg tcp latency, avg http latency, avg combined latency
//            metricsString = String.format("%s %d %d %s %s %s %s", df.format(localResult.getOpsPerSecond()),
//                    localResult.cacheHits, localResult.cacheMisses,
//                    df.format(cacheHitRate),
//                    df.format(avgTcpLatency),
//                    df.format(avgHttpLatency),
//                    df.format(avgLatency));
//
//            aggregatedResult = new AggregatedResult(localResult.getOpsPerSecond(), localResult.cacheHits,
//                    localResult.cacheMisses, metricsString, avgTcpLatency, avgHttpLatency, avgLatency,
//                    localResult.durationSeconds);
//        } else {
//            LOG.info("Expecting " + expectedNumResponses + " distributed results.");
//            aggregatedResult = extractDistributedResultFromQueue(resultQueues.get(operationId), localResult);
//        }

        System.out.println("throughput (ops/sec), cache hits, cache misses, cache hit rate, avg tcp latency, avg http latency, avg combined latency");
        System.out.println(aggregatedResult.metricsString);

        DecimalFormat df = new DecimalFormat("#.####");
        System.out.println(aggregatedResult.toString(df));

        String outputDirectory = "./random_workload_data/" + operationId;
        File dir = new File(outputDirectory);
        dir.mkdirs();

        // Write workload summary to a file.
        try (Writer writer = new BufferedWriter(
                new OutputStreamWriter(new FileOutputStream(dir + "/summary.dat"), StandardCharsets.UTF_8))) {
            writer.write(aggregatedResult.toString());
            writer.write("\n");
            writer.write(aggregatedResult.toString(df));
        }

        Utils.writeRandomWorkloadResultsToFile(outputDirectory, primaryHdfs.getOperationsPerformed());
    }

    private void mkdirWeakScaling(final DistributedFileSystem sharedHdfs) throws IOException, InterruptedException {
        int directoryChoice = getIntFromUser("Should the threads create directories within the " +
                "SAME DIRECTORY [1], DIFFERENT DIRECTORIES [2], or \"RANDOM MKDIRs\" [3]?");

        // Validate input.
        if (directoryChoice < 1 || directoryChoice > 3) {
            LOG.error("Invalid argument specified. Should be \"1\" for same directory, \"2\" for different directories. " +
                    "Or \"3\" for random MKDIRs. Instead, got \"" + directoryChoice + "\"");
            return;
        }

        int dirInputMethodChoice = getIntFromUser("Manually input (comma-separated list) [1], " +
                "or specify file containing directories [2]?");

        List<String> directories;
        if (dirInputMethodChoice == 1) {
            System.out.print("Please enter the directories as a comma-separated list:\n> ");
            String listOfDirectories = scanner.nextLine();
            directories = Arrays.asList(listOfDirectories.split(","));

            if (directories.size() == 1)
                LOG.info("1 directory specified.");
            else
                LOG.info(directories.size() + " directories specified.");
        }
        else if (dirInputMethodChoice == 2) {
            System.out.print("Please provide path to file containing HopsFS directories:\n> ");
            String filePath = scanner.nextLine();
            directories = Utils.getFilePathsFromFile(filePath);

            if (directories.size() == 1)
                LOG.info("1 directory specified in file.");
            else
                LOG.info(directories.size() + " directories specified in file.");
        }
        else {
            LOG.error("Invalid option specified (" + dirInputMethodChoice +
                    "). Please enter \"1\" or \"2\" for this prompt.");
            return;
        }

        System.out.print("Number of threads? \n> ");
        int numberOfThreads = Integer.parseInt(scanner.nextLine());

        if (directoryChoice == 1) {
            Random rng = new Random();
            int idx = rng.nextInt(directories.size());
            String dir = directories.get(idx);
            directories = new ArrayList<>(numberOfThreads);
            for (int i = 0; i < numberOfThreads; i++)
                directories.add(dir); // This way, they'll all write to the same directory. We can reuse old code.
        } else if (directoryChoice == 2) {
            Collections.shuffle(directories);
            directories = directories.subList(0, numberOfThreads);
        } // If neither of the above are true, then we use the entire directories list.
        //   We'll generate a bunch of random writes using the full list.

        System.out.print("Number of directories to be created per thread? \n> ");
        int mkdirsPerThread = Integer.parseInt(scanner.nextLine());

        int numTrials = getIntFromUser("How many trials would you like to perform?");

        if (numTrials <= 0)
            throw new IllegalArgumentException("The number of trials should be at least 1.");

        boolean writePathsToFile = getBooleanFromUser("Write HopsFS directory paths to file?");

        JsonObject payload = new JsonObject();
        payload.addProperty(OPERATION, OP_MKDIR_WEAK_SCALING);
        payload.addProperty("n", mkdirsPerThread);
        payload.addProperty("numberOfThreads", numberOfThreads);
        payload.addProperty("randomMkdirs", directoryChoice == 3);
        payload.addProperty("writePathsToFile", writePathsToFile);

        JsonArray directoriesJson = new JsonArray();
        for (String dir : directories)
            directoriesJson.add(dir);

        payload.add("directories", directoriesJson);

        performDistributedBenchmark(sharedHdfs, numTrials, payload, numberOfThreads, mkdirsPerThread, null,
                false, OP_MKDIR_WEAK_SCALING, directories, "Weak Scaling (MKDIR)",
                new DistributedBenchmarkOperation() {
                    @Override
                    public DistributedBenchmarkResult call(DistributedFileSystem sharedHdfs, String nameNodeEndpoint,
                                                           int numThreads, int opsPerFile, String inputPath,
                                                           boolean shuffle, int opCode, List<String> directories)
                            throws IOException, InterruptedException {
                        return Commands.mkdirWeakScaling(sharedHdfs, opsPerFile, numThreads, directories,
                                OP_MKDIR_WEAK_SCALING, (directoryChoice == 3));
                    }
                });
    }

    private void statFilesWeakScaling(final DistributedFileSystem sharedHdfs) throws InterruptedException, IOException {
        System.out.print("How many threads should be used?\n> ");
        String inputN = scanner.nextLine();
        int numThreads = Integer.parseInt(inputN);

        System.out.print("How many files should each thread stat?\n> ");
        String inputFilesPerThread = scanner.nextLine();
        int filesPerThread = Integer.parseInt(inputFilesPerThread);

        System.out.print("Please provide a path to a local file containing at least " + inputN + " HopsFS file " +
                (numThreads == 1 ? "path.\n> " : "paths.\n> "));
        String inputPath = scanner.nextLine();

        boolean shuffle = getBooleanFromUser("Shuffle file paths around?");

        int numTrials = getIntFromUser("How many trials should this benchmark be performed?");

        JsonObject payload = new JsonObject();
        payload.addProperty(OPERATION, OP_STAT_FILES_WEAK_SCALING);
        payload.addProperty("numThreads", numThreads);
        payload.addProperty("filesPerThread", filesPerThread);
        payload.addProperty("inputPath", inputPath);
        payload.addProperty("shuffle", shuffle);

        performDistributedBenchmark(sharedHdfs, numTrials, payload, numThreads,
                filesPerThread, inputPath, shuffle, OP_STAT_FILES_WEAK_SCALING, null,
                "Stat n Files with n Threads (Weak Scaling - Stat File)", new DistributedBenchmarkOperation() {
                    @Override
                    public DistributedBenchmarkResult call(DistributedFileSystem sharedHdfs, String nameNodeEndpoint,
                                                           int numThreads, int opsPerFile, String inputPath,
                                                           boolean shuffle, int opCode, List<String> directories)
                            throws FileNotFoundException, InterruptedException {
                        return Commands.statFilesWeakScaling(sharedHdfs, numThreads,
                                opsPerFile, inputPath, shuffle, OP_STAT_FILES_WEAK_SCALING);
                    }
                });
    }

    private void listDirectoriesFromFile(final DistributedFileSystem sharedHdfs) throws InterruptedException, IOException {
        System.out.print("How many clients (i.e., threads) should be used?\n> ");
        String inputN = scanner.nextLine();
        int numberOfThreads = Integer.parseInt(inputN);

        System.out.print("How many times should each client list their assigned directory?\n> ");
        String inputReadsPerFile = scanner.nextLine();
        int readsPerFile = Integer.parseInt(inputReadsPerFile);

        System.out.print("Please provide a path to a local file containing at least " + inputN + " HopsFS directory " +
                (numberOfThreads == 1 ? "path.\n> " : "paths.\n> "));
        String inputPath = scanner.nextLine();

        boolean shuffle = getBooleanFromUser("Shuffle file paths around?");

        int numTrials = getIntFromUser("How many trials should this benchmark be performed?");

        JsonObject payload = new JsonObject();
        payload.addProperty(OPERATION, OP_LIST_DIRECTORIES_FROM_FILE);
        payload.addProperty("n", numberOfThreads);
        payload.addProperty("listsPerFile", readsPerFile);
        payload.addProperty("inputPath", inputPath);
        payload.addProperty("shuffle", shuffle);

        performDistributedBenchmark(sharedHdfs, numTrials, payload, numberOfThreads,
                readsPerFile, inputPath, shuffle, OP_LIST_DIRECTORIES_FROM_FILE, null,
                "List n Directories with n Threads (Weak Scaling - List Dir)", new DistributedBenchmarkOperation() {
                    @Override
                    public DistributedBenchmarkResult call(DistributedFileSystem sharedHdfs, String nameNodeEndpoint,
                                                           int numThreads, int opsPerFile, String inputPath,
                                                           boolean shuffle, int opCode, List<String> directories)
                            throws FileNotFoundException, InterruptedException {
                        return Commands.listDirectoryWeakScaling(sharedHdfs, numThreads,
                                opsPerFile, inputPath, shuffle, OP_LIST_DIRECTORIES_FROM_FILE);
                    }
                });
    }

    /**
     * Update the running totals for number of GCs performed and time spent GC-ing.
     */
    private void updateGCMetrics() {
        List<GarbageCollectorMXBean> gcMxBeans = ManagementFactory.getGarbageCollectorMXBeans();
        this.numGarbageCollections = 0;
        this.garbageCollectionTime = 0;
        for (GarbageCollectorMXBean mxBean : gcMxBeans) {
            long count = mxBean.getCollectionCount();
            long time  = mxBean.getCollectionTime();

            if (count > 0)
                this.numGarbageCollections += count;

            if (time > 0)
                this.garbageCollectionTime += time;
        }
    }

    private void saveLatenciesToFile() {
        LOG.info("Saving latency data to file.");

        System.out.print("Please enter a filename (without an extension):\n>");
        String fileName = scanner.nextLine().trim();

        System.out.println("Writing TCP latencies to file: ./latencies/" + fileName + "-tcp.dat");
        System.out.println("Writing HTTP latencies to file: ./latencies/" + fileName + "-http.dat");
        System.out.println("Writing merged latencies to file: ./latencies/" + fileName + "-merged.dat");

        String directoryPath = "./latencies";
        File dir = new File(directoryPath);

        if (!dir.exists())
            dir.mkdir();

        File fileTcp = new File(directoryPath + "/" + fileName + "-tcp.dat");
        File fileHttp = new File(directoryPath + "/" + fileName + "-http.dat");
        File fileMerged = new File(directoryPath + "/" + fileName + "-merged.dat");
        DescriptiveStatistics tcpStatistics = primaryHdfs.getLatencyTcpStatistics();
        DescriptiveStatistics httpStatistics = primaryHdfs.getLatencyHttpStatistics();
        DescriptiveStatistics mergedStatistics = primaryHdfs.getLatencyStatistics();

        try {
            FileWriter fw = new FileWriter(fileTcp.getAbsoluteFile());
            BufferedWriter bw = new BufferedWriter(fw);

            for (double tcpLatency : tcpStatistics.getValues()) {
                bw.write(String.valueOf(tcpLatency));
                bw.write("\n");
            }

            bw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            FileWriter fw = new FileWriter(fileHttp.getAbsoluteFile());
            BufferedWriter bw = new BufferedWriter(fw);

            for (double httpLatency : httpStatistics.getValues()) {
                bw.write(String.valueOf(httpLatency));
                bw.write("\n");
            }

            bw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            FileWriter fw = new FileWriter(fileMerged.getAbsoluteFile());
            BufferedWriter bw = new BufferedWriter(fw);

            for (double mergedLatency : mergedStatistics.getValues()) {
                bw.write(String.valueOf(mergedLatency));
                bw.write("\n");
            }

            bw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void toggleBenchmarkMode() {
        boolean toggle = getBooleanFromUser("Enter 'true' to enable benchmark mode and 'false' to disable it.");

        if (toggle) {
            LOG.info("ENABLING benchmark mode. This will also disable 'Followers Track Ops Perf'");
            followersTrackOpsPerformed = false;
        }
        else {
            LOG.info("DISABLING benchmark mode. This will NOT enable 'Followers Track Ops Perf'. That must be done separately.");
        }

        BENCHMARKING_MODE = toggle;
        primaryHdfs.setBenchmarkModeEnabled(toggle);

        for (DistributedFileSystem hdfs : hdfsClients) {
            hdfs.setBenchmarkModeEnabled(toggle);
        }

        if (!nonDistributed) {
            JsonObject payload = new JsonObject();
            String operationId = UUID.randomUUID().toString();
            payload.addProperty(OPERATION, OP_TOGGLE_BENCHMARK_MODE);
            payload.addProperty(OPERATION_ID, operationId);
            payload.addProperty(BENCHMARK_MODE, toggle);

            issueCommandToFollowers((toggle ? "Enabling" : "Disabling" ) + " Benchmark Mode", operationId, payload, false);
        } else {
            LOG.warn("Running in non-distributed mode. We have no followers.");
        }
    }

    /**
     * Enable/disable followers tracking OperationPerformed instances and sending them after benchmarks.
     */
    private void toggleOperationsPerformedInFollowers() {
        if (!nonDistributed) {
            if (followersTrackOpsPerformed) {
                LOG.info("DISABLING OperationPerformed tracking.");
            } else {
                LOG.info("ENABLING OperationPerformed tracking.");
            }

            followersTrackOpsPerformed = !followersTrackOpsPerformed;

            JsonObject payload = new JsonObject();
            String operationId = UUID.randomUUID().toString();
            payload.addProperty(OPERATION, OP_TOGGLE_OPS_PERFORMED_FOLLOWERS);
            payload.addProperty(OPERATION_ID, operationId);
            payload.addProperty(TRACK_OP_PERFORMED, followersTrackOpsPerformed);

            issueCommandToFollowers("Toggle Operation Performed", operationId, payload, false);
        } else {
            LOG.warn("Running in non-distributed mode. We have no followers.");
        }
    }

    /**
     * Perform GCs on this Client VM as well as any other client VMs if we're the Commander for a distributed setup.
     */
    private void performClientVMGarbageCollection() {
        if (!nonDistributed) {
            JsonObject payload = new JsonObject();
            String operationId = UUID.randomUUID().toString();
            payload.addProperty(OPERATION, OP_TRIGGER_CLIENT_GC);
            payload.addProperty(OPERATION_ID, operationId);

            issueCommandToFollowers("Client VM Garbage Collection", operationId, payload, false);
        }
        System.gc();
    }

    private void handleSetLogLevel() {
        if (!isServerless) {
            LOG.error("Modifying the NN debug level thru the benchmarking tool is not supported for Vanilla HopsFS!");
            return;
        }

        String currentLogLevel = primaryHdfs.getServerlessFunctionLogLevel();
        LOG.info("");
        LOG.info("Current log level: " + currentLogLevel);

        System.out.print("Please enter the new log level, or nothing to keep it the same:\n> ");
        String newLogLevel = scanner.nextLine();
        newLogLevel = newLogLevel.trim();

        if (newLogLevel.isEmpty())
            return;

        if (!(newLogLevel.equalsIgnoreCase("INFO") ||
                newLogLevel.equalsIgnoreCase("DEBUG") ||
                newLogLevel.equalsIgnoreCase("WARN") ||
                newLogLevel.equalsIgnoreCase("ERROR") ||
                newLogLevel.equalsIgnoreCase("FATAL") ||
                newLogLevel.equalsIgnoreCase("ALL") ||
                newLogLevel.equalsIgnoreCase("TRACE"))) {
            LOG.error("Invalid log level specified: '" + newLogLevel + "'");
            return;
        } else {
            primaryHdfs.setServerlessFunctionLogLevel(newLogLevel);

            for (DistributedFileSystem hdfs : hdfsClients) {
                hdfs.setServerlessFunctionLogLevel(newLogLevel);
            }
        }

        if (!nonDistributed) {
            JsonObject payload = new JsonObject();
            String operationId = UUID.randomUUID().toString();
            payload.addProperty(OPERATION, OP_SET_LOG_LEVEL);
            payload.addProperty(OPERATION_ID, operationId);
            payload.addProperty("LOG_LEVEL", newLogLevel);

            issueCommandToFollowers("Setting serverless log level to " + newLogLevel, operationId,
                    payload, false);
        } else {
            LOG.warn("Running in non-distributed mode. We have no followers.");
        }
    }

    private void handleSetConsistencyProtocolEnabled() {
        if (!isServerless) {
            LOG.error("The consistency protocol is not supported by Vanilla HopsFS!");
            return;
        }

        boolean currentFlag = primaryHdfs.getConsistencyProtocolEnabled();
        LOG.info("");
        LOG.info("Consistency protocol is currently " + (currentFlag ? "ENABLED." : "DISABLED."));

        System.out.print("Enable [t/y] or Disable [f/n] consistency protocol? (Enter anything else to keep it the same):\n> ");
        String newFlag = scanner.nextLine();
        newFlag = newFlag.trim();

        boolean toggle;

        if (newFlag.equalsIgnoreCase("t") || newFlag.equalsIgnoreCase("y")) {
            if (!currentFlag)
                LOG.info("ENABLING consistency protocol.");
            else
                LOG.info("Consistency protocol is already enabled.");

            toggle = true;
        }
        else if (newFlag.equalsIgnoreCase("f") || newFlag.equalsIgnoreCase("n")) {
            if (currentFlag)
                LOG.info("DISABLING consistency protocol.");
            else
                LOG.info("Consistency protocol is already disabled.");

            toggle = false;
        }
        else {
            LOG.info("Keeping consistency protocol toggle the same as before (" + (currentFlag ? "ENABLED)." : "DISABLED)."));
            return;
        }

        primaryHdfs.setConsistencyProtocolEnabled(toggle);
        consistencyEnabled = toggle;

        for (DistributedFileSystem hdfs : hdfsClients) {
            hdfs.setConsistencyProtocolEnabled(toggle);
        }

        if (!nonDistributed) {
            JsonObject payload = new JsonObject();
            String operationId = UUID.randomUUID().toString();
            payload.addProperty(OPERATION, OP_SET_CONSISTENCY_PROTOCOL_ENABLED);
            payload.addProperty(OPERATION_ID, operationId);
            payload.addProperty("TOGGLE", toggle);

            issueCommandToFollowers(toggle ? "Enabling " : "Disabling " + "consistency protocol.",
                    operationId, payload, false);
        } else {
            LOG.warn("Running in non-distributed mode. We have no followers.");
        }
    }

    private void printAndModifyPostTrialSleep() {
        System.out.println("Post-trial sleep interval: " + postTrialSleepInterval + " ms");

        System.out.print("Please enter a new sleep interval duration (in ms), or nothing to keep it the same:\n> ");
        String newIntervalString = scanner.nextLine();
        newIntervalString = newIntervalString.trim();

        if (newIntervalString.isEmpty())
            return;

        try {
            int newInterval = Integer.parseInt(newIntervalString);

            if (newInterval < 0) {
                LOG.error("Invalid interval specified: " + newInterval + ". Interval must be non-negative.");
                return;
            }

            System.out.println("Changed post-trial sleep interval FROM " + postTrialSleepInterval + " ms TO " +
                    newInterval + " ms.");
            postTrialSleepInterval = newInterval;
        } catch (NumberFormatException ex) {
            LOG.error("Invalid value specified. Returning.");
        }
    }

    private void setHttpTcpReplacementChance() {
        if (!isServerless) {
            LOG.error("HTTP/TCP replacement is not supported by Vanilla HopsFS!");
            return;
        }

        double currentChance = primaryHdfs.getHttpTcpReplacementChance();
        LOG.info("");
        LOG.info("Current HTTP/TCP replacement chance: " + currentChance);

        System.out.print("New value (between 0.0 and 1.0)? (Enter nothing to keep it the same.):\n> ");
        String newChance = scanner.nextLine();
        newChance = newChance.trim();

        if (newChance.equals("")) {
            LOG.info("Keeping the value the same.");
            return;
        }

        double chance;
        try {
             chance = Double.parseDouble(newChance);

             primaryHdfs.setHttpTcpReplacementChance(chance);

            for (DistributedFileSystem hdfs : hdfsClients) {
                hdfs.setHttpTcpReplacementChance(chance);
            }
        } catch (NumberFormatException ex) {
            LOG.error("Invalid chance specified: " + newChance);
            return;
        } catch (IllegalArgumentException ex) {
            LOG.error("Illegal chance specified:", ex);
            return;
        }

        if (!nonDistributed) {
            JsonObject payload = new JsonObject();
            String operationId = UUID.randomUUID().toString();
            payload.addProperty(OPERATION, OP_SET_HTTP_TCP_REPLACEMENT_CHANCE);
            payload.addProperty(OPERATION_ID, operationId);
            payload.addProperty("CHANCE", chance);

            issueCommandToFollowers("Setting HTTP/TCP replacement chance.",
                    operationId, payload, false);
        } else {
            LOG.warn("Running in non-distributed mode. We have no followers.");
        }
    }

    /**
     * Issue a command to all our followers.
     * @param opName The name of the command.
     * @param operationId Unique ID of this operation.
     * @param payload Contains the command and necessary arguments.
     */
    private void issueCommandToFollowers(String opName, String operationId, JsonObject payload, boolean addToWaiting) {
        int numFollowers = followers.size();
        if (numFollowers == 0) {
            LOG.warn("We have no followers (though we are in distributed mode).");
            return;
        }

        LOG.debug("Issuing '" + opName + "' (id=" + operationId + ") command to " +
                numFollowers + " follower(s).");

        BlockingQueue<DistributedBenchmarkResult> resultQueue = new
                ArrayBlockingQueue<>(numFollowers);
        resultQueues.put(operationId, resultQueue);

        String payloadStr = new Gson().toJson(payload);
        for (FollowerConnection followerConnection : followers) {
            LOG.debug("Sending '" + opName + "' operation to follower at " +
                    followerConnection.getRemoteAddressTCP());
            followerConnection.sendTCP(payloadStr);

            if (addToWaiting)
                waitingOn.add(followerConnection.name);
        }
    }

    /**
     * Display the provided prompt and accept input from the user, trying to convert it to a boolean value.
     *
     * The following user inputs will be converted to true (ignoring case):
     * - y
     * - yes
     * - t
     * - true
     * - 1
     *
     * Any other input will be converted to false.
     *
     * @param prompt The prompt to be displayed to the user.
     * @return True or false depending on the user's input.
     */
    private boolean getBooleanFromUser(String prompt) {
        System.out.print(prompt + " [y/n]\n> ");
        String input = scanner.nextLine().trim();

        checkForExit(input);

        return input.equalsIgnoreCase("y") || input.equalsIgnoreCase("yes") ||
                input.equalsIgnoreCase("t") || input.equalsIgnoreCase("true") ||
                input.equalsIgnoreCase("1");
    }

    /**
     * Check if the user is trying to cancel the current operation.
     *
     * @param input The user's input.
     */
    private static void checkForExit(String input) {
        if (input.equalsIgnoreCase("abort") || input.equalsIgnoreCase("cancel") ||
                input.equalsIgnoreCase("exit"))
            throw new IllegalArgumentException("User specified '" + input + "'. Aborting operation.");
    }

    private int getIntFromUser(String prompt) {
        System.out.print(prompt + "\n> ");
        String input = scanner.nextLine();
        checkForExit(input);
        return Integer.parseInt(input);
    }

    private String getStringFromUser(String prompt) {
        System.out.print(prompt + "\n> ");
        String input = scanner.nextLine();
        checkForExit(input);
        return input;
    }

    public void strongScalingWriteOperation(final DistributedFileSystem sharedHdfs)
            throws InterruptedException, IOException {
        int totalNumberOfFiles = getIntFromUser("Total number of files to write?");
        int numberOfThreads = getIntFromUser("Number of threads to use?");

        if (totalNumberOfFiles < numberOfThreads) {
            LOG.error("The number of files to be written (" + totalNumberOfFiles +
                    ") should be less than the number of threads used to write said files (" + numberOfThreads + ").");
            return;
        }

        int writesPerThread = totalNumberOfFiles / numberOfThreads;
        int remainder = totalNumberOfFiles % numberOfThreads;

        if (remainder != 0) {
            LOG.error("Cannot cleanly divide " + totalNumberOfFiles + " writes among " + numberOfThreads + " threads.");
            return;
        }

        int directoryChoice = getIntFromUser(
                "Should threads all write to SAME DIRECTORY [1] or DIFFERENT DIRECTORIES [2]?");

        if (directoryChoice < 1 || directoryChoice > 2) {
            LOG.error("Invalid argument specified. Should be \"1\" for same directory or \"2\" for different directories. " +
                    "Instead, got \"" + directoryChoice + "\"");
            return;
        }

        int dirInputMethodChoice = getIntFromUser("Manually input (comma-separated list) [1], or specify file containing directories [2]?");

        List<String> directories;
        if (dirInputMethodChoice == 1) {
            System.out.print("Please enter the directories as a comma-separated list:\n> ");
            String listOfDirectories = scanner.nextLine();
            directories = Arrays.asList(listOfDirectories.split(","));

            if (directories.size() == 1)
                LOG.info("1 directory specified.");
            else
                LOG.info(directories.size() + " directories specified.");
        }
        else if (dirInputMethodChoice == 2) {
            System.out.print("Please provide path to file containing HopsFS directories:\n> ");
            String filePath = scanner.nextLine();
            directories = Utils.getFilePathsFromFile(filePath);

            if (directories.size() == 1)
                LOG.info("1 directory specified in file.");
            else
                LOG.info(directories.size() + " directories specified in file.");
        }
        else {
            LOG.error("Invalid option specified (" + dirInputMethodChoice +
                    "). Please enter \"1\" or \"2\" for this prompt.");
            return;
        }

        // IMPORTANT: Make directories the same size as the number of threads, so we have one directory per thread.
        //            This allows us to directly reuse the writeFilesInternal() function, which creates a certain
        //            number of files per directory. If number of threads is equal to number of directories, then
        //            we are essentially creating a certain number of files per thread, which is what we want.
        if (directoryChoice == 1) {
            Random rng = new Random();
            int idx = rng.nextInt(directories.size());
            String dir = directories.get(idx);
            directories = new ArrayList<>(numberOfThreads);
            for (int i = 0; i < numberOfThreads; i++)
                directories.add(dir); // This way, they'll all write to the same directory. We can reuse old code.
        } else {
            Collections.shuffle(directories);
            directories = directories.subList(0, numberOfThreads);
        }

        int minLength = 0;
        try {
            minLength = getIntFromUser("Min string length (default: " + minLength + ")?");
        } catch (NumberFormatException ex) {
            LOG.info("Defaulting to " + minLength + ".");
        }

        int maxLength = 0;
        try {
            maxLength = getIntFromUser("Max string length (default: " + maxLength + ")?");
        } catch (NumberFormatException ex) {
            LOG.info("Defaulting to " + maxLength + ".");
        }

        JsonObject payload = new JsonObject();
        payload.addProperty(OPERATION, OP_STRONG_SCALING_WRITES);
        payload.addProperty("n", writesPerThread);
        payload.addProperty("minLength", minLength);
        payload.addProperty("maxLength", maxLength);
        payload.addProperty("numberOfThreads", numberOfThreads);
        JsonArray directoriesJson = new JsonArray();
        for (String dir : directories)
            directoriesJson.add(dir);
        payload.add("directories", directoriesJson);

        performDistributedBenchmark(sharedHdfs, 1, payload, numberOfThreads, writesPerThread, null,
                false, OP_STRONG_SCALING_WRITES, directories, "Write n Files with n Threads (Strong Scaling - Write)",
                new DistributedBenchmarkOperation() {
                    @Override
                    public DistributedBenchmarkResult call(DistributedFileSystem sharedHdfs, String nameNodeEndpoint,
                                                           int numThreads, int opsPerFile, String inputPath,
                                                           boolean shuffle, int opCode, List<String> directories)
                            throws InterruptedException, IOException {
                        return Commands.writeFilesInternal(opsPerFile, numThreads, directories,
                                sharedHdfs, OP_STRONG_SCALING_WRITES, false);
                    }
                });
    }

    public void strongScalingReadOperation(final DistributedFileSystem sharedHdfs)
            throws InterruptedException, FileNotFoundException {
        // User provides file containing HopsFS file paths.
        // Specifies how many files each thread should read.
        // Specifies number of threads.
        // Specifies how many times each file should be read.
        int filesPerThread = getIntFromUser("How many files should be read by each thread?");

        int readsPerFile = getIntFromUser("How many times should each file be read?");

        int numThreads = getIntFromUser("Number of threads");

        System.out.print("Please provide a path to a local file containing at least " + filesPerThread + " HopsFS file " +
                (filesPerThread == 1 ? "path.\n> " : "paths.\n> "));
        String inputPath = scanner.nextLine();

        String operationId = UUID.randomUUID().toString();
        int numDistributedResults = followers.size();
        if (followers.size() > 0) {
            JsonObject payload = new JsonObject();
            payload.addProperty(OPERATION, OP_STRONG_SCALING_READS);
            payload.addProperty(OPERATION_ID, operationId);
            payload.addProperty("n", filesPerThread);
            payload.addProperty("readsPerFile", readsPerFile);
            payload.addProperty("numThreads", numThreads);
            payload.addProperty("inputPath", inputPath);

            issueCommandToFollowers("Read n Files y Times with z Threads (Strong Scaling)", operationId, payload, true);
        }

        DistributedBenchmarkResult localResult =
                Commands.strongScalingBenchmark(sharedHdfs, filesPerThread, readsPerFile,
                        numThreads, inputPath);

        if (localResult == null) {
            LOG.warn("Local result is null. Aborting.");
            return;
        }

        LOG.info("LOCAL result of strong scaling benchmark: " + localResult);
        localResult.setOperationId(operationId);

        // Wait for followers' results if we had followers when we first started the operation.
        if (numDistributedResults > 0)
            waitForDistributedResult(numDistributedResults, operationId, localResult);
    }

    /**
     * Weak scaling, writes.
     */
    public void weakScalingWriteOperation(final DistributedFileSystem sharedHdfs)
            throws IOException, InterruptedException {
        int directoryChoice = getIntFromUser("Should the threads write their files to the SAME DIRECTORY [1], DIFFERENT DIRECTORIES [2], or RANDOM WRITES [3]?");

        // Validate input.
        if (directoryChoice < 1 || directoryChoice > 3) {
            LOG.error("Invalid argument specified. Should be \"1\" for same directory, \"2\" for different directories. " +
                    "Or \"3\" for random writes. Instead, got \"" + directoryChoice + "\"");
            return;
        }

        int dirInputMethodChoice = getIntFromUser("Manually input (comma-separated list) [1], or specify file containing directories [2]?");

        List<String> directories;
        if (dirInputMethodChoice == 1) {
            System.out.print("Please enter the directories as a comma-separated list:\n> ");
            String listOfDirectories = scanner.nextLine();
            directories = Arrays.asList(listOfDirectories.split(","));

            if (directories.size() == 1)
                LOG.info("1 directory specified.");
            else
                LOG.info(directories.size() + " directories specified.");
        }
        else if (dirInputMethodChoice == 2) {
            System.out.print("Please provide path to file containing HopsFS directories:\n> ");
            String filePath = scanner.nextLine();
            directories = Utils.getFilePathsFromFile(filePath);

            if (directories.size() == 1)
                LOG.info("1 directory specified in file.");
            else
                LOG.info(directories.size() + " directories specified in file.");
        }
        else {
            LOG.error("Invalid option specified (" + dirInputMethodChoice +
                    "). Please enter \"1\" or \"2\" for this prompt.");
            return;
        }

        System.out.print("Number of threads? \n> ");
        int numberOfThreads = Integer.parseInt(scanner.nextLine());

        if (directoryChoice == 1) {
            Random rng = new Random();
            int idx = rng.nextInt(directories.size());
            String dir = directories.get(idx);
            directories = new ArrayList<>(numberOfThreads);
            for (int i = 0; i < numberOfThreads; i++)
                directories.add(dir); // This way, they'll all write to the same directory. We can reuse old code.
        } else if (directoryChoice == 2) {
            Collections.shuffle(directories);
            directories = directories.subList(0, numberOfThreads);
        } // Else, use the entire directories list. We'll generate a bunch of random writes using the full list.

        System.out.print("Number of writes per thread? \n> ");
        int writesPerThread = Integer.parseInt(scanner.nextLine());

        int numTrials = getIntFromUser("How many trials should be performed?");

        JsonObject payload = new JsonObject();
        payload.addProperty(OPERATION, OP_WEAK_SCALING_WRITES);
        payload.addProperty("n", writesPerThread);
        // payload.addProperty("minLength", minLength);
        // payload.addProperty("maxLength", maxLength);
        payload.addProperty("numberOfThreads", numberOfThreads);
        payload.addProperty("randomWrites", directoryChoice == 3);

        JsonArray directoriesJson = new JsonArray();
        for (String dir : directories)
            directoriesJson.add(dir);

        payload.add("directories", directoriesJson);

        performDistributedBenchmark(sharedHdfs, numTrials, payload, numberOfThreads, writesPerThread, null,
                false, OP_WEAK_SCALING_WRITES, directories, "Write n Files with n Threads (Weak Scaling - Write)",
                new DistributedBenchmarkOperation() {
                    @Override
                    public DistributedBenchmarkResult call(DistributedFileSystem sharedHdfs, String nameNodeEndpoint,
                                                           int numThreads, int opsPerFile, String inputPath,
                                                           boolean shuffle, int opCode, List<String> directories)
                            throws IOException, InterruptedException {
                        return Commands.writeFilesInternal(opsPerFile, numThreads, directories,
                                sharedHdfs, OP_WEAK_SCALING_WRITES, (directoryChoice == 3));
                    }
                });
    }

    /**
     * Wait for distributed results. Aggregate them as they are received. Print the results at the end.
     *
     * @param numDistributedResults The number of results that we're waiting for.
     * @param operationId Unique ID of this distributed operation.
     * @param localResult The result obtained by our local execution of the operation.
     *
     * @return The aggregated throughput.
     */
    private AggregatedResult waitForDistributedResult(
            int numDistributedResults,
            String operationId,
            DistributedBenchmarkResult localResult) throws InterruptedException {
        // If there are no distributed results, then the local result should be non-null.
        // If the local result is null, then the Commander should be configured to NOT execute tasks (if that were the
        // case, then we'd have a local result). If the Commander is configured to execute tasks, then we'll throw an
        // assertion error, as that's a bug. Otherwise, it's just weird that we called waitForDistributedResult(),
        // given there are both no results and the Commander isn't executing anything itself.
        if (numDistributedResults < 1 && localResult == null) {
            assert(!commanderExecutesToo);
            LOG.error("We are not expecting any distributed results, but the Commander does not execute tasks/jobs.");
            return AggregatedResult.DEFAULT_RESULT;
        }

//        if (numDistributedResults < 1) {
//            if (localResult == null) {
//                assert(!commanderExecutesToo);
//                LOG.error("We are not expecting any distributed results, but the Commander does not execute tasks/jobs.");
//                return AggregatedResult.DEFAULT_RESULT;
//            }
//
//            String metricsString = "";
//
//            DecimalFormat df = new DecimalFormat("#.####");
//            double avgTcpLatency;
//            if (localResult.tcpLatencyStatistics != null)
//                avgTcpLatency = localResult.tcpLatencyStatistics.getMean();
//            else
//                avgTcpLatency = -1;
//
//            double avgHttpLatency;
//            if (localResult.httpLatencyStatistics != null)
//                avgHttpLatency = (localResult.httpLatencyStatistics.getN() > 0) ? localResult.httpLatencyStatistics.getMean() : 0;
//            else
//                avgHttpLatency = -1;
//
//            double avgLatency = avgTcpLatency + avgHttpLatency / 2.0;
//            double cacheHitRate = ((double)localResult.cacheHits / (double)(localResult.cacheHits + localResult.cacheMisses));
//            // throughput (ops/sec), cache hits, cache misses, cache hit rate, avg tcp latency, avg http latency, avg combined latency
//            metricsString = String.format("%s %d %d %s %s %s %s %s", df.format(localResult.getOpsPerSecond()),
//                    localResult.cacheHits, localResult.cacheMisses,
//                    df.format(cacheHitRate),
//                    df.format(avgTcpLatency),
//                    df.format(avgHttpLatency),
//                    df.format(avgLatency),
//                    df.format(localResult.durationSeconds));
//
//            return new AggregatedResult(localResult.getOpsPerSecond(), localResult.cacheHits,
//                    localResult.cacheMisses, metricsString, avgTcpLatency, avgHttpLatency, avgLatency,
//                    localResult.durationSeconds);
//        }

        BlockingQueue<DistributedBenchmarkResult> resultQueue;
        if (numDistributedResults >= 1) {
            LOG.debug("Waiting for " + numDistributedResults + " distributed result(s).");
            resultQueue = resultQueues.get(operationId);
            assert (resultQueue != null);

            int counter = 0;
            long time = System.currentTimeMillis();

            while (waitingOn.size() > 0) {
                LOG.debug("Still waiting on the following Followers: " + StringUtils.join(waitingOn, ", "));
                Thread.sleep(1000);

                int numDisconnects = numDisconnections.getAndSet(0);

                if (numDisconnects > 0) {
                    LOG.warn("There has been at least one disconnection.");
                }

                counter += (System.currentTimeMillis() - time);
                time = System.currentTimeMillis();
                if (counter >= 60000) {
                    boolean decision = getBooleanFromUser("Stop waiting early?");

                    if (decision) {
                        // We won't be waiting on these anymore.
                        break;
                    }

                    counter = 0;
                }
            }
        } else {
            resultQueue = new LinkedBlockingQueue<>(); // Empty queue, no distributed results.
        }

        return extractDistributedResultFromQueue(resultQueue, localResult);
    }

    /**
     * Extract results from queue and aggregate them together. Return the aggregated result.
     *
     * @param resultQueue The queue from which to extract results.
     * @param localResult The result from local execution.
     */
    private AggregatedResult extractDistributedResultFromQueue(
            BlockingQueue<DistributedBenchmarkResult> resultQueue,
            DistributedBenchmarkResult localResult) {
        DescriptiveStatistics numOpsPerformed = new DescriptiveStatistics();
        DescriptiveStatistics duration = new DescriptiveStatistics();
        DescriptiveStatistics throughput = new DescriptiveStatistics();
        DescriptiveStatistics cacheHits = new DescriptiveStatistics();
        DescriptiveStatistics cacheMisses = new DescriptiveStatistics();

        DescriptiveStatistics tcpLatency = new DescriptiveStatistics();
        DescriptiveStatistics httpLatency = new DescriptiveStatistics();

        DescriptiveStatistics gcTime = new DescriptiveStatistics();
        int totalNumberOfGCs = 0;

        List<Double> allThroughputValues = new ArrayList<>(resultQueue.size() + 1);

        long startProcessLocalResult = System.currentTimeMillis();
        if (localResult != null) {
            numOpsPerformed.addValue(localResult.numOpsPerformed);
            duration.addValue(localResult.durationSeconds);
            throughput.addValue(localResult.getOpsPerSecond());
            cacheHits.addValue(localResult.cacheHits);
            cacheMisses.addValue(localResult.cacheMisses);

            LOG.info("========== LOCAL RESULT ==========");
            LOG.info("Num Ops Performed   : " + localResult.numOpsPerformed);
            LOG.info("Duration (sec)      : " + localResult.durationSeconds);
            LOG.info("Cache hits          : " + localResult.cacheHits);
            LOG.info("Cache misses        : " + localResult.cacheMisses);
            if (localResult.cacheHits + localResult.cacheMisses > 0)
                LOG.info("Cache hit percentage: " + (localResult.cacheHits / (localResult.cacheHits + localResult.cacheMisses)));
            else
                LOG.info("Cache hit percentage: N/A");
            LOG.info("Throughput          : " + localResult.getOpsPerSecond() + "\n");

            allThroughputValues.add(localResult.getOpsPerSecond());

            for (double d : localResult.tcpLatencyStatistics.getValues())
                tcpLatency.addValue(d);

            for (double d : localResult.httpLatencyStatistics.getValues())
                httpLatency.addValue(d);

            // Gather garbage collection information.
            if (localResult.opsPerformed != null) {
                for (OperationPerformed operationPerformed : localResult.opsPerformed) {
                    gcTime.addValue(operationPerformed.getGarbageCollectionTime());
                    totalNumberOfGCs += operationPerformed.getNumGarbageCollections();
                }

                // Local result is the only result processed so far, so just use running totals to print.
                LOG.info("Number of GCs: " + totalNumberOfGCs);
                LOG.info("Time spent GC-ing: " + gcTime.getSum() + " ms");
            }

            LOG.info("Processed local result in " + (System.currentTimeMillis() - startProcessLocalResult) + " ms.");
        }

        long startProcessDistributedResults = System.currentTimeMillis();
        if (resultQueue.size() > 0) {
            int numDistributedResults = resultQueue.size();
            LOG.info("Result queue contains " + numDistributedResults + " distributed results.");
            for (DistributedBenchmarkResult res : resultQueue) {
                long startProcessCurrDistResult = System.currentTimeMillis();
                LOG.info("========== RECEIVED RESULT FROM " + res.jvmId + " ==========");
                LOG.info("Num Ops Performed   : " + res.numOpsPerformed);
                LOG.info("Duration (sec)      : " + res.durationSeconds);
                LOG.info("Cache hits          : " + res.cacheHits);
                LOG.info("Cache misses        : " + res.cacheMisses);
                if (res.cacheHits + res.cacheMisses > 0)
                    LOG.info("Cache hit percentage: " + (res.cacheHits / (res.cacheHits + res.cacheMisses)));
                else
                    LOG.info("Cache hit percentage: N/A");
                LOG.info("Throughput          : " + res.getOpsPerSecond());

                numOpsPerformed.addValue(res.numOpsPerformed);
                duration.addValue(res.durationSeconds);
                throughput.addValue(res.getOpsPerSecond());
                cacheHits.addValue(res.cacheHits);
                cacheMisses.addValue(res.cacheMisses);

                if (res.tcpLatencyStatistics != null && res.httpLatencyStatistics != null) {
                    DescriptiveStatistics latencyTcp = res.tcpLatencyStatistics;
                    DescriptiveStatistics latencyHttp = res.httpLatencyStatistics;
                    primaryHdfs.addLatencies(latencyTcp.getValues(), latencyHttp.getValues());

                    LOG.info("Latency TCP (ms) [min: " + latencyTcp.getMin() + ", max: " + latencyTcp.getMax() +
                            ", avg: " + latencyTcp.getMean() + ", std dev: " + latencyTcp.getStandardDeviation() +
                            ", N: " + latencyTcp.getN() + "]");
                    LOG.info("Latency HTTP (ms) [min: " + latencyHttp.getMin() + ", max: " + latencyHttp.getMax() +
                            ", avg: " + latencyHttp.getMean() + ", std dev: " + latencyHttp.getStandardDeviation() +
                            ", N: " + latencyHttp.getN() + "]");

                    for (double d : latencyTcp.getValues())
                        tcpLatency.addValue(d);

                    for (double d : latencyHttp.getValues())
                        httpLatency.addValue(d);
                }

                // Gather garbage collection information.
                if (res.opsPerformed != null) {
                    OperationPerformed[] tmpOperationsPerformed = res.opsPerformed;

                    long currentResultGcTime = 0;
                    int currentResultNumGCs = 0;
                    for (OperationPerformed operationPerformed : tmpOperationsPerformed) {
                        currentResultGcTime += operationPerformed.getGarbageCollectionTime();
                        currentResultNumGCs += operationPerformed.getNumGarbageCollections();

                        gcTime.addValue(operationPerformed.getGarbageCollectionTime());
                        totalNumberOfGCs += operationPerformed.getNumGarbageCollections();
                    }

                    LOG.info("Number of GCs: " + currentResultNumGCs);
                    LOG.info("Time spent GC-ing: " + currentResultGcTime + " ms");

                    primaryHdfs.addOperationPerformeds(tmpOperationsPerformed);
                }

                if (res.txEvents != null) {
                    primaryHdfs.mergeTransactionEvents(res.txEvents, true);
                }

                allThroughputValues.add(res.getOpsPerSecond());

                LOG.info("Processed result from " + res.jvmId + " in " +
                        (System.currentTimeMillis() - startProcessCurrDistResult) + " ms.");
            }

            LOG.info("Processed all " + numDistributedResults + " distributed result(s) in " +
                    (System.currentTimeMillis() - startProcessDistributedResults) + " ms.");
        } else {
            LOG.info("There are no distributed results in the queue.");
        }

        double aggregateThroughput = (numOpsPerformed.getSum() / duration.getMean());

        int totalCacheHits = (int)(cacheHits.getN() > 0 ? cacheHits.getSum() : 0);
        int totalCacheMisses = (int)(cacheMisses.getN() > 0 ? cacheMisses.getSum() : 0);

        // Compute weighted average.
        double avgCombinedLatency = 0.0;
        double totalNumLatencies = tcpLatency.getN() + httpLatency.getN();
        if (tcpLatency.getN() > 0)
            // If TCP only, then `(tcpLatency.getN() / totalNumLatencies)` will evaluate to 1.
            // Otherwise, `(tcpLatency.getN() / totalNumLatencies)` will help compute a weighted average.
            avgCombinedLatency += (tcpLatency.getMean() * (tcpLatency.getN() / totalNumLatencies));
        if (httpLatency.getN() > 0)
            // If HTTP only, then `(httpLatency.getN() / totalNumLatencies)` will evaluate to 1.
            // Otherwise, `(httpLatency.getN() / totalNumLatencies)` will help compute a weighted average.
            avgCombinedLatency += (httpLatency.getMean() * (httpLatency.getN() / totalNumLatencies));

        double cacheHitRate = 0.0;
        if (totalCacheHits > 0 || totalCacheMisses > 0)
            cacheHitRate = ((double)totalCacheHits / ((double)totalCacheHits + (double)totalCacheMisses));

        LOG.info("");
        LOG.info("==== AGGREGATED RESULTS ====");
        LOG.info("Average Duration: " + duration.getMean() * 1000.0 + " ms.");
        LOG.info("Cache hits: " + totalCacheHits);
        LOG.info("Cache misses: " + totalCacheMisses);
        LOG.info("Cache hit percentage: " + cacheHitRate);
        LOG.info("Average TCP latency: " + tcpLatency.getMean() + " ms");
        LOG.info("Average HTTP latency: " + httpLatency.getMean() + " ms");
        LOG.info("Average combined latency: " + avgCombinedLatency + " ms");
        LOG.info("Aggregate Throughput (ops/sec): " + aggregateThroughput);

        DecimalFormat df = new DecimalFormat("#.####");
        String metricsString = String.format("%s %d %d %s %s %s %s %s", df.format(aggregateThroughput),
                totalCacheHits, totalCacheMisses,
                df.format(cacheHitRate), df.format(tcpLatency.getMean()),
                df.format(httpLatency.getMean()), df.format(avgCombinedLatency), df.format(duration.getMean()));

        LOG.info(metricsString);

        // Create a copy and sort it so the estimated results are ordered by largest to the smallest throughput.
        Collections.sort(allThroughputValues);
        System.out.println("\n['Estimated' Throughput]");
        String format = "%10.2f --> %10.2f";
        for (double throughputResult : allThroughputValues) {
            String formatted = String.format(format, throughputResult, throughputResult * allThroughputValues.size());
            System.out.println(formatted);
        }

        return new AggregatedResult(aggregateThroughput, totalCacheHits, totalCacheMisses, metricsString,
                tcpLatency.getMean(), httpLatency.getMean(), avgCombinedLatency, duration.getMean(),
                totalNumberOfGCs, gcTime.getSum());
    }

    /**
     * Weak scaling, reads.
     *
     * Query the user for:
     *  - An integer `n`, the number of threads.
     *  - The path to a local file containing HopsFS file paths.
     *  - The number of files each thread should read.
     */
    private void weakScalingReadOperationV2(final DistributedFileSystem sharedHdfs)
            throws InterruptedException, IOException {
        System.out.print("How many threads should be used?\n> ");
        String inputN = scanner.nextLine();
        int numThreads = Integer.parseInt(inputN);

        System.out.print("How many files should each thread read?\n> ");
        String inputFilesPerThread = scanner.nextLine();
        int filesPerThread = Integer.parseInt(inputFilesPerThread);

        System.out.print("Please provide a path to a local file containing at least " + inputN + " HopsFS file " +
                (numThreads == 1 ? "path.\n> " : "paths.\n> "));
        String inputPath = scanner.nextLine();

        boolean shuffle = getBooleanFromUser("Shuffle file paths around?");

        int numTrials = getIntFromUser("How many trials should this benchmark be performed?");

        JsonObject payload = new JsonObject();
        payload.addProperty(OPERATION, OP_WEAK_SCALING_READS_V2);
        payload.addProperty("numThreads", numThreads);
        payload.addProperty("filesPerThread", filesPerThread);
        payload.addProperty("inputPath", inputPath);
        payload.addProperty("shuffle", shuffle);

        performDistributedBenchmark(sharedHdfs, numTrials, payload, numThreads,
                filesPerThread, inputPath, shuffle, OP_WEAK_SCALING_READS_V2, null,
                "Read n Files with n Threads (Weak Scaling Read v2)", new DistributedBenchmarkOperation() {
                    @Override
                    public DistributedBenchmarkResult call(DistributedFileSystem sharedHdfs, String nameNodeEndpoint,
                                                           int numThreads, int opsPerFile, String inputPath,
                                                           boolean shuffle, int opCode, List<String> directories)
                            throws FileNotFoundException, InterruptedException {
                        return Commands.weakScalingBenchmarkV2(sharedHdfs, numThreads,
                                opsPerFile, inputPath, shuffle, OP_WEAK_SCALING_READS_V2);
                    }
                });
    }

    /**
     * Weak scaling, reads.
     *
     * Query the user for:
     *  - An integer `n`, the number of files to read
     *  - The path to a local file containing `n` or more HopsFS file paths.
     *  - The number of reads per file.
     *
     * This function will use `n` threads to read those `n` files.
     */
    private void weakScalingReadOperation(final DistributedFileSystem sharedHdfs)
            throws InterruptedException, IOException {
        System.out.print("How many files should be read?\n> ");
        String inputN = scanner.nextLine();
        int n = Integer.parseInt(inputN);

        System.out.print("How many times should each file be read?\n> ");
        String inputReadsPerFile = scanner.nextLine();
        int readsPerFile = Integer.parseInt(inputReadsPerFile);

        System.out.print("Please provide a path to a local file containing at least " + inputN + " HopsFS file " +
                (n == 1 ? "path.\n> " : "paths.\n> "));
        String inputPath = scanner.nextLine();

        boolean shuffle = getBooleanFromUser("Shuffle file paths around?");

        int numTrials = getIntFromUser("How many trials should this benchmark be performed?");

        JsonObject payload = new JsonObject();
        payload.addProperty(OPERATION, OP_WEAK_SCALING_READS);
        payload.addProperty("n", n);
        payload.addProperty("readsPerFile", readsPerFile);
        payload.addProperty("inputPath", inputPath);
        payload.addProperty("shuffle", shuffle);

        performDistributedBenchmark(sharedHdfs, numTrials, payload, n, readsPerFile, inputPath, shuffle,
                OP_WEAK_SCALING_READS, null, "Read n Files with n Threads (Weak Scaling - Read)",
                new DistributedBenchmarkOperation() {
                    @Override
                    public DistributedBenchmarkResult call(DistributedFileSystem sharedHdfs, String nameNodeEndpoint,
                                                           int numThreads, int opsPerFile, String inputPath,
                                                           boolean shuffle, int opCode, List<String> directories)
                            throws FileNotFoundException, InterruptedException {
                        return Commands.weakScalingReadsV1(sharedHdfs, numThreads, opsPerFile, inputPath, shuffle, opCode);
                    }
                });
    }

    protected void performDistributedBenchmark(DistributedFileSystem sharedHdfs, int numTrials, JsonObject payload,
                                               int numThreads, int opsPerFile, String inputPath, boolean shuffle,
                                               int opCode, List<String> directories, String commandDescription,
                                               DistributedBenchmarkOperation operation)
            throws InterruptedException, IOException {
        int currentTrial = 0;
        List<AggregatedResult> aggregatedResults = new ArrayList<>(numTrials);
        Double[] results = new Double[numTrials];
        Integer[] cacheHits = new Integer[numTrials];
        Integer[] cacheMisses = new Integer[numTrials];
        while (currentTrial < numTrials) {
            LOG.info("|====| TRIAL #" + currentTrial + " |====|");
            String operationId = UUID.randomUUID().toString();
            int numDistributedResults = followers.size();
            if (followers.size() > 0) {
                payload.addProperty(OPERATION_ID, operationId);
                issueCommandToFollowers(commandDescription, operationId, payload, true);
            }

            DistributedBenchmarkResult localResult = null;
            if (commanderExecutesToo) {localResult = operation.call(sharedHdfs, NAME_NODE_ENDPOINT, numThreads, opsPerFile, inputPath, shuffle, opCode, directories);

                if (localResult == null) {
                    LOG.warn("Local result is null. Aborting.");
                    return;
                }

                localResult.setOperationId(operationId);
            } else {
                LOG.info("Commander is not executing this job.");
            }

            double throughput;
            int aggregatedCacheMisses;
            int aggregatedCacheHits;

            // Wait for followers' results if we had followers when we first started the operation.
            AggregatedResult aggregatedResult = waitForDistributedResult(numDistributedResults, operationId, localResult);
            throughput = aggregatedResult.throughput;
            aggregatedCacheHits = aggregatedResult.cacheHits;
            aggregatedCacheMisses = aggregatedResult.cacheMisses;
            aggregatedResults.set(currentTrial, aggregatedResult);

            results[currentTrial] = throughput;
            cacheHits[currentTrial] = aggregatedCacheHits;
            cacheMisses[currentTrial] = aggregatedCacheMisses;
            currentTrial++;

            if (!(currentTrial >= numTrials)) {
                LOG.info("Trial " + currentTrial + "/" + numTrials + " completed. Performing GC and sleeping for " +
                        postTrialSleepInterval + " ms.");
                performClientVMGarbageCollection();
                Thread.sleep(postTrialSleepInterval);
            }
        }

        System.out.println("\n[THROUGHPUT]");
        DecimalFormat df = new DecimalFormat("#.####");
        for (double throughputResult : results) {
            System.out.println(df.format(throughputResult));
        }
        System.out.println("\n[CACHE HITS] [CACHE MISSES] [HIT RATE]");
        String formatString = "%-12s %-14s %10f";
        for (int i = 0; i < numTrials; i++) {
            System.out.printf((formatString) + "%n", cacheHits[i], cacheMisses[i], ((double)cacheHits[i] / (cacheHits[i] + cacheMisses[i])));
        }

        System.out.println("\nthroughput (ops/sec), cache hits, cache misses, cache hit rate, avg tcp latency, avg http latency, avg combined latency, duration");
        for (AggregatedResult result : aggregatedResults)
            System.out.println(result.metricsString);
    }

    /**
     * Used to automatically establish a number of connections between clients and NameNodes.
     */
    private void establishConnections() throws IOException, InterruptedException {
        List<Integer> numClients = Arrays.asList(1, 2, 4, 8, 16, 24, 32, 48, 64, 80, 96, 112, 128, 144, 160, 176, 192, 208, 224, 240, 256);

        int targetNumConnections = getIntFromUser("How many connections do you want to establish per client VM?");

        if (!numClients.contains(targetNumConnections))
            throw new IllegalArgumentException("The target number of connections must be one of the following: " +
                    StringUtils.join(numClients, ",") + ". You specified: " + targetNumConnections + ".");
        // if (targetNumConnections % 2 != 0)
        //    throw new IllegalArgumentException("The number of connections to be established must be a power of 2.");

        if (targetNumConnections > 256)
            throw new IllegalArgumentException("Targeting more than 256 connections per VM is not supported.");

        System.out.print("Please specify a file path containing HopsFS files to use:\n> ");
        String inputPath = scanner.nextLine();

        int originalPostTrialSleepInterval = postTrialSleepInterval;

        LOG.info("Target connections per VM: " + targetNumConnections);
        LOG.info("Total number of clients: " + ((followers.size() + 1) * targetNumConnections));
        LOG.info("File containing HopsFS file paths: '" + inputPath + "'");

        boolean acceptable = getBooleanFromUser("Are these settings acceptable?");

        if (!acceptable) {
            LOG.warn("Settings were deemed unacceptable. Aborting.");
            return;
        }

        List<Integer> sleepIntervals = Arrays.asList(125, 125, 250, 500, 500, 750, 750, 750, 775, 800, 950, 1250, 1350, 1450, 1550, 1650, 1750, 1850, 1950, 2050, 2150);

        int targetIndex = numClients.indexOf(targetNumConnections);

        for (int i = 0; i <= targetIndex; i++) {
            int numClientsToUse = numClients.get(i);
            postTrialSleepInterval = sleepIntervals.get(i);

            LOG.info("Beginning trials using " + numClientsToUse + " clients per VM. Sleep interval: " +
                    postTrialSleepInterval + " ms.");

            JsonObject payload = new JsonObject();
            payload.addProperty(OPERATION, OP_WEAK_SCALING_READS_V2);
            payload.addProperty("numThreads", numClientsToUse);
            payload.addProperty("filesPerThread", 4);
            payload.addProperty("inputPath", inputPath);
            payload.addProperty("shuffle", true);

            performDistributedBenchmark(primaryHdfs, 2, payload, numClientsToUse,
                    4, inputPath, true, OP_WEAK_SCALING_READS_V2, null,
                    "Weak Scaling Read v2 -- Connection Establishment", new DistributedBenchmarkOperation() {
                        @Override
                        public DistributedBenchmarkResult call(DistributedFileSystem sharedHdfs, String nameNodeEndpoint,
                                                               int numThreads, int opsPerFile, String inputPath,
                                                               boolean shuffle, int opCode, List<String> directories)
                                throws FileNotFoundException, InterruptedException {
                            return Commands.weakScalingBenchmarkV2(sharedHdfs, numThreads,
                                    opsPerFile, inputPath, shuffle, OP_WEAK_SCALING_READS_V2);
                        }
                    });

            LOG.info("Finished iteration " + (i+1) + "/" + (targetIndex+1) + ". Sleeping for 0.5 seconds...");
            Thread.sleep(500);
        }

        // Reset `postTrialSleepInterval` to its original value.
        postTrialSleepInterval = originalPostTrialSleepInterval;
    }

    private int getNextOperation() {
        while (true) {
            try {
                String input = scanner.nextLine();
                return Integer.parseInt(input);
            } catch (NumberFormatException ex) {
                LOG.info("\t Invalid input! Please enter an integer.");
            } catch (Exception ex) {
                ex.printStackTrace();
                System.exit(1);
            }
        }
    }

    /**
     * Create an HDFS client.
     * @param primaryHdfs The main/shared DistributedFileSystem instance. Will be null if we're creating it, of course.
     * @param creatingPrimary Are we creating the primary/shared instance?
     */
    public static DistributedFileSystem initDfsClient(DistributedFileSystem primaryHdfs,
                                                      boolean creatingPrimary) {
        LOG.debug("Creating HDFS client now...");
        Configuration hdfsConfiguration;
        hdfsConfiguration = Utils.getConfiguration(hdfsConfigFilePath);
        //LOG.info("Created configuration.");
        DistributedFileSystem hdfs = new DistributedFileSystem();
        //LOG.info("Created DistributedFileSystem object.");

        try {
            hdfs.initialize(new URI(NAME_NODE_ENDPOINT), hdfsConfiguration);
            //LOG.info("Called initialize() successfully.");
        } catch (URISyntaxException | IOException ex) {
            LOG.error("");
            LOG.error("");
            LOG.error("ERROR: Encountered exception while initializing DistributedFileSystem object.");
            ex.printStackTrace();
            System.exit(1);
        }

        // For the HDFS instance we're creating, toggle the consistency protocol + benchmark mode
        // based on whether the client has toggled those options within the benchmarking application.
        // hdfs.setConsistencyProtocolEnabled(consistencyEnabled);
        // hdfs.setBenchmarkModeEnabled(Commands.BENCHMARKING_MODE);

        // The primary HDFS instance should use whatever the default log level is for the HDFS instance we create,
        // as HopsFS has a default log level. If we're creating a non-primary HDFS instance, then we just assign it
        // whatever our primary instance has been set to (as it can change dynamically).
        if (creatingPrimary) {
            consistencyEnabled = hdfs.getConsistencyProtocolEnabled();
        }
        else {
            hdfs.setServerlessFunctionLogLevel(primaryHdfs.getServerlessFunctionLogLevel());
            hdfs.setConsistencyProtocolEnabled(primaryHdfs.getConsistencyProtocolEnabled());
        }

        return hdfs;
    }

    /**
     * Wrapper around Kryo connection objects in order to track per-connection state without needing to use
     * connection IDs to perform state look-up.
     */
    private static class FollowerConnection extends Connection {
        /**
         * Name of the connection. It's just the unique ID of the NameNode to which we are connected.
         * NameNode IDs are longs, so that's why this is of type long.
         */
        public String name = null; // Hides super type.

        /**
         * Default constructor.
         */
        public FollowerConnection() {

        }

        @Override
        public String toString() {
            return "FollowerConnection[" + name + "]";
        }
    }

    private class ServerListener extends Listener {
        /**
         * Listener handles connection establishment with remote NameNodes.
         */
        @Override
        public void connected(Connection conn) {
            LOG.debug(LEADER_PREFIX + " Connection established with remote NameNode at "
                    + conn.getRemoteAddressTCP());
            conn.setKeepAliveTCP(5000);
            conn.setTimeout(30000);
            followers.add((FollowerConnection) conn);

            LOG.debug("We now have " + followers.size() + " Followers connected.");

            JsonObject registrationPayload = new JsonObject();
            registrationPayload.addProperty(OPERATION, OP_REGISTRATION);
            registrationPayload.addProperty(NAMENODE_ENDPOINT, NAME_NODE_ENDPOINT);
            registrationPayload.addProperty(HDFS_CONFIG_PATH, hdfsConfigFilePath);
            registrationPayload.addProperty(TRACK_OP_PERFORMED, followersTrackOpsPerformed);

            LOG.debug("Sending '" + NAMENODE_ENDPOINT + "' as '" + NAMENODE_ENDPOINT + "'.");
            LOG.debug("Sending '" + HDFS_CONFIG_PATH + "' as '" + hdfsConfigFilePath + "'.");

            conn.sendTCP(new Gson().toJson(registrationPayload));
        }

        /**
         * This listener handles receiving TCP messages from followers.
         * @param conn The connection to the followers.
         * @param object The object that was sent by the followers to the leader (us).
         */
        @Override
        public void received(Connection conn, Object object) {
            FollowerConnection connection = (FollowerConnection) conn;
            String followerName;
            if (connection.name == null) {
                followerName = conn.getRemoteAddressTCP().getHostName();
                connection.name = followerName;
            } else {
                followerName = connection.name;
            }

            if (object instanceof String) {
                try {
                    JsonObject body = JsonParser.parseString((String)object).getAsJsonObject();
                    LOG.debug("Received message from follower: " + body);
                } catch (Exception ex) {
                    LOG.debug("Received message from follower: " + object);
                }
            }
            else if (object instanceof DistributedBenchmarkResult) {
                DistributedBenchmarkResult result = (DistributedBenchmarkResult)object;

                LOG.info("Received result from Follower " + followerName + ": " + result);

                String opId = result.opId;

                BlockingQueue<DistributedBenchmarkResult> resultQueue = resultQueues.get(opId);
                resultQueue.add(result);

                waitingOn.remove(followerName);

                LOG.info("resultQueue.size(): " + resultQueue.size());
            }
            else if (object instanceof WorkloadResponse) {
                WorkloadResponse response = (WorkloadResponse)object;
                workloadResponseQueue.add(response);
                waitingOn.remove(followerName);
            }
            else if (object instanceof FrameworkMessage.KeepAlive) {
                // Do nothing...
            }
            else {
                LOG.error("Received object of unexpected/unsupported type from Follower " + followerName + ": " +
                        object.getClass().getSimpleName());
            }
        }

        public void disconnected(Connection conn) {
            FollowerConnection connection = (FollowerConnection)conn;
            followers.remove(connection);
            numDisconnections.incrementAndGet();
            waitingOn.remove(connection.name);
            if (connection.name != null) {
                LOG.warn("Lost connection to follower at " + connection.name);
                LOG.debug("We now have " + followers.size() + " followers registered.");
                //LOG.info("Trying to re-launch Follower " + connection.name + " now...");
                //launchFollower("ben", connection.name, String.format(LAUNCH_FOLLOWER_CMD, ip, port));
            } else {
                LOG.warn("Lost connection to follower.");
                LOG.debug("We now have " + followers.size() + " followers registered.");
                LOG.error("Follower connection did not have a name. Cannot attempt to re-launch follower.");
            }
        }
    }

    private static void printMenu() {
        System.out.println("\n====== MENU ======");
        System.out.println("Debug Operations:");
        System.out.println(
                "(-14) Set Random HTTP/TCP Replacement Chance\n" +
                "(-13) Establish/Pre-Warm Connections\n" +
                "(-12) Print latency statistics to a file\n" +
                "(-11) Toggle 'Benchmarking Mode' in self and followers\n" +
                "(-10) Toggle 'OperationPerformed' tracking in followers\n" +
                "(-9)  Perform client VM garbage collection\n" +
                "(-8)  Print/modify post-trial sleep interval\n" +
                "(-7)  Print currently active NameNodes\n" +
                "(-6)  Get/set consistency protocol enabled flag.\n(-5)  Get/set serverless log4j debug level.\n" +
                "(-4)  Clear statistics\n(-3)  Output statistics packages to CSV\n" +
                "(-2)  Output operations performed + write to file\n(-1)  Print TCP debug information.");
        System.out.println("\nStandard Operations:");
        System.out.println("(0) Exit\n(1) Create file\n(2) Create directory\n(3) Read contents of file.\n(4) Rename" +
                "\n(5) Delete\n(6) List directory\n(7) Append\n(8) Create Subtree.\n(9) Ping\n(10) Prewarm" +
                "\n(11) Write Files to Directory\n(12) Read files\n(13) Delete files\n(14) Write Files to Directories" +
                "\n(15) Read n Files with n Threads (Weak Scaling - Read)\n(16) Read n Files y Times with z Threads (Strong Scaling - Read)" +
                "\n(17) Write n Files with n Threads (Weak Scaling - Write)\n(18) Write n Files y Times with z Threads (Strong Scaling - Write)" +
                "\n(19) Create n directories one-after-another.\n(20) Weak Scaling Reads v2\n(21) File Stat Benchmark" +
                "\n(22) Unavailable.\n(23) List Directories from File (Weak Scaling)\n(24) Stat File (Weak Scaling)" +
                "\n(25) Weak Scaling (MKDIR).\n(26) Randomly-generated workload.\n(30) Create from file." +
                "\n(31) Reader-Writer Test #1.\n");
        System.out.println("==================\n");
        System.out.println("What would you like to do?");
        System.out.print("> ");
    }

    public static class AggregatedResult {
        public double throughput;
        public double avgTcpLatency;
        public double avgHttpLatency;
        public double avgCombinedLatency;
        public int cacheHits;
        public int cacheMisses;
        public double durationSeconds;
        public int numGCs;        // Number of GCs performed by the NNs.
        public double gcTimeMs;   // Time in milliseconds spent GC-ing by the NNs.

        public static final AggregatedResult DEFAULT_RESULT = new AggregatedResult(0, 0, 0,
                "[INVALID METRICS STRING]", -1, -1, -1,
                -1, -1, -1);

        // Format (for serverless):
        // throughput (ops/sec), cache hits, cache misses, cache hit rate, avg tcp latency, avg http latency, avg combined latency
        public String metricsString; // All the metrics I'd want formatted so that I can copy and paste into Excel.

        public AggregatedResult(double throughput, int cacheHits, int cacheMisses, String metricsString,
                                double avgTcpLatency, double avgHttpLatency, double avgCombinedLatency,
                                double durationSeconds, int numGCs, double gcTimeMs) {
            this.throughput = throughput;
            this.cacheHits = cacheHits;
            this.cacheMisses = cacheMisses;
            this.metricsString = metricsString;
            this.avgHttpLatency = avgHttpLatency;
            this.avgTcpLatency = avgTcpLatency;
            this.avgCombinedLatency = avgCombinedLatency;
            this.durationSeconds = durationSeconds;
        }

        public double getCacheHitRate() {
            if (cacheHits > 0 || cacheMisses > 0) {
                return (double)cacheHits / (cacheHits + cacheMisses);
            }

            return -1;
        }

        @Override
        public String toString() {
            return "Throughput: " + throughput + " ops/sec, Cache Hits: " + cacheHits + ", Cache Misses: " +
                    cacheMisses + ", Cache Hit Rate: " + getCacheHitRate() + ", Average TCP Latency: " + avgTcpLatency +
                    " ms, Average HTTP Latency: " + avgHttpLatency + " ms, Average Combined Latency: " + avgCombinedLatency
                    + " ms, Average duration: " + durationSeconds + " seconds, Number of GCs: " + numGCs +
                    ", Total Time Spent GCing: " + gcTimeMs + " ms.";
        }

        public String toString(DecimalFormat df) {
            if (df == null)
                throw new IllegalArgumentException("DecimalFormat parameter cannot be null.");

            return "Throughput: " + df.format(throughput) + " ops/sec, Cache Hits: " + cacheHits + ", Cache Misses: " +
                    cacheMisses + ", Cache Hit Rate: " + getCacheHitRate() + ", Average TCP Latency: " +
                    df.format(avgTcpLatency) + " ms, Average HTTP Latency: " + df.format(avgHttpLatency) +
                    " ms, Average Combined Latency: " + df.format(avgCombinedLatency) +
                    " ms, Average Duration: " + df.format(durationSeconds) + " seconds, Number of GCs: " + numGCs +
                    ", Total Time Spent GCing: " + gcTimeMs + " ms.";

        }
    }
}
