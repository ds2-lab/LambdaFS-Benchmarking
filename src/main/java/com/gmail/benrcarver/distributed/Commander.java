package com.gmail.benrcarver.distributed;

import com.esotericsoftware.kryonet.Connection;
import com.esotericsoftware.kryonet.FrameworkMessage;
import com.esotericsoftware.kryonet.Listener;
import com.esotericsoftware.kryonet.Server;
import com.gmail.benrcarver.distributed.coin.BMConfiguration;
import com.gmail.benrcarver.distributed.util.Utils;
import com.gmail.benrcarver.distributed.workload.BMOpStats;
import com.gmail.benrcarver.distributed.workload.RandomlyGeneratedWorkload;
import com.gmail.benrcarver.distributed.workload.WorkloadResponse;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.jcraft.jsch.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.math3.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.yaml.snakeyaml.Yaml;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.io.*;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.DecimalFormat;
import java.util.*;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static com.gmail.benrcarver.distributed.Commands.*;
import static com.gmail.benrcarver.distributed.Constants.*;

/**
 * Controls a fleet of distributed machines. Executes HopsFS benchmarks based on user input/commands.
 */
public class Commander {
    public static final Logger LOG = LoggerFactory.getLogger(Commander.class);

    private final List<FollowerConnection> followers;

    private static final int COMMANDER_TCP_BUFFER_SIZES = Follower.FOLLOWER_TCP_BUFFER_SIZES * 4;

    private static final String LEADER_PREFIX = "[LEADER TCP SERVER]";

    /**
     * Use with String.format(LAUNCH_FOLLOWER_CMD, leader_ip, leader_port)
     */
    private static final String LAUNCH_FOLLOWER_CMD = "source ~/.bashrc; cd /home/ubuntu/repos/HopsFS-Benchmarking-Utility; java -Dlog4j.configuration=file:/home/ubuntu/repos/HopsFS-Benchmarking-Utility/src/main/resources/log4j.properties -Dsun.io.serialization.extendedDebugInfo=true -Xmx58g -Xms58g -XX:+UseConcMarkSweepGC -XX:+UnlockDiagnosticVMOptions -XX:ParGCCardsPerStrideChunk=32768 -XX:+CMSScavengeBeforeRemark -XX:MaxGCPauseMillis=350 -XX:MaxTenuringThreshold=2 -XX:MaxNewSize=32000m -XX:+CMSClassUnloadingEnabled -XX:+UseCMSInitiatingOccupancyOnly -XX:CMSInitiatingOccupancyFraction=75 -XX:+ScavengeBeforeFullGC -verbose:gc -XX:+PrintGCTimeStamps -XX:+PrintGCDetails -cp \".:target/HopsFSBenchmark-1.0-jar-with-dependencies.jar:/home/ben/repos/hops/hadoop-dist/target/hadoop-3.2.0.2-RC0/share/hadoop/hdfs/lib/*:/home/ben/repos/hops/hadoop-dist/target/hadoop-3.2.0.2-RC0/share/hadoop/common/lib/*:/home/ben/repos/hops/hadoop-hdfs-project/hadoop-hdfs-client/target/hadoop-hdfs-client-3.2.0.2-RC0.jar:/home/ben/repos/hops/hops-leader-election/target/hops-leader-election-3.2.0.2-RC0.jar:/home/ben/openwhisk-runtime-java/core/java8/libs/*:/home/ben/repos/hops/hadoop-hdfs-project/hadoop-hdfs/target/hadoop-hdfs-3.2.0.2-RC0.jar:/home/ben/repos/hops/hadoop-common-project/hadoop-common/target/hadoop-common-3.2.0.2-RC0.jar\" com.gmail.benrcarver.distributed.InteractiveTest --leader_ip %s --leader_port %d --yaml_path /home/ubuntu/repos/HopsFS-Benchmarking-Utility/config.yaml --worker";

    private static final String BENCHMARK_JAR_PATH = "/home/ubuntu/repos/HopsFS-Benchmarking-Utility/target/HopsFSBenchmark-1.0-jar-with-dependencies.jar";

    private static final String HADOOP_HDFS_JAR_PATH = "/home/ben/repos/hops/hadoop-hdfs-project/hadoop-hdfs/target/hadoop-hdfs-3.2.0.2-RC0.jar";

    /**
     * Has a default value.
     */
    protected static String NAME_NODE_ENDPOINT = "hdfs://ip-10-0-0-4.ec2.internal:8020/";

    /**
     * Used to obtain input from the user.
     */
    private final Scanner scanner = new Scanner(System.in);

    /**
     * Used to communicate with followers.
     */
    private final Server tcpServer;

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
    private int postTrialSleepInterval = 3500;

    /**
     * Fully-qualified path of hdfs-site.xml configuration file.
     */
    public static String hdfsSiteConfigFilePath;

    /**
     * Fully-qualified path of core-site.xml configuration file.
     */
    public static String coreSiteConfigFilePath;

    /**
     * Map from operation ID to the queue in which distributed results should be placed by the TCP server.
     */
    private final ConcurrentHashMap<String, BlockingQueue<DistributedBenchmarkResult>> resultQueues;

    /**
     * The {@link Commander} class uses a singleton pattern.
     */
    private static Commander instance;

    public static DistributedFileSystem PRIMARY_HDFS;

    /**
     * The approximate number of collections that occurred.
     */
    private long numGarbageCollections = 0L;

    private final boolean nondistributed;

    /**
     * The approximate time, in milliseconds, that has elapsed during GCs
     */
    private long garbageCollectionTime = 0L;

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

    private final BlockingQueue<WorkloadResponse> workloadResponseQueue;

    public static Commander getOrCreateCommander(String ip, int port, String yamlPath, boolean nondistributed,
                                                 int numFollowers, boolean scpJars, boolean scpConfig,
                                                 boolean manuallyLaunchFollowers) throws IOException, JSchException {
        if (instance == null) {
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
        this.nondistributed = nondistributed;
        // TODO: Maybe do book-keeping or fault-tolerance here.
        this.followers = new ArrayList<>();
        this.resultQueues = new ConcurrentHashMap<>();
        this.numFollowersFromConfigToStart = numFollowersFromConfigToStart;
        this.scpJars = scpJars;
        this.scpConfig = scpConfig;
        this.manuallyLaunchFollowers = manuallyLaunchFollowers;
        this.workloadResponseQueue = new ArrayBlockingQueue<>(16);

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

    /**
     * Create all the directories listed in the file located at {@code filePath}.
     * @param filePath Path to the file containing HopsFS paths of directories to be created.
     * @param numThreads Number of threads to use when creating the directories.
     *
     * @throws FileNotFoundException If there is no file located at the path specified by {@code filePath}.
     */
    protected DistributedBenchmarkResult createDirectoriesFromFile(String filePath, int numThreads)
            throws FileNotFoundException, InterruptedException {
        String[] directories = Utils.getFilePathsFromFile(filePath).toArray(new String[0]);

        int numWritesPerThread = directories.length / numThreads;
        final String[][] directoriesPerThread = Utils.splitArray(directories, numWritesPerThread);

        assert(directoriesPerThread != null);
        assert(directoriesPerThread.length == numThreads);

        if (directories.length > 1)
            LOG.info("Creating " + directories.length + " directories now...");
        else
            LOG.info("Creating " + directories.length + " directory now...");

        long start = System.currentTimeMillis();
        DistributedBenchmarkResult res = Commands.executeBenchmark(numThreads, directoriesPerThread,
                1, OP_WRITE_FILES_TO_DIRS, new FSOperation(NAME_NODE_ENDPOINT) {
                    @Override
                    public boolean call(DistributedFileSystem hdfs, String path, String content) {
                        return Commands.mkdir(path, hdfs);
                    }
                });
        long end = System.currentTimeMillis();
        long duration = end - start;

        if (directories.length > 1)
            LOG.info("Created all " + directories.length + " directories in " + duration + " ms.");
        else
            LOG.info("Created " + directories.length + " directory in " + duration + " ms.");

        LOG.info("========== DIRS RESULT ==========");
        LOG.info("Num Ops Performed   : " + res.numOpsPerformed);
        LOG.info("Duration (sec)      : " + res.durationSeconds);
        LOG.info("Throughput          : " + res.getOpsPerSecond());

        if (res.latencyStatistics != null) {
            DescriptiveStatistics latency = new DescriptiveStatistics(res.latencyStatistics);

            LOG.info("Latency (ms) [min: " + latency.getMin() + ", max: " + latency.getMax() +
                    ", avg: " + latency.getMean() + ", std dev: " + latency.getStandardDeviation() +
                    ", N: " + latency.getN() + "]");
        }

        return res;
    }

    /**
     * Create all the files listed in the file located at {@code filePath}.
     * @param filePath Path to the file containing HopsFS paths of files to be created.
     * @param numThreads Number of threads to use when creating the directories.
     *
     * @throws FileNotFoundException If there is no file located at the path specified by {@code filePath}.
     */
    protected DistributedBenchmarkResult createEmptyFilesFromFile(String filePath, int numThreads)
            throws FileNotFoundException, InterruptedException {
        String[] files = Utils.getFilePathsFromFile(filePath).toArray(new String[0]);

        int numWritesPerThread = files.length / numThreads;
        final String[][] directoriesPerThread = Utils.splitArray(files, numWritesPerThread);

        assert(directoriesPerThread != null);
        assert(directoriesPerThread.length == numThreads);

        if (files.length > 1)
            LOG.info("Creating " + files.length + " directories now...");
        else
            LOG.info("Creating " + files.length + " directory now...");

        long start = System.currentTimeMillis();
        DistributedBenchmarkResult res = Commands.executeBenchmark(numThreads, directoriesPerThread,
                1, OP_WRITE_FILES_TO_DIRS, new FSOperation(NAME_NODE_ENDPOINT) {
                    @Override
                    public boolean call(DistributedFileSystem hdfs, String path, String content) {
                        // We're hard-coding the empty string here to ensure the file is empty.
                        return Commands.createFile(path, "", hdfs);
                    }
                });
        long end = System.currentTimeMillis();
        long duration = end - start;

        if (files.length > 1)
            LOG.info("Created all " + files.length + " files in " + duration + " ms.");
        else
            LOG.info("Created " + files.length + " file in " + duration + " ms.");

        LOG.info("========== FILES RESULT ==========");
        LOG.info("Num Ops Performed   : " + res.numOpsPerformed);
        LOG.info("Duration (sec)      : " + res.durationSeconds);
        LOG.info("Throughput          : " + res.getOpsPerSecond());

        if (res.latencyStatistics != null) {
            DescriptiveStatistics latency = new DescriptiveStatistics(res.latencyStatistics);

            LOG.info("Latency (ms) [min: " + latency.getMin() + ", max: " + latency.getMax() +
                    ", avg: " + latency.getMean() + ", std dev: " + latency.getStandardDeviation() +
                    ", N: " + latency.getN() + "]");
        }

        return res;
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
            hdfsSiteConfigFilePath = config.getHdfsConfigFile();
            coreSiteConfigFilePath = config.getCoreSiteConfigFile();

            LOG.info("Loaded configuration!");
            LOG.debug("NameNode endpoint: " + NAME_NODE_ENDPOINT);
            LOG.debug("hdfs-site.xml configuration file path: " + hdfsSiteConfigFilePath);
            LOG.debug("core-site.xml configuration file path: " + coreSiteConfigFilePath);
            LOG.info(String.valueOf(config));
        }
    }

    public void startNoLoop() throws IOException {
        if (!nondistributed) {
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
    }

    public void start() throws IOException, InterruptedException {
        startNoLoop();
        interactiveLoop();
    }

    private void executeCommand(String user, String host, String launchCommand) {
        java.util.Properties sshConfig = new java.util.Properties();
        sshConfig.put("StrictHostKeyChecking", "no");

        Session session;
        try {
            session = jsch.getSession(user, host, 22);
            session.setConfig(sshConfig);
            session.connect();

            Channel channel = session.openChannel("exec");
            ((ChannelExec) channel).setCommand(launchCommand);
            channel.setInputStream(null);
            ((ChannelExec) channel).setErrStream(System.err);

            channel.connect();
            channel.disconnect();
            session.disconnect();
            System.out.println("Finished executing command \"" + launchCommand + "\" at " + user + "@" + host + ".");
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

            if (scpJars) {
                LOG.debug("SFTP-ing hadoop-hdfs-3.2.0.3-SNAPSHOT.jar to Follower " + host + " now.");
                ChannelSftp sftpChannel = (ChannelSftp) session.openChannel("sftp");
                sftpChannel.connect();
                sftpChannel.put(HADOOP_HDFS_JAR_PATH, HADOOP_HDFS_JAR_PATH);
                sftpChannel.disconnect();

                LOG.debug("SFTP'd hadoop-hdfs-3.2.0.3-SNAPSHOT.jar to Follower " + host + ".");

                LOG.debug("SFTP-ing HopsFSBenchmark-1.0-jar-with-dependencies.jar to Follower " + host + " now.");
                sftpChannel = (ChannelSftp) session.openChannel("sftp");
                sftpChannel.connect();
                sftpChannel.put(BENCHMARK_JAR_PATH, BENCHMARK_JAR_PATH);
                sftpChannel.disconnect();
                LOG.debug("SFTP'd HopsFSBenchmark-1.0-jar-with-dependencies.jar to Follower " + host + ".");
            }

            if (scpConfig) {
                LOG.debug("SFTP-ing hdfs-site.xml to Follower " + host + " now.");
                ChannelSftp sftpChannel = (ChannelSftp) session.openChannel("sftp");
                sftpChannel.connect();
                sftpChannel.put(hdfsSiteConfigFilePath, hdfsSiteConfigFilePath);
                sftpChannel.disconnect();
                LOG.debug("SFTP'd hdfs-site.xml to Follower " + host + ".");

                LOG.debug("SFTP-ing core-site.xml to Follower " + host + " now.");
                sftpChannel = (ChannelSftp) session.openChannel("sftp");
                sftpChannel.connect();
                sftpChannel.put(coreSiteConfigFilePath, coreSiteConfigFilePath);
                sftpChannel.disconnect();
                LOG.debug("SFTP'd core-site.xml to Follower " + host + ".");

                LOG.debug("SFTP-ing log4j.properties to Follower " + host + " now.");
                sftpChannel = (ChannelSftp) session.openChannel("sftp");
                sftpChannel.connect();
                String log4jPath = "/home/ben/repos/HopsFS-Benchmarking-Utility/src/main/resources/log4j.properties";
                sftpChannel.put(log4jPath, log4jPath);
                sftpChannel.disconnect();
                LOG.debug("SFTP'd log4j.properties to Follower " + host + ".");
            }

//            LOG.debug("SFTP-ing '109200' file to Follower " + host + " now.");
//            ChannelSftp sftpChannel = (ChannelSftp) session.openChannel("sftp");
//            sftpChannel.connect();
//            sftpChannel.put("/home/ben/repos/HopsFS-Benchmarking-Utility/109200", "/home/ben/repos/HopsFS-Benchmarking-Utility/109200");
//            sftpChannel.disconnect();
//            LOG.debug("SFTP'd '109200' file to Follower " + host + ".");

            if (!manuallyLaunchFollowers)
                executeCommand(user, host, launchCommand);
            else
                LOG.debug("'Manually Launch Followers' is set to TRUE. Commander will not auto-launch Follower.");
        } catch (JSchException | SftpException e) {
            LOG.error("Exception encountered while trying to launch follower at " + user + "@" + host + ":", e);
            e.printStackTrace();
        }
    }

    /**
     * Using SSH, launch the follower processes.
     */
    private void launchFollowers() throws IOException, JSchException {
        final String launchCommand = String.format(LAUNCH_FOLLOWER_CMD, ip, port);

        // If 'numFollowersFromConfigToStart' is negative, then use all followers.
        if (numFollowersFromConfigToStart < 0)
            numFollowersFromConfigToStart = followerConfigs.size();

        LOG.info("Starting " + numFollowersFromConfigToStart + " follower(s) now...");

        for (int i = 0; i < numFollowersFromConfigToStart; i++) {
            FollowerConfig config = followerConfigs.get(i);
            LOG.info("Starting follower at " + config.getUser() + "@" + config.getIp() + " now.");

            // Don't kill Java processes if we're not auto-launching Followers. We might kill the user's process.
            if (!manuallyLaunchFollowers) {
                LOG.debug("Killing running Java processes at " + config.getUser() + "@" + config.getIp() + " now.");
                executeCommand(config.getUser(), config.getIp(), "pkill -9 java");
            }

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

        PRIMARY_HDFS = initDfsClient(NAME_NODE_ENDPOINT);

        while (true) {
            updateGCMetrics();
            long startingGCs = numGarbageCollections;
            long startingGCTime = garbageCollectionTime;

            try {
                printMenu();
                int op = getNextOperation();

                switch (op) {
                    case OP_SAVE_LATENCIES_TO_FILE:
                        saveLatenciesToFile();
                        break;
                    case OP_TRIGGER_CLIENT_GC:
                        performClientVMGarbageCollection();
                        break;
                    case OP_GET_ABSOLUTE_GC_INFO:
                        getGarbageCollectionInformation(true);
                        break;
                    case OP_GET_RELATIVE_GC_INFO:
                        getGarbageCollectionInformation(false);
                        break;
                    case OP_CHANGE_SLEEP_INTERVAL:
                        changeSleepInterval();
                        break;
                    case OP_CLEAR_METRIC_DATA:
                        LOG.info("Clearing metric data (including latencies) now...");
                        Commands.clearMetricData(PRIMARY_HDFS);

                        if (!nondistributed) {
                            JsonObject payload = new JsonObject();
                            String operationId = UUID.randomUUID().toString();
                            payload.addProperty(OPERATION, OP_CLEAR_METRIC_DATA);
                            payload.addProperty(OPERATION_ID, operationId);

                            issueCommandToFollowers("Clear Metric Data", operationId, payload);
                        }

                        break;
                    case OP_EXIT:
                        LOG.info("Exiting now... goodbye!");
                        try {
                            PRIMARY_HDFS.close();
                        } catch (IOException ex) {
                            LOG.info("Encountered exception while closing file system...");
                            ex.printStackTrace();
                        }
                        stopServer();
                        System.exit(0);
                    case OP_CREATE_FILE:
                        LOG.info("CREATE FILE selected!");
                        Commands.createFileOperation(PRIMARY_HDFS);
                        break;
                    case OP_MKDIR:
                        LOG.info("MAKE DIRECTORY selected!");
                        Commands.mkdirOperation(PRIMARY_HDFS);
                        break;
                    case OP_READ_FILE:
                        LOG.info("READ FILE selected!");
                        Commands.readOperation(PRIMARY_HDFS);
                        break;
                    case OP_RENAME:
                        LOG.info("RENAME selected!");
                        Commands.renameOperation(PRIMARY_HDFS);
                        break;
                    case OP_DELETE:
                        LOG.info("DELETE selected!");
                        Commands.deleteOperation(PRIMARY_HDFS);
                        break;
                    case OP_LIST:
                        LOG.info("LIST selected!");
                        Commands.listOperation(PRIMARY_HDFS);
                        break;
                    case OP_APPEND:
                        LOG.info("APPEND selected!");
                        Commands.appendOperation(PRIMARY_HDFS);
                        break;
                    case OP_CREATE_SUBTREE:
                        LOG.info("CREATE SUBTREE selected!");
                        Commands.createSubtree(PRIMARY_HDFS);
                        break;
                    case OP_PING:
                        LOG.warn("PING operation is not supported for Vanilla HopsFS.");
                        break;
                    case OP_PREWARM:
                        LOG.warn("The PREWARM operation is NOT supported for Vanilla HopsFS.");
                        break;
                    case OP_WRITE_FILES_TO_DIR:
                        LOG.info("WRITE FILES TO DIRECTORY selected!");
                        Commands.writeFilesToDirectory();
                        break;
                    case OP_READ_FILES:
                        LOG.info("READ FILES selected!");
                        Commands.readFilesOperation();
                        break;
                    case OP_DELETE_FILES:
                        LOG.info("DELETE FILES selected!");
                        Commands.deleteFilesOperation(PRIMARY_HDFS);
                        break;
                    case OP_WRITE_FILES_TO_DIRS:
                        LOG.info("WRITE FILES TO DIRECTORIES selected!");
                        Commands.writeFilesToDirectories();
                        break;
                    case OP_WEAK_SCALING_READS:
                        LOG.info("'Read n Files with n Threads (Weak Scaling - Read)' selected!");
                        weakScalingReadOperation();
                        break;
                    case OP_STRONG_SCALING_READS:
                        LOG.info("'Read n Files y Times with z Threads (Strong Scaling - Read)' selected!");
                        strongScalingReadOperation();
                        break;
                    case OP_WEAK_SCALING_WRITES:
                        LOG.info("'Write n Files with n Threads (Weak Scaling - Write)' selected!");
                        weakScalingWriteOperation();
                        break;
                    case OP_STRONG_SCALING_WRITES:
                        LOG.info("'Write n Files y Times with z Threads (Strong Scaling - Write)' selected!");
                        strongScalingWriteOperation();
                        break;
                    case OP_CREATE_DIRECTORIES:
                        LOG.info("CREATE DIRECTORIES selected!");
                        Commands.createDirectories(PRIMARY_HDFS);
                        break;
                    case OP_WEAK_SCALING_READS_V2:
                        LOG.info("WeakScalingReadsV2 Selected!");
                        weakScalingReadOperationV2();
                        break;
                    case OP_LIST_DIRECTORIES_FROM_FILE:
                        LOG.info("LIST DIRECTORIES FROM FILE selected!");
                        listDirectoriesFromFile();
                        break;
                    case OP_STAT_FILES_WEAK_SCALING:
                        LOG.info("STAT FILES WEAK SCALING selected!");
                        statFilesWeakScaling();
                        break;
                    case OP_MKDIR_WEAK_SCALING:
                        LOG.info("MKDIR WEAK SCALING selected!");
                        mkdirWeakScaling();
                        break;
                    case OP_PREPARE_GENERATED_WORKLOAD:
                        LOG.info("Randomly-Generated Workload selected!");
                        randomlyGeneratedWorkload(PRIMARY_HDFS);
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

    private Pair<Long, DescriptiveStatistics> getGarbageCollectionInformation(boolean getAbsoluteInfo) throws IOException {
        Pair<Long, DescriptiveStatistics> gcInfo;

        if (getAbsoluteInfo) {
            LOG.info("Getting absolute GC information.");
            gcInfo = PRIMARY_HDFS.getAbsoluteGCInformation();

            LOG.info("There have been a total of " + gcInfo.getFirst() + " GC events across all NNs thus far.");
            LOG.info("Combined absolute time spent GCing: " + gcInfo.getSecond().getSum() + " ms");
            LOG.info("Absolute time spent GCing per NameNode:");
            for (double val : gcInfo.getSecond().getValues())
                LOG.info(val + " ms");
        }
        else {
            LOG.info("Getting relative GC information.");
            gcInfo = PRIMARY_HDFS.getRelativeGCInformation();

            LOG.info("There have been a total of " + gcInfo.getFirst() +
                    " GC events across all NNs since the previous query for relative GC information.");
            LOG.info("Combined relative time spent GCing: " + gcInfo.getSecond().getSum() + " ms");
            LOG.info("Relative time spent GCing per NameNode:");
            for (double val : gcInfo.getSecond().getValues())
                LOG.info(val + " ms");
        }

        return gcInfo;
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

        final int expectedNumResponses = followers.size();

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

                if (decision)
                    break;

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

                if (decision)
                    break;

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

                if (decision)
                    break;

                counter = 0;
            }
        }

        DescriptiveStatistics latencyStatistics = new DescriptiveStatistics(localResult.latencyStatistics);
        if (PRIMARY_HDFS != null)
            PRIMARY_HDFS.addLatencyValues(latencyStatistics.getValues());

        AggregatedResult aggregatedResult;
        if (expectedNumResponses < 1) {
            LOG.info("The number of distributed results is 1. We have nothing to wait for.");

            String metricsString;
            try {
                metricsString = String.format("%f %f %f", localResult.getOpsPerSecond(),
                        new DescriptiveStatistics(localResult.latencyStatistics).getMean(),
                        localResult.durationSeconds);
            } catch (NullPointerException ex) {
                LOG.warn("Could not generate metrics string due to NPE.");
                metricsString = "";
            }

            double avgLatency = 0.0;

            if (localResult.latencyStatistics != null)
                avgLatency = new DescriptiveStatistics(localResult.latencyStatistics).getMean();

            aggregatedResult = new AggregatedResult(localResult.getOpsPerSecond(), avgLatency,
                    metricsString, localResult.opsStats, localResult.durationSeconds);
        } else {
            LOG.info("Expecting " + expectedNumResponses + " distributed results.");
            aggregatedResult = extractDistributedResultFromQueue(resultQueues.get(operationId), localResult,
                    expectedNumResponses);
        }

        System.out.println("throughput (ops/sec), cache hits, cache misses, cache hit rate, avg tcp latency, avg http latency, avg combined latency, duration (seconds)");
        System.out.println(aggregatedResult.metricsString);

        String outputDirectory = "./random_workload_data/" + operationId;
        File dir = new File(outputDirectory);
        dir.mkdirs();

        // Write workload summary to a file.
        Utils.writeAggregatedResultToFile(aggregatedResult, outputDirectory);
    }

    /**
     * Extract results from queue and aggregate them together. Return the aggregated result.
     *
     * @param resultQueue The queue from which to extract results.
     * @param localResult The result from local execution.
     * @param numDistributedResults Expected number of results from queue.
     */
    private AggregatedResult extractDistributedResultFromQueue(
            BlockingQueue<DistributedBenchmarkResult> resultQueue,
            DistributedBenchmarkResult localResult,
            int numDistributedResults) {
        DescriptiveStatistics opsPerformed = new DescriptiveStatistics();
        DescriptiveStatistics duration = new DescriptiveStatistics();
        DescriptiveStatistics throughput = new DescriptiveStatistics();

        opsPerformed.addValue(localResult.numOpsPerformed);
        duration.addValue(localResult.durationSeconds);
        throughput.addValue(localResult.getOpsPerSecond());

        LOG.info("========== LOCAL RESULT ==========");
        LOG.info("Num Ops Performed   : " + localResult.numOpsPerformed);
        LOG.info("Duration (sec)      : " + localResult.durationSeconds);
        LOG.info("Throughput          : " + localResult.getOpsPerSecond() + "\n");

        DescriptiveStatistics statistics = new DescriptiveStatistics(localResult.latencyStatistics);
        double trialAvgTcpLatency =
                statistics.getN() > 0 ? statistics.getMean() : 0;

        Map<String, List<BMOpStats>> opStats = new HashMap<>();

        LOG.info("Result queue contains " + resultQueue.size() + " distributed results.");
        List<Double> allThroughputValues = new ArrayList<>(resultQueue.size() + 1);
        allThroughputValues.add(localResult.getOpsPerSecond());
        for (DistributedBenchmarkResult res : resultQueue) {
            LOG.info("========== RECEIVED RESULT FROM " + res.jvmId + " ==========");
            LOG.info("Num Ops Performed   : " + res.numOpsPerformed);
            LOG.info("Duration (sec)      : " + res.durationSeconds);
            LOG.info("Throughput          : " + res.getOpsPerSecond());

            opsPerformed.addValue(res.numOpsPerformed);
            duration.addValue(res.durationSeconds);
            throughput.addValue(res.getOpsPerSecond());

            if (res.latencyStatistics != null) {
                DescriptiveStatistics latencyTcp = new DescriptiveStatistics(res.latencyStatistics);
                PRIMARY_HDFS.addLatencyValues(latencyTcp.getValues());

                LOG.info("Latency TCP (ms) [min: " + latencyTcp.getMin() + ", max: " + latencyTcp.getMax() +
                        ", avg: " + latencyTcp.getMean() + ", std dev: " + latencyTcp.getStandardDeviation() +
                        ", N: " + latencyTcp.getN() + "]");

                trialAvgTcpLatency += (latencyTcp.getN() > 0 ? latencyTcp.getMean() : 0);
            }

            allThroughputValues.add(res.getOpsPerSecond());

            if (res.opsStats != null) {
                for (Map.Entry<String, List<BMOpStats>> entry : res.opsStats.entrySet()) {
                    String key = entry.getKey(); // op name
                    List<BMOpStats> remoteStats = entry.getValue();

                    List<BMOpStats> localStats = opStats.computeIfAbsent(key, k -> new ArrayList<>());
                    localStats.addAll(remoteStats);
                }
            }
        }

        trialAvgTcpLatency = trialAvgTcpLatency / (1 + numDistributedResults);   // Add 1 to account for local result
        double aggregateThroughput = (opsPerformed.getSum() / duration.getMean());

        LOG.info("");
        LOG.info("==== AGGREGATED RESULTS ====");
        LOG.info("Average Duration: " + duration.getMean() * 1000.0 + " ms.");
        LOG.info("Average TCP latency: " + trialAvgTcpLatency + " ms");
        LOG.info("Aggregate Throughput (ops/sec): " + aggregateThroughput);

        DecimalFormat df = new DecimalFormat("#.####");
        String metricsString = String.format("%s %s %s %s",
                df.format(aggregateThroughput), df.format(trialAvgTcpLatency), df.format(duration.getMean()),
                df.format(duration.getMean()));

        LOG.info(metricsString);

        // Create a copy and sort it so the estimated results are ordered by largest to smallest throughput.
        Collections.sort(allThroughputValues);
        System.out.println("\n['Estimated' Throughput]");
        String format = "%10.2f --> %10.2f";
        for (double throughputResult : allThroughputValues) {
            String formatted = String.format(format, throughputResult, throughputResult * allThroughputValues.size());
            System.out.println(formatted);
        }

        return new AggregatedResult(aggregateThroughput, trialAvgTcpLatency, metricsString, opStats, duration.getMean());
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
                success = Commands.mkdir(fileOrDir, PRIMARY_HDFS);
            else
                success = Commands.createFile(fileOrDir, "", PRIMARY_HDFS);

            if (success)
                numDirsCreated++;
        }

        LOG.debug("Successfully created " + numDirsCreated + "/" + filesAndDirectories.size() +
                " files/directories in " + (System.currentTimeMillis() - start) + " ms.");
    }

    private String getStringFromUser(String prompt) {
        System.out.print(prompt + "\n> ");
        String input = scanner.nextLine();
        checkForExit(input);
        return input;
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

            boolean readPathExists = Commands.exists(PRIMARY_HDFS, readPath);

            if (!readPathExists) {
                LOG.info("Creating read directory '" + readPath + "' now...");
                Commands.mkdir(readPath, PRIMARY_HDFS);
            }

            LOG.info("Creating files to be read by readers now...");
            String[] fileNames = Utils.getFixedLengthRandomStrings(numFilesToRead, 8);
            readerFiles = new ArrayList<>(); // Successfully-created files.
            long createStart = System.currentTimeMillis();
            for (String fileName : fileNames) {
                String fullPath = readPath + "/" + fileName;
                boolean created = FSOperation.CREATE_FILE.call(PRIMARY_HDFS, fullPath, "");

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

        boolean writePathExists = Commands.exists(PRIMARY_HDFS, writePath);

        if (!writePathExists) {
            LOG.info("Creating write directory '" + writePath + "' now...");
            Commands.mkdir(writePath, PRIMARY_HDFS);
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

        // Keep track of number of successful operations.
        AtomicInteger numSuccessfulOps = new AtomicInteger(0);
        AtomicInteger numOps = new AtomicInteger(0);

        for (int i = 0; i < numReaders; i++) {
            int threadId = i;
            Thread readerThread = new Thread(() -> {
                LOG.info("Reader thread " + threadId + " started.");
                DistributedFileSystem hdfs = Commands.getHdfsClient();

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

                DescriptiveStatistics latencyStatistics = hdfs.getLatencyStatistics();

                if (Commander.PRIMARY_HDFS != null)
                    Commander.PRIMARY_HDFS.addLatencyValues(latencyStatistics.getValues());

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
                DistributedFileSystem hdfs = Commands.getHdfsClient();

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

                DescriptiveStatistics latencyStatistics = hdfs.getLatencyStatistics();

                if (Commander.PRIMARY_HDFS != null)
                    Commander.PRIMARY_HDFS.addLatencyValues(latencyStatistics.getValues());

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

        double durationSeconds = (end - start) / 1.0e3;

        // TODO: Verify that I've calculated the total number of operations correctly.
        int numSuccess = numSuccessfulOps.get();
        double totalOperations = numOps.get();
        LOG.info("Finished performing all " + totalOperations + " operations in " + durationSeconds + " sec.");
        double totalThroughput = totalOperations / durationSeconds;
        double successThroughput = numSuccess / durationSeconds;
        LOG.info("Number of successful operations: " + numSuccess);
        LOG.info("Number of failed operations: " + (totalOperations - numSuccess));

        LOG.info("Total Throughput: " + totalThroughput + " ops/sec.");
        LOG.info("Successful Throughput: " + successThroughput + " ops/sec.");

        if (LOG.isDebugEnabled())
            LOG.debug("At end of benchmark, the HDFS Clients Cache has " + hdfsClients.size() + " clients.");
    }

    private void mkdirWeakScaling() throws InterruptedException, IOException {
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

        // Establish baseline for number of GCs.
        PRIMARY_HDFS.getRelativeGCInformation();

        int currentTrial = 0;
        AggregatedResult[] aggregatedResults = new AggregatedResult[numTrials];
        double[] results = new double[numTrials];

        while (currentTrial < numTrials) {
            LOG.info("|====| TRIAL #" + currentTrial + " |====|");

            String operationId = UUID.randomUUID().toString();
            int numDistributedResults = followers.size();
            if (followers.size() > 0) {
                JsonObject payload = new JsonObject();
                payload.addProperty(OPERATION, OP_MKDIR_WEAK_SCALING);
                payload.addProperty(OPERATION_ID, operationId);
                payload.addProperty("n", mkdirsPerThread);
                payload.addProperty("numberOfThreads", numberOfThreads);
                payload.addProperty("randomMkdirs", directoryChoice == 3);
                payload.addProperty("writePathsToFile", writePathsToFile);

                JsonArray directoriesJson = new JsonArray();
                for (String dir : directories)
                    directoriesJson.add(dir);

                payload.add("directories", directoriesJson);

                issueCommandToFollowers("Write n Files with n Threads (Weak Scaling - Write)", operationId, payload);
            }

            LOG.info("Each thread should be writing " + mkdirsPerThread + " files...");
            DistributedBenchmarkResult localResult =
                    Commands.mkdirWeakScaling(mkdirsPerThread, numberOfThreads, directories,
                            OP_MKDIR_WEAK_SCALING, (directoryChoice == 3),
                            operationId, writePathsToFile);
            LOG.info("Received local result...");
            localResult.setOperationId(operationId);
            localResult.setOperation(OP_MKDIR_WEAK_SCALING);

            //LOG.info("LOCAL result of weak scaling benchmark: " + localResult);
            localResult.setOperationId(operationId);

            // Wait for followers' results if we had followers when we first started the operation.
            AggregatedResult aggregatedResult = waitForDistributedResult(numDistributedResults, operationId, localResult);

            aggregatedResults[currentTrial] = aggregatedResult;
            results[currentTrial] = aggregatedResult.throughput;
            currentTrial++;

            Pair<Long, DescriptiveStatistics> gcInfo = PRIMARY_HDFS.getRelativeGCInformation();

            aggregatedResult.numGCs = gcInfo.getFirst();
            aggregatedResult.timeSpentGCing = gcInfo.getSecond().getSum();

            LOG.info("Number of GCs: " + gcInfo.getFirst());
            LOG.info("Time spent GC-ing: " + gcInfo.getSecond().getSum() + " ms");

            if (!(currentTrial >= numTrials)) {
                LOG.info("Trial " + currentTrial + "/" + numTrials + " completed. Performing client GC and sleeping for " +
                        postTrialSleepInterval + " ms.");
                performClientVMGarbageCollection();
                Thread.sleep(postTrialSleepInterval);
            }
        }

        System.out.println("[THROUGHPUT ONLY]");
        for (double throughputResult : results) {
            System.out.println(throughputResult);
        }

        System.out.println("\nThroughput, Total Duration (sec), Average Latency (ms), Number of GCs, Time Spent GC-ing (ms)");
        for (AggregatedResult result : aggregatedResults)
            System.out.println(result.metricsString + " " + result.numGCs + " " + result.timeSpentGCing);
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

        File fileMerged = new File(directoryPath + "/" + fileName + ".dat");
        DescriptiveStatistics latencyStatistics = PRIMARY_HDFS.getLatencyStatistics();

        try {
            FileWriter fw = new FileWriter(fileMerged.getAbsoluteFile());
            BufferedWriter bw = new BufferedWriter(fw);

            for (double mergedLatency : latencyStatistics.getValues()) {
                bw.write(String.valueOf(mergedLatency));
                bw.write("\n");
            }

            bw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void statFilesWeakScaling() throws InterruptedException, IOException {
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

        // Establish baseline for number of GCs.
        PRIMARY_HDFS.getRelativeGCInformation();

        int currentTrial = 0;
        AggregatedResult[] aggregatedResults = new AggregatedResult[numTrials];
        Double[] results = new Double[numTrials];
        while (currentTrial < numTrials) {
            LOG.info("|====| TRIAL #" + currentTrial + " |====|");
            String operationId = UUID.randomUUID().toString();
            int numDistributedResults = followers.size();
            if (followers.size() > 0) {
                JsonObject payload = new JsonObject();
                payload.addProperty(OPERATION, OP_STAT_FILES_WEAK_SCALING);
                payload.addProperty(OPERATION_ID, operationId);
                payload.addProperty("numThreads", numThreads);
                payload.addProperty("filesPerThread", filesPerThread);
                payload.addProperty("inputPath", inputPath);
                payload.addProperty("shuffle", shuffle);

                issueCommandToFollowers("Read n Files with n Threads (Weak Scaling - Read)", operationId, payload);
            }

            // TODO: Make this return some sort of 'result' object encapsulating the result.
            //       Then, if we have followers, we'll wait for their results to be sent to us, then we'll merge them.
            DistributedBenchmarkResult localResult =
                    Commands.statFilesWeakScaling(numThreads,
                            filesPerThread, inputPath, shuffle, OP_STAT_FILES_WEAK_SCALING);

            if (localResult == null) {
                LOG.warn("Local result is null. Aborting.");
                return;
            }

            localResult.setOperationId(operationId);

            AggregatedResult aggregatedResult = waitForDistributedResult(numDistributedResults, operationId, localResult);

            aggregatedResults[currentTrial] = aggregatedResult;
            results[currentTrial] = aggregatedResult.throughput;
            currentTrial++;

            Pair<Long, DescriptiveStatistics> gcInfo = PRIMARY_HDFS.getRelativeGCInformation();

            aggregatedResult.numGCs = gcInfo.getFirst();
            aggregatedResult.timeSpentGCing = gcInfo.getSecond().getSum();

            LOG.info("Number of GCs: " + gcInfo.getFirst());
            LOG.info("Time spent GC-ing: " + gcInfo.getSecond().getSum() + " ms");

            if (!(currentTrial >= numTrials)) {
                LOG.info("Trial " + currentTrial + "/" + numTrials + " completed. Performing GC and sleeping for " +
                        postTrialSleepInterval + " ms.");
                performClientVMGarbageCollection();
                Thread.sleep(postTrialSleepInterval);
            }
        }

        System.out.println("[THROUGHPUT ONLY]");
        for (double throughputResult : results) {
            System.out.println(throughputResult);
        }

        System.out.println("\nThroughput, Total Duration (sec), Average Latency (ms), Number of GCs, Time Spent GC-ing (ms)");
        for (AggregatedResult result : aggregatedResults)
            System.out.println(result.metricsString + " " + result.numGCs + " " + result.timeSpentGCing);
    }

    private void listDirectoriesFromFile() throws InterruptedException, IOException {
        System.out.print("How many clients (i.e., threads) should be used?\n> ");
        String inputN = scanner.nextLine();
        int n = Integer.parseInt(inputN);

        System.out.print("How many times should each client list their assigned directory?\n> ");
        String inputReadsPerFile = scanner.nextLine();
        int readsPerFile = Integer.parseInt(inputReadsPerFile);

        System.out.print("Please provide a path to a local file containing at least " + inputN + " HopsFS directory " +
                (n == 1 ? "path.\n> " : "paths.\n> "));
        String inputPath = scanner.nextLine();

        boolean shuffle = getBooleanFromUser("Shuffle file paths around?");

        int numTrials = getIntFromUser("How many trials should this benchmark be performed?");

        // Establish baseline for number of GCs.
        PRIMARY_HDFS.getRelativeGCInformation();

        int currentTrial = 0;
        AggregatedResult[] aggregatedResults = new AggregatedResult[numTrials];
        Double[] results = new Double[numTrials];
        while (currentTrial < numTrials) {
            LOG.info("|====| TRIAL #" + currentTrial + " |====|");
            String operationId = UUID.randomUUID().toString();
            int numDistributedResults = followers.size();
            if (followers.size() > 0) {
                JsonObject payload = new JsonObject();
                payload.addProperty(OPERATION, OP_LIST_DIRECTORIES_FROM_FILE);
                payload.addProperty(OPERATION_ID, operationId);
                payload.addProperty("n", n);
                payload.addProperty("listsPerFile", readsPerFile);
                payload.addProperty("inputPath", inputPath);
                payload.addProperty("shuffle", shuffle);

                issueCommandToFollowers("Read n Files with n Threads (Weak Scaling - Read)", operationId, payload);
            }

            // TODO: Make this return some sort of 'result' object encapsulating the result.
            //       Then, if we have followers, we'll wait for their results to be sent to us, then we'll merge them.
            DistributedBenchmarkResult localResult =
                    Commands.listDirectoryWeakScaling(n,
                            readsPerFile, inputPath, shuffle, OP_STAT_FILES_WEAK_SCALING);

            if (localResult == null) {
                LOG.warn("Local result is null. Aborting.");
                return;
            }

            //LOG.info("LOCAL result of weak scaling benchmark: " + localResult);
            localResult.setOperationId(operationId);

            localResult.setOperationId(operationId);
            // Wait for followers' results if we had followers when we first started the operation.
            AggregatedResult aggregatedResult = waitForDistributedResult(numDistributedResults, operationId, localResult);
            double throughput = aggregatedResult.throughput;
            aggregatedResults[currentTrial] = aggregatedResult;

            results[currentTrial] = throughput;
            currentTrial++;

            Pair<Long, DescriptiveStatistics> gcInfo = PRIMARY_HDFS.getRelativeGCInformation();

            aggregatedResult.numGCs = gcInfo.getFirst();
            aggregatedResult.timeSpentGCing = gcInfo.getSecond().getSum();

            LOG.info("Number of GCs: " + gcInfo.getFirst());
            LOG.info("Time spent GC-ing: " + gcInfo.getSecond().getSum() + " ms");

            if (!(currentTrial >= numTrials)) {
                LOG.info("Trial " + currentTrial + "/" + numTrials + " completed. Performing GC and sleeping for " +
                        postTrialSleepInterval + " ms.");
                performClientVMGarbageCollection();
                Thread.sleep(postTrialSleepInterval);
            }
        }

        System.out.println("[THROUGHPUT ONLY]");
        for (double throughputResult : results) {
            System.out.println(throughputResult);
        }

        System.out.println("\nThroughput, Total Duration (sec), Average Latency (ms), Number of GCs, Time Spent GC-ing (ms)");
        for (AggregatedResult result : aggregatedResults)
            System.out.println(result.metricsString + " " + result.numGCs + " " + result.timeSpentGCing);
    }

    /**
     * Update the running totals for number of GCs performed and time spent GC-ing.
     */
    private void updateGCMetrics() {
        List<GarbageCollectorMXBean> mxBeans = ManagementFactory.getGarbageCollectorMXBeans();
        this.numGarbageCollections = 0;
        this.garbageCollectionTime = 0;
        for (GarbageCollectorMXBean mxBean : mxBeans) {
            long count = mxBean.getCollectionCount();
            long time  = mxBean.getCollectionTime();

            if (count > 0)
                this.numGarbageCollections += count;

            if (time > 0)
                this.garbageCollectionTime += time;
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
     * Issue a command to all our followers.
     * @param opName The name of the command.
     * @param operationId Unique ID of this operation.
     * @param payload Contains the command and necessary arguments.
     */
    private void issueCommandToFollowers(String opName, String operationId, JsonObject payload) {
        issueCommandToFollowers(opName, operationId, payload, true);
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

    private void changeSleepInterval() {
        System.out.println("Current sleep interval: " + postTrialSleepInterval + " ms");

        System.out.println("Please enter a new value for the sleep interval (in ms). Enter nothing to keep it the same.");

        String input = scanner.nextLine();

        if (input.equals("")) {
            System.out.println("Keeping the sleep interval the same (" + postTrialSleepInterval + " ms).");
            return;
        }

         try {
             postTrialSleepInterval = Integer.parseInt(input);

             System.out.println("Successfully updated post-trial sleep interval. New value: " + postTrialSleepInterval + " ms");
         } catch (NumberFormatException ex) {
             System.out.println("[ERROR] Specified value is not a valid integer: \"" + input + "\"");
             System.out.println("Keeping the sleep interval the same (" + postTrialSleepInterval + " ms).");
         }
    }

    /**
     * Perform GCs on this Client VM as well as any other client VMs if we're the Commander for a distributed setup.
     */
    private void performClientVMGarbageCollection() {
        if (!nondistributed) {
            JsonObject payload = new JsonObject();
            String operationId = UUID.randomUUID().toString();
            payload.addProperty(OPERATION, OP_TRIGGER_CLIENT_GC);
            payload.addProperty(OPERATION_ID, operationId);

            issueCommandToFollowers("Client VM Garbage Collection", operationId, payload);
        }
        System.gc();
    }

    public void strongScalingWriteOperation()
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

        String operationId = UUID.randomUUID().toString();
        int numDistributedResults = followers.size();
        if (followers.size() > 0) {
            JsonObject payload = new JsonObject();
            payload.addProperty(OPERATION, OP_STRONG_SCALING_WRITES);
            payload.addProperty(OPERATION_ID, operationId);
            payload.addProperty("n", writesPerThread);
            payload.addProperty("numberOfThreads", numberOfThreads);

            JsonArray directoriesJson = new JsonArray();
            for (String dir : directories)
                directoriesJson.add(dir);

            payload.add("directories", directoriesJson);

            issueCommandToFollowers("Write n Files with n Threads (Weak Scaling - Write)", operationId, payload);
        }

        LOG.info("Each thread should be writing " + writesPerThread + " files...");

        DistributedBenchmarkResult localResult =
                Commands.writeFilesInternal(writesPerThread, numberOfThreads, directories,
                        OP_STRONG_SCALING_WRITES, false,
                        OPERATION_ID, false);
        localResult.setOperationId(operationId);
        localResult.setOperation(OP_STRONG_SCALING_WRITES);

        //LOG.info("LOCAL result of weak scaling benchmark: " + localResult);
        localResult.setOperationId(operationId);

        // Wait for followers' results if we had followers when we first started the operation.
        if (numDistributedResults > 0)
            waitForDistributedResult(numDistributedResults, operationId, localResult);
    }

    //    public void strongScalingReadOperationNew(final Configuration configuration,
    //                                           final String nameNodeEndpoint)
    //            throws InterruptedException, FileNotFoundException {
    //
    //        // User provides file containing HopsFS file paths.
    //        // Specifies how many files each thread should read.
    //        // Specifies number of threads.
    //        // Specifies how many times each file should be read.
    //        int totalNumberOfReads = getIntFromUser("What is the total number of reads to be performed?");
    //
    //        int numClientVMs = getIntFromUser("How many client VMs are involved in the operation?");
    //
    //        int clientsPerVM = getIntFromUser("How many clients should run on each VM?");
    //
    //        System.out.print("Please provide a path to a local file containing HopsFS file paths.\n> ");
    //        String inputPath = scanner.nextLine();
    //
    //        String operationId = UUID.randomUUID().toString();
    //        int numDistributedResults = followers.size();
    //        if (followers.size() > 0) {
    //            JsonObject payload = new JsonObject();
    //            payload.addProperty(OPERATION, OP_STRONG_SCALING_READS);
    //            payload.addProperty(OPERATION_ID, operationId);
    //            payload.addProperty("totalNumberOfReads", totalNumberOfReads);
    //            payload.addProperty("numClientVMs", numClientVMs);
    //            payload.addProperty("numClientVMs", numClientVMs);
    //            payload.addProperty("inputPath", inputPath);
    //
    //            issueCommandToFollowers("New Strong Scaling Reads", operationId, payload);
    //        }
    //
    //        DistributedBenchmarkResult localResult =
    //                Commands.strongScalingBenchmarkOld(configuration, nameNodeEndpoint, filesPerThread, readsPerFile,
    //                        numThreads, inputPath);
    //
    //        if (localResult == null) {
    //            LOG.warn("Local result is null. Aborting.");
    //            return;
    //        }
    //
    //        LOG.info("LOCAL result of strong scaling benchmark: " + localResult);
    //        localResult.setOperationId(operationId);
    //
    //        // Wait for followers' results if we had followers when we first started the operation.
    //        if (numDistributedResults > 0)
    //            waitForDistributedResult(numDistributedResults, operationId, localResult);
    //    }

    public void strongScalingReadOperation()
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

            issueCommandToFollowers("Read n Files y Times with z Threads (Strong Scaling)", operationId, payload);
        }

        DistributedBenchmarkResult localResult =
                Commands.strongScalingBenchmarkOld(filesPerThread, readsPerFile,
                        numThreads, inputPath);

        if (localResult == null) {
            LOG.warn("Local result is null. Aborting.");
            return;
        }

        LOG.info("LOCAL result of strong scaling benchmark: " + localResult);
        localResult.setOperationId(operationId);
        localResult.setOperation(OP_STRONG_SCALING_READS);

        // Wait for followers' results if we had followers when we first started the operation.
        if (numDistributedResults > 0)
            waitForDistributedResult(numDistributedResults, operationId, localResult);
    }

    /**
     * Weak scaling, writes.
     */
    public void weakScalingWriteOperation()
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
        } // If neither of the above are true, then we use the entire directories list.
        //   We'll generate a bunch of random writes using the full list.

        System.out.print("Number of writes per thread? \n> ");
        int writesPerThread = Integer.parseInt(scanner.nextLine());

        int numTrials = getIntFromUser("How many trials would you like to perform?");

        if (numTrials <= 0)
            throw new IllegalArgumentException("The number of trials should be at least 1.");

        boolean writePathsToFile = getBooleanFromUser("Write HopsFS file paths to file?");

        // Establish baseline for number of GCs.
        PRIMARY_HDFS.getRelativeGCInformation();

        int currentTrial = 0;
        AggregatedResult[] aggregatedResults = new AggregatedResult[numTrials];
        double[] results = new double[numTrials];

        while (currentTrial < numTrials) {
            LOG.info("|====| TRIAL #" + currentTrial + " |====|");

            String operationId = UUID.randomUUID().toString();
            int numDistributedResults = followers.size();
            if (followers.size() > 0) {
                JsonObject payload = new JsonObject();
                payload.addProperty(OPERATION, OP_WEAK_SCALING_WRITES);
                payload.addProperty(OPERATION_ID, operationId);
                payload.addProperty("n", writesPerThread);
                payload.addProperty("numberOfThreads", numberOfThreads);
                payload.addProperty("randomWrites", directoryChoice == 3);
                payload.addProperty("writePathsToFile", writePathsToFile);

                JsonArray directoriesJson = new JsonArray();
                for (String dir : directories)
                    directoriesJson.add(dir);

                payload.add("directories", directoriesJson);

                issueCommandToFollowers("Write n Files with n Threads (Weak Scaling - Write)", operationId, payload);
            }

            LOG.info("Each thread should be writing " + writesPerThread + " files...");
            DistributedBenchmarkResult localResult =
                    Commands.writeFilesInternal(writesPerThread, numberOfThreads, directories,
                            OP_WEAK_SCALING_WRITES, (directoryChoice == 3),
                            operationId, writePathsToFile);
            LOG.info("Received local result...");
            localResult.setOperationId(operationId);
            localResult.setOperation(OP_WEAK_SCALING_WRITES);

            //LOG.info("LOCAL result of weak scaling benchmark: " + localResult);
            localResult.setOperationId(operationId);

            // Wait for followers' results if we had followers when we first started the operation.
            AggregatedResult aggregatedResult = waitForDistributedResult(numDistributedResults, operationId, localResult);

            aggregatedResults[currentTrial] = aggregatedResult;
            results[currentTrial] = aggregatedResult.throughput;
            currentTrial++;

            Pair<Long, DescriptiveStatistics> gcInfo = PRIMARY_HDFS.getRelativeGCInformation();

            aggregatedResult.numGCs = gcInfo.getFirst();
            aggregatedResult.timeSpentGCing = gcInfo.getSecond().getSum();

            LOG.info("Number of GCs: " + gcInfo.getFirst());
            LOG.info("Time spent GC-ing: " + gcInfo.getSecond().getSum() + " ms");

            if (!(currentTrial >= numTrials)) {
                LOG.info("Trial " + currentTrial + "/" + numTrials + " completed. Performing GC and sleeping for " +
                        postTrialSleepInterval + " ms.");
                performClientVMGarbageCollection();
                Thread.sleep(postTrialSleepInterval);
            }
        }

        System.out.println("[THROUGHPUT ONLY]");
        for (double throughputResult : results) {
            System.out.println(throughputResult);
        }

        System.out.println("\nThroughput, Total Duration (sec), Average Latency (ms), Number of GCs, Time Spent GC-ing (ms)");
        for (AggregatedResult result : aggregatedResults)
            System.out.println(result.metricsString + " " + result.numGCs + " " + result.timeSpentGCing);
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
        if (numDistributedResults < 1) {
            // LOG.warn("The number of distributed results is 1. We have nothing to wait for.");
            String metricsString = "";

            DescriptiveStatistics statistics = new DescriptiveStatistics(localResult.latencyStatistics);
            try {
                DecimalFormat df = new DecimalFormat("#.####");
                double avgTcpLatency = statistics.getMean();
                // throughput (ops/sec), cache hits, cache misses, cache hit rate, avg tcp latency, avg http latency, avg combined latency
                metricsString = String.format("%s %s %s", df.format(localResult.getOpsPerSecond()), df.format(avgTcpLatency),
                        localResult.durationSeconds);
            } catch (NullPointerException ex) {
                LOG.warn("Could not generate metrics string due to NPE.");
            }

            return new AggregatedResult(localResult.getOpsPerSecond(), statistics.getMean(),
                    metricsString, localResult.opsStats, localResult.durationSeconds);
        }

        LOG.debug("Waiting for " + numDistributedResults + " distributed result(s).");
        BlockingQueue<DistributedBenchmarkResult> resultQueue = resultQueues.get(operationId);
        assert(resultQueue != null);

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

                if (decision)
                    break;

                counter = 0;
            }
        }

        return extractDistributedResultFromQueue(resultQueue, localResult, numDistributedResults);
    }

    /**
     * Weak scaling, reads.
     *
     * Query the user for:
     *  - An integer `n`, the number of threads.
     *  - The path to a local file containing HopsFS file paths.
     *  - The number of files each thread should read.
     */
    private void weakScalingReadOperationV2()
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

        // Establish baseline for number of GCs.
        PRIMARY_HDFS.getRelativeGCInformation();

        int currentTrial = 0;
        AggregatedResult[] aggregatedResults = new AggregatedResult[numTrials];
        Double[] results = new Double[numTrials];
        while (currentTrial < numTrials) {
            LOG.info("|====| TRIAL #" + currentTrial + " |====|");
            String operationId = UUID.randomUUID().toString();
            int numDistributedResults = followers.size();
            if (followers.size() > 0) {
                JsonObject payload = new JsonObject();
                payload.addProperty(OPERATION, OP_WEAK_SCALING_READS_V2);
                payload.addProperty(OPERATION_ID, operationId);
                payload.addProperty("numThreads", numThreads);
                payload.addProperty("filesPerThread", filesPerThread);
                payload.addProperty("inputPath", inputPath);
                payload.addProperty("shuffle", shuffle);

                issueCommandToFollowers("Read n Files with n Threads (Weak Scaling - Read)", operationId, payload);
            }

            // TODO: Make this return some sort of 'result' object encapsulating the result.
            //       Then, if we have followers, we'll wait for their results to be sent to us, then we'll merge them.
            DistributedBenchmarkResult localResult =
                    Commands.weakScalingBenchmarkV2(numThreads,
                            filesPerThread, inputPath, shuffle, OP_WEAK_SCALING_READS_V2);

            if (localResult == null) {
                LOG.warn("Local result is null. Aborting.");
                return;
            }

            //LOG.info("LOCAL result of weak scaling benchmark: " + localResult);
            localResult.setOperationId(operationId);

            // If we have no followers, this will just use the local result.
            AggregatedResult aggregatedResult = waitForDistributedResult(numDistributedResults, operationId, localResult);

            aggregatedResults[currentTrial] = aggregatedResult;
            results[currentTrial] = aggregatedResult.throughput;
            currentTrial++;

            Pair<Long, DescriptiveStatistics> gcInfo = PRIMARY_HDFS.getRelativeGCInformation();

            aggregatedResult.numGCs = gcInfo.getFirst();
            aggregatedResult.timeSpentGCing = gcInfo.getSecond().getSum();

            LOG.info("Number of GCs: " + gcInfo.getFirst());
            LOG.info("Time spent GC-ing: " + gcInfo.getSecond().getSum() + " ms");

            if (!(currentTrial >= numTrials)) {
                LOG.info("Trial " + currentTrial + "/" + numTrials + " completed. Performing GC and sleeping for " +
                        postTrialSleepInterval + " ms.");
                performClientVMGarbageCollection();
                Thread.sleep(postTrialSleepInterval);
            }
        }

        System.out.println("[THROUGHPUT ONLY]");
        for (double throughputResult : results) {
            System.out.println(throughputResult);
        }

        System.out.println("\nThroughput, Total Duration (sec), Average Latency (ms), Number of GCs, Time Spent GC-ing (ms)");
        for (AggregatedResult result : aggregatedResults)
            System.out.println(result.metricsString + " " + result.numGCs + " " + result.timeSpentGCing);
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
    private void weakScalingReadOperation()
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

        // Establish baseline for number of GCs.
        PRIMARY_HDFS.getRelativeGCInformation();

        int currentTrial = 0;
        AggregatedResult[] aggregatedResults = new AggregatedResult[numTrials];
        Double[] results = new Double[numTrials];
        while (currentTrial < numTrials) {
            LOG.info("|====| TRIAL #" + currentTrial + " |====|");
            String operationId = UUID.randomUUID().toString();
            int numDistributedResults = followers.size();
            if (followers.size() > 0) {
                JsonObject payload = new JsonObject();
                payload.addProperty(OPERATION, OP_WEAK_SCALING_READS);
                payload.addProperty(OPERATION_ID, operationId);
                payload.addProperty("n", n);
                payload.addProperty("readsPerFile", readsPerFile);
                payload.addProperty("inputPath", inputPath);
                payload.addProperty("shuffle", shuffle);

                issueCommandToFollowers("Read n Files with n Threads (Weak Scaling - Read)", operationId, payload);
            }

            // TODO: Make this return some sort of 'result' object encapsulating the result.
            //       Then, if we have followers, we'll wait for their results to be sent to us, then we'll merge them.
            DistributedBenchmarkResult localResult =
                    Commands.weakScalingReadsV1(n,
                            readsPerFile, inputPath, shuffle, OP_WEAK_SCALING_READS);

            if (localResult == null) {
                LOG.warn("Local result is null. Aborting.");
                return;
            }

            //LOG.info("LOCAL result of weak scaling benchmark: " + localResult);
            localResult.setOperationId(operationId);

            localResult.setOperationId(operationId);
            // Wait for followers' results if we had followers when we first started the operation.
            AggregatedResult aggregatedResult = waitForDistributedResult(numDistributedResults, operationId, localResult);
            double throughput = aggregatedResult.throughput;
            aggregatedResults[currentTrial] = aggregatedResult;

            results[currentTrial] = throughput;
            currentTrial++;

            Pair<Long, DescriptiveStatistics> gcInfo = PRIMARY_HDFS.getRelativeGCInformation();

            aggregatedResult.numGCs = gcInfo.getFirst();
            aggregatedResult.timeSpentGCing = gcInfo.getSecond().getSum();

            LOG.info("Number of GCs: " + gcInfo.getFirst());
            LOG.info("Time spent GC-ing: " + gcInfo.getSecond().getSum() + " ms");

            if (!(currentTrial >= numTrials)) {
                LOG.info("Trial " + currentTrial + "/" + numTrials + " completed. Performing GC and sleeping for " +
                        postTrialSleepInterval + " ms.");
                performClientVMGarbageCollection();
                Thread.sleep(postTrialSleepInterval);
            }
        }

        System.out.println("[THROUGHPUT ONLY]");
        for (double throughputResult : results) {
            System.out.println(throughputResult);
        }

        System.out.println("\nThroughput, Total Duration (sec), Average Latency (ms), Number of GCs, Time Spent GC-ing (ms)");
        for (AggregatedResult result : aggregatedResults)
            System.out.println(result.metricsString + " " + result.numGCs + " " + result.timeSpentGCing);
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
     * @param nameNodeEndpoint Where HTTP requests are directed.
     */
    public static DistributedFileSystem initDfsClient(String nameNodeEndpoint) {
        LOG.debug("Creating HDFS client now...");
        Configuration hdfsConfiguration = Utils.getConfiguration(hdfsSiteConfigFilePath);
        try {
            hdfsConfiguration.addResource(new File(hdfsSiteConfigFilePath).toURI().toURL());
        } catch (MalformedURLException ex) {
            ex.printStackTrace();
        }
        LOG.info("Created configuration.");
        DistributedFileSystem hdfs = new DistributedFileSystem();
        LOG.info("Created DistributedFileSystem object.");

        try {
            hdfs.initialize(new URI(nameNodeEndpoint), hdfsConfiguration);
            LOG.info("Called initialize() successfully.");
        } catch (URISyntaxException | IOException ex) {
            LOG.error("");
            LOG.error("");
            LOG.error("ERROR: Encountered exception while initializing DistributedFileSystem object.");
            ex.printStackTrace();
            System.exit(1);
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
            registrationPayload.addProperty(HDFS_CONFIG_PATH, hdfsSiteConfigFilePath);

            LOG.debug("Sending '" + NAMENODE_ENDPOINT + "' as '" + NAMENODE_ENDPOINT + "'.");
            LOG.debug("Sending '" + HDFS_CONFIG_PATH + "' as '" + hdfsSiteConfigFilePath + "'.");

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
                LOG.info("Trying to re-launch Follower " + connection.name + " now...");
                launchFollower("ben", connection.name, String.format(LAUNCH_FOLLOWER_CMD, ip, port));
            } else {
                LOG.warn("Lost connection to follower.");
                LOG.debug("We now have " + followers.size() + " followers registered.");
                LOG.error("Follower connection did not have a name. Cannot attempt to re-launch follower.");
            }
        }
    }

    private static void printMenu() {
        System.out.println("\n====== MENU ======");
        System.out.println("(-9) Garbage Collection\n(-7) Get absolute GC information\n(-6) Get relative GC information" +
                "\n(-5) Change inter-trial sleep interval\n(-4) Clear metric data");
        System.out.println("(0) Exit\n(1) Create file\n(2) Create directory\n(3) Read contents of file.\n(4) Rename" +
                "\n(5) Delete\n(6) List directory\n(7) Append\n(8) Create Subtree.\n(9) Ping\n(10) Prewarm" +
                "\n(11) Write Files to Directory\n(12) Read files\n(13) Delete files\n(14) Write Files to Directories" +
                "\n(15) Read n Files with n Threads (Weak Scaling - Read)\n(16) Read n Files y Times with z Threads (Strong Scaling - Read)" +
                "\n(17) Write n Files with n Threads (Weak Scaling - Write)\n(18) Write n Files y Times with z Threads (Strong Scaling - Write)" +
                "\n(19) Create directories\n(20) Weak Scaling Reads v2\n(21) File Stat Benchmark" +
                "\n(22) Unavailable.\n(23) List Dir Weak Scaling\n(24) Stat File Weak Scaling." +
                "\n(25) MKDIR Weak Scaling\n(26) Randomly-generated workload.\n(30) Create from file." +
                "\n(31) Reader-Writer Test #1.\n");
        System.out.println("==================\n");
        System.out.println("What would you like to do?");
        System.out.print("> ");
    }

    public static class AggregatedResult implements Serializable {
        public double throughput;
        public double averageLatency;
        public double durationSeconds;
        public String metricsString; // All the metrics I'd want formatted so that I can copy & paste into Excel.
        public Map<String, List<BMOpStats>> opsStats;

        public long numGCs;
        public double timeSpentGCing; // Milliseconds

        public AggregatedResult(double throughput, double averageLatency, String metricsString,
                                Map<String, List<BMOpStats>> opsStats, double durationSeconds) {
            this.throughput = throughput;
            this.averageLatency = averageLatency;
            this.metricsString = metricsString;
            this.opsStats = opsStats;
            this.durationSeconds = durationSeconds;
        }

        @Override
        public String toString() {
            return "Throughput (ops/sec): " + throughput + ", Average Latency: " + averageLatency +
                    " ms, Duration (seconds): " + durationSeconds + ", NumGCs: " + numGCs +
                    ", Time Spent GCing: " + timeSpentGCing + " ms";

        }
    }
}
