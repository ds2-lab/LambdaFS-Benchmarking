package com.gmail.benrcarver.distributed;

import com.esotericsoftware.kryonet.Connection;
import com.esotericsoftware.kryonet.FrameworkMessage;
import com.esotericsoftware.kryonet.Listener;
import com.esotericsoftware.kryonet.Server;
import com.gmail.benrcarver.distributed.util.Utils;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.jcraft.jsch.*;
import io.hops.metrics.TransactionEvent;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.yaml.snakeyaml.Yaml;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.io.*;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static com.gmail.benrcarver.distributed.Commands.hdfsClients;
import static com.gmail.benrcarver.distributed.Constants.*;

/**
 * Controls a fleet of distributed machines. Executes HopsFS benchmarks based on user input/commands.
 */
public class Commander {
    public static final Log LOG = LogFactory.getLog(Commander.class);
    private static final Console con = System.console();

    private final List<Connection> followers;

    private static final int COMMANDER_TCP_BUFFER_SIZES = Follower.FOLLOWER_TCP_BUFFER_SIZES * 4;

    private static final String LEADER_PREFIX = "[LEADER TCP SERVER]";

    /**
     * Use with String.format(LAUNCH_FOLLOWER_CMD, leader_ip, leader_port)
     */
    private static final String LAUNCH_FOLLOWER_CMD = "source ~/.bashrc & cd /home/ubuntu/repos/HopsFS-Benchmarking-Utility & java -Dlog4j.configuration=file:/home/ubuntu/repos/HopsFS-Benchmarking-Utility/src/main/resources/log4j.properties -Dsun.io.serialization.extendedDebugInfo=true -Xmx58g -Xms58g -XX:+UseConcMarkSweepGC -XX:+UnlockDiagnosticVMOptions -XX:ParGCCardsPerStrideChunk=32768 -XX:+CMSScavengeBeforeRemark -XX:MaxGCPauseMillis=350 -XX:MaxTenuringThreshold=2 -XX:MaxNewSize=32000m -XX:+CMSClassUnloadingEnabled -XX:+UseCMSInitiatingOccupancyOnly -XX:CMSInitiatingOccupancyFraction=75 -XX:+ScavengeBeforeFullGC -verbose:gc -XX:+PrintGCTimeStamps -XX:+PrintGCDetails -cp \".:target/HopsFSBenchmark-1.0-jar-with-dependencies.jar:/home/ben/repos/hops/hadoop-dist/target/hadoop-3.2.0.3-SNAPSHOT/share/hadoop/hdfs/lib/*:/home/ben/repos/hops/hadoop-dist/target/hadoop-3.2.0-SNAPSHOT/share/hadoop/common/lib/*:/home/ben/repos/hops/hadoop-hdfs-project/hadoop-hdfs-client/target/hadoop-hdfs-client-3.2.0.3-SNAPSHOT.jar:/home/ben/repos/hops/hops-leader-election/target/hops-leader-election-3.2.0.3-SNAPSHOT.jar:/home/ben/openwhisk-runtime-java/core/java8/libs/*:/home/ben/repos/hops/hadoop-hdfs-project/hadoop-hdfs/target/hadoop-hdfs-3.2.0.3-SNAPSHOT.jar:/home/ben/repos/hops/hadoop-common-project/hadoop-common/target/hadoop-common-3.2.0.3-SNAPSHOT.jar\" com.gmail.benrcarver.distributed.InteractiveTest --leader_ip %s --leader_port %d --yaml_path /home/ubuntu/repos/HopsFS-Benchmarking-Utility/config.yaml --worker";

    /**
     * Has a default value.
     */
    private String nameNodeEndpoint = "hdfs://10.150.0.17:9000/";

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
     * The hdfs-site.xml configuration file.
     */
    private Configuration hdfsConfiguration;

    /**
     * Map from follower IP to the associated SSH client.
     */
    private HashMap<String, Session> sshClients;

    /**
     * Map from operation ID to the queue in which distributed results should be placed by the TCP server.
     */
    private ConcurrentHashMap<String, BlockingQueue<DistributedBenchmarkResult>> resultQueues;

    private final boolean nondistributed;

    private static String serverlessLogLevel = null;
    public static boolean consistencyEnabled = true;

    private static Commander instanace;

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
     * Start the first 'numFollowersFromConfigToStart' followers listed in the config.
     */
    private int numFollowersFromConfigToStart;

    /**
     * If true, then we SCP the JAR files to each follower before starting them.
     */
    private boolean scpJars;

    /**
     * If true, then we SCP the config file to each follower before starting them.
     */
    private boolean scpConfig;

    public static Commander getOrCreateCommander(String ip, int port, String yamlPath, boolean nondistributed,
                                                 boolean disableConsistency, int numFollowers,
                                                 boolean scpJars, boolean scpConfig) throws IOException {
        if (instanace == null) {
            // serverlessLogLevel = logLevel;
            consistencyEnabled = !disableConsistency;
            instanace = new Commander(ip, port, yamlPath, nondistributed, numFollowers, scpJars, scpConfig);
        }

        return instanace;
    }

    private Commander(String ip, int port, String yamlPath, boolean nondistributed, int numFollowersFromConfigToStart,
                      boolean scpJars, boolean scpConfig)
            throws IOException {
        this.ip = ip;
        this.port = port;
        this.nondistributed = nondistributed;
        // TODO: Maybe do book-keeping or fault-tolerance here.
        this.followers = new ArrayList<>();
        this.resultQueues = new ConcurrentHashMap<>();
        this.numFollowersFromConfigToStart = numFollowersFromConfigToStart;
        this.scpJars = scpJars;
        this.scpConfig = scpConfig;

        tcpServer = new Server(COMMANDER_TCP_BUFFER_SIZES, COMMANDER_TCP_BUFFER_SIZES) {
            @Override
            protected Connection newConnection() {
                LOG.debug(LEADER_PREFIX + " Creating new FollowerConnection.");
                return new FollowerConnection();
            }
        };

        tcpServer.addListener(new Listener.ThreadedListener(new ServerListener()));

        processConfiguration(yamlPath);
    }

    /**
     * Process the configuration file for the benchmarking utility.
     */
    private void processConfiguration(String yamlPath) throws IOException {
        Yaml yaml = new Yaml();
        try (InputStream in = Files.newInputStream(Paths.get(yamlPath))) {
            LocalConfiguration config = yaml.loadAs(in, LocalConfiguration.class);

            LOG.info("Loaded configuration: " + config.toString());

            nameNodeEndpoint = config.getNamenodeEndpoint();
            followerConfigs = config.getFollowers();
            hdfsConfigFilePath = config.getHdfsConfigFile();
            isServerless = config.getIsServerless();

            Commands.IS_SERVERLESS = isServerless;

            LOG.info("Loaded configuration!");
            LOG.info(config);
        }
    }

    public void start() throws IOException, InterruptedException {
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
        }
        Commands.TRACK_OP_PERFORMED = true;
        LOG.info("Commander is operating in NON-DISTRIBUTED mode.");
        interactiveLoop();
    }

    /**
     * Using SSH, launch the follower processes.
     */
    private void launchFollowers() throws IOException, JSchException {
        final String launchCommand = String.format(LAUNCH_FOLLOWER_CMD, ip, port);
        final String scpHadoopHdfsJarCommand = "scp /home/ben/repos/hops/hadoop-hdfs-project/hadoop-hdfs/target/hadoop-hdfs-3.2.0.3-SNAPSHOT.jar %s@%s:/home/ben/repos/hops/hadoop-hdfs-project/hadoop-hdfs/target/hadoop-hdfs-3.2.0.3-SNAPSHOT.jar";
        final String scpBenchmarkJarCommand = "scp /home/ubuntu/repos/HopsFS-Benchmarking-Utility/target/HopsFSBenchmark-1.0-jar-with-dependencies.jar %s@%s:HopsFSBenchmark-1.0-jar-with-dependencies.jar";
        final String scpConfigCommand = "scp /home/ben/repos/hops/hadoop-dist/target/hadoop-3.2.0.3-SNAPSHOT/etc/hadoop/hdfs-site.xml %s@%s:/home/ben/repos/hops/hadoop-dist/target/hadoop-3.2.0.3-SNAPSHOT/etc/hadoop/hdfs-site.xml";

        // If 'numFollowersFromConfigToStart' is negative, then use all followers.
        if (numFollowersFromConfigToStart < 0)
            numFollowersFromConfigToStart = followerConfigs.size();

        LOG.info("Starting " + numFollowersFromConfigToStart + " follower(s) now...");

        JSch jsch = new JSch();
        jsch.addIdentity("/home/ubuntu/.ssh/id_rsa");

        for (int i = 0; i < numFollowersFromConfigToStart; i++) {
            FollowerConfig config = followerConfigs.get(i);
            LOG.info("Starting follower at " + config.getUser() + "@" + config.getIp() + " now.");

            java.util.Properties sshConfig = new java.util.Properties();
            sshConfig.put("StrictHostKeyChecking", "no");

            Session session;
            try {
                session = jsch.getSession(config.getUser(), config.getIp(), 22);
                session.setConfig(sshConfig);
                session.connect();

                if (scpJars) {
                    LOG.debug("SCP-ing hadoop-hdfs-3.2.0.3-SNAPSHOT.jar to Follower " + config.getIp() + ".");
                    Channel channel = session.openChannel("exec");
                    String command = String.format(scpHadoopHdfsJarCommand, config.getUser(), config.getIp());
                    ((ChannelExec)channel).setCommand(command);
                    channel.connect();

                    LOG.debug("SCP-ing HopsFSBenchmark-1.0-jar-with-dependencies.jar to Follower " + config.getIp() + ".");
                    channel = session.openChannel("exec");
                    command = String.format(scpBenchmarkJarCommand, config.getUser(), config.getIp());
                    ((ChannelExec)channel).setCommand(command);
                    channel.connect();
                }

                if (scpConfig) {
                    LOG.debug("SCP-ing hdfs-site.xml to Follower " + config.getIp() + ".");
                    Channel channel = session.openChannel("exec");
                    String command = String.format(scpConfigCommand, config.getUser(), config.getIp());
                    ((ChannelExec)channel).setCommand(command);
                    channel.connect();
                }

                Channel channel = session.openChannel("exec");
                ((ChannelExec)channel).setCommand(launchCommand);
                channel.setInputStream(null);
                ((ChannelExec)channel).setErrStream(System.err);

                InputStream in=channel.getInputStream();
                channel.connect();
                byte[] tmp=new byte[1024];
                while(true){
                    while(in.available()>0){
                        int j=in.read(tmp, 0, 1024);
                        if(j<0)break;
                        System.out.print(new String(tmp, 0, j));
                    }
                    if(channel.isClosed()){
                        System.out.println("exit-status: "+channel.getExitStatus());
                        break;
                    }
                    try{Thread.sleep(1000);}catch(Exception ee){}
                }
                channel.disconnect();
                session.disconnect();
                System.out.println("DONE");

            } catch (JSchException e) {
                e.printStackTrace();
            }

//            SSHClient ssh = new SSHClient();
//            ssh.loadKnownHosts();
//            ssh.connect(config.getIp());

            LOG.debug("Connected to follower at " + config.getUser() + "@" + config.getIp() + " now.");

//            Session session = null;
//
//            try {
//                ssh.authPublickey(config.getUser());
//
//                LOG.debug("Authenticated with follower at " + config.getUser() + "@" + config.getIp() + " now.");
//
//                session = ssh.startSession();
//
//                LOG.debug("Started session with follower at " + config.getUser() + "@" + config.getIp() + " now.");
//
//                Session.Command cmd = session.exec(fullCommand);
//
//                LOG.debug("Executed command: " + fullCommand);
//
//                ByteArrayOutputStream inputStream = IOUtils.readFully(cmd.getInputStream());
//                LOG.debug("Output: " + inputStream);
//
//                con.writer().print(inputStream);
//                cmd.join(5, TimeUnit.SECONDS);
//                con.writer().print("\n** exit status: " + cmd.getExitStatus());
//                LOG.debug("Exit status: " + cmd.getExitStatus());
//            } finally {
//                if (session != null)
//                    session.close();
//
//                ssh.disconnect();
//            }
        }
    }

    private void startServer() throws IOException {
        //com.esotericsoftware.minlog.Log.set(com.esotericsoftware.minlog.Log.LEVEL_DEBUG);
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

        primaryHdfs = initDfsClient(null, nameNodeEndpoint, true);

        while (true) {
            updateGCMetrics();
            long startingGCs = numGarbageCollections;
            long startingGCTime = garbageCollectionTime;

            try {
                Thread.sleep(50);
                printMenu();
                int op = getNextOperation();

                switch (op) {
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
                        primaryHdfs.printDebugInformation();
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
                        Commands.createFileOperation(primaryHdfs, nameNodeEndpoint);
                        break;
                    case OP_MKDIR:
                        LOG.info("MAKE DIRECTORY selected!");
                        Commands.mkdirOperation(primaryHdfs, nameNodeEndpoint);
                        ;
                        break;
                    case OP_READ_FILE:
                        LOG.info("READ FILE selected!");
                        Commands.readOperation(primaryHdfs, nameNodeEndpoint);
                        break;
                    case OP_RENAME:
                        LOG.info("RENAME selected!");
                        Commands.renameOperation(primaryHdfs, nameNodeEndpoint);
                        break;
                    case OP_DELETE:
                        LOG.info("DELETE selected!");
                        Commands.deleteOperation(primaryHdfs, nameNodeEndpoint);
                        break;
                    case OP_LIST:
                        LOG.info("LIST selected!");
                        Commands.listOperation(primaryHdfs, nameNodeEndpoint);
                        break;
                    case OP_APPEND:
                        LOG.info("APPEND selected!");
                        Commands.appendOperation(primaryHdfs, nameNodeEndpoint);
                        break;
                    case OP_CREATE_SUBTREE:
                        LOG.info("CREATE SUBTREE selected!");
                        Commands.createSubtree(primaryHdfs, nameNodeEndpoint);
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
                        Commands.writeFilesToDirectory(primaryHdfs, hdfsConfiguration, nameNodeEndpoint);
                        break;
                    case OP_READ_FILES:
                        LOG.info("READ FILES selected!");
                        Commands.readFilesOperation(hdfsConfiguration, primaryHdfs, nameNodeEndpoint, OP_READ_FILES);
                        break;
                    case OP_DELETE_FILES:
                        LOG.info("DELETE FILES selected!");
                        Commands.deleteFilesOperation(primaryHdfs, nameNodeEndpoint);
                        break;
                    case OP_WRITE_FILES_TO_DIRS:
                        LOG.info("WRITE FILES TO DIRECTORIES selected!");
                        Commands.writeFilesToDirectories(primaryHdfs, hdfsConfiguration, nameNodeEndpoint);
                        break;
                    case OP_WEAK_SCALING_READS:
                        LOG.info("'Read n Files with n Threads (Weak Scaling - Read)' selected!");
                        weakScalingReadOperation(hdfsConfiguration, primaryHdfs, nameNodeEndpoint);
                        break;
                    case OP_STRONG_SCALING_READS:
                        LOG.info("'Read n Files y Times with z Threads (Strong Scaling - Read)' selected!");
                        strongScalingReadOperation(hdfsConfiguration, primaryHdfs, nameNodeEndpoint);
                        break;
                    case OP_WEAK_SCALING_WRITES:
                        LOG.info("'Write n Files with n Threads (Weak Scaling - Write)' selected!");
                        weakScalingWriteOperation(hdfsConfiguration, primaryHdfs, nameNodeEndpoint);
                        break;
                    case OP_STRONG_SCALING_WRITES:
                        LOG.info("'Write n Files y Times with z Threads (Strong Scaling - Write)' selected!");
                        throw new NotImplementedException("Not yet implemented.");
                    case OP_CREATE_DIRECTORIES:
                        LOG.info("CREATE DIRECTORIES selected!");
                        Commands.createDirectories(primaryHdfs, nameNodeEndpoint);
                        break;
                    case OP_WEAK_SCALING_READS_V2:
                        LOG.info("WeakScalingReadsV2 Selected!");
                        weakScalingReadOperationV2(hdfsConfiguration, primaryHdfs, nameNodeEndpoint);
                        break;
                    case OP_GET_FILE_STATUS:
                        LOG.info("OP_GET_FILE_STATUS selected!");
                        Commands.getFileStatusOperation(hdfsConfiguration, primaryHdfs, nameNodeEndpoint);
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

    private void toggleBenchmarkMode() {
        boolean toggle = getBooleanFromUser("Enter 'true' to enable benchmark mode and 'false' to disable it.");

        if (toggle) {
            LOG.info("ENABLING benchmark mode. This will also disable 'Followers Track Ops Perf'");
            followersTrackOpsPerformed = false;
        }
        else {
            LOG.info("DISABLING benchmark mode. This will NOT enable 'Followers Track Ops Perf'. That must be done separately.");
        }

        Commands.BENCHMARKING_MODE = toggle;
        primaryHdfs.setBenchmarkModeEnabled(toggle);

        for (DistributedFileSystem hdfs : hdfsClients) {
            hdfs.setBenchmarkModeEnabled(toggle);
        }

        if (!nondistributed) {
            JsonObject payload = new JsonObject();
            String operationId = UUID.randomUUID().toString();
            payload.addProperty(OPERATION, OP_TOGGLE_BENCHMARK_MODE);
            payload.addProperty(OPERATION_ID, operationId);
            payload.addProperty(BENCHMARK_MODE, toggle);

            issueCommandToFollowers((toggle ? "Enabling" : "Disabling" ) + " Benchmark Mode", operationId, payload);
        } else {
            LOG.warn("Running in non-distributed mode. We have no followers.");
        }
    }

    /**
     * Enable/disable followers tracking OperationPerformed instances and sending them after benchmarks.
     */
    private void toggleOperationsPerformedInFollowers() {
        if (!nondistributed) {
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

            issueCommandToFollowers("Toggle Operation Performed", operationId, payload);
        } else {
            LOG.warn("Running in non-distributed mode. We have no followers.");
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
        } else {
            serverlessLogLevel = newLogLevel;
            primaryHdfs.setServerlessFunctionLogLevel(newLogLevel);

            for (DistributedFileSystem hdfs : hdfsClients) {
                hdfs.setServerlessFunctionLogLevel(newLogLevel);
            }
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

        if (newFlag.equalsIgnoreCase("t") || newFlag.equalsIgnoreCase("y")) {
            if (!currentFlag)
                LOG.info("ENABLING consistency protocol.");
            else
                LOG.info("Consistency protocol is already enabled.");

            primaryHdfs.setConsistencyProtocolEnabled(true);
            consistencyEnabled = true;

            for (DistributedFileSystem hdfs : hdfsClients) {
                hdfs.setConsistencyProtocolEnabled(true);
            }
        }
        else if (newFlag.equalsIgnoreCase("f") || newFlag.equalsIgnoreCase("n")) {
            if (currentFlag)
                LOG.info("DISABLING consistency protocol.");
            else
                LOG.info("Consistency protocol is already disabled.");

            primaryHdfs.setConsistencyProtocolEnabled(false);
            consistencyEnabled = false;

            for (DistributedFileSystem hdfs : hdfsClients) {
                hdfs.setConsistencyProtocolEnabled(false);
            }
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

    /**
     * Issue a command to all our followers.
     * @param opName The name of the command.
     * @param operationId Unique ID of this operation.
     * @param payload Contains the command and necessary arguments.
     */
    private void issueCommandToFollowers(String opName, String operationId, JsonObject payload) {
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
        for (Connection followerConnection : followers) {
            LOG.debug("Sending '" + opName + "' operation to follower at " +
                    followerConnection.getRemoteAddressTCP());
            followerConnection.sendTCP(payloadStr);
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

    public void strongScalingWriteOperation(final Configuration configuration,
                                            final DistributedFileSystem sharedHdfs,
                                            final String nameNodeEndpoint)
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

        List<String> directories = null;
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

        String operationId = UUID.randomUUID().toString();
        int numDistributedResults = followers.size();
        if (followers.size() > 0) {
            JsonObject payload = new JsonObject();
            payload.addProperty(OPERATION, OP_STRONG_SCALING_READS);
            payload.addProperty(OPERATION_ID, operationId);
            payload.addProperty("n", writesPerThread);
            payload.addProperty("minLength", minLength);
            payload.addProperty("maxLength", maxLength);
            payload.addProperty("numberOfThreads", numberOfThreads);

            JsonArray directoriesJson = new JsonArray();
            for (String dir : directories)
                directoriesJson.add(dir);

            payload.add("directories", directoriesJson);

            issueCommandToFollowers("Write n Files with n Threads (Weak Scaling - Write)", operationId, payload);
        }

        LOG.info("Each thread should be writing " + writesPerThread + " files...");

        DistributedBenchmarkResult localResult =
                Commands.writeFilesInternal(writesPerThread, minLength, maxLength, numberOfThreads, directories,
                        sharedHdfs, OP_WEAK_SCALING_WRITES, hdfsConfiguration, nameNodeEndpoint, false);
        localResult.setOperationId(operationId);
        localResult.setOperation(OP_WEAK_SCALING_WRITES);

        //LOG.info("LOCAL result of weak scaling benchmark: " + localResult);
        localResult.setOperationId(operationId);

        // Wait for followers' results if we had followers when we first started the operation.
        if (numDistributedResults > 0)
            waitForDistributedResult(numDistributedResults, operationId, localResult);
    }

    public void strongScalingReadOperation(final Configuration configuration,
                                           final DistributedFileSystem sharedHdfs,
                                           final String nameNodeEndpoint)
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
                Commands.strongScalingBenchmark(configuration, sharedHdfs, nameNodeEndpoint, filesPerThread, readsPerFile,
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
    public void weakScalingWriteOperation(final Configuration configuration,
                                          final DistributedFileSystem sharedHdfs,
                                          final String nameNodeEndpoint)
            throws IOException, InterruptedException {
        int directoryChoice = getIntFromUser("Should the threads write their files to the SAME DIRECTORY [1], DIFFERENT DIRECTORIES [2], or RANDOM WRITES [3]?");

        // Validate input.
        if (directoryChoice < 1 || directoryChoice > 3) {
            LOG.error("Invalid argument specified. Should be \"1\" for same directory, \"2\" for different directories. " +
                    "Or \"3\" for random writes. Instead, got \"" + directoryChoice + "\"");
            return;
        }

        int dirInputMethodChoice = getIntFromUser("Manually input (comma-separated list) [1], or specify file containing directories [2]?");

        List<String> directories = null;
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

        // IMPORTANT: Make directories the same size as the number of threads, so we have one directory per thread.
        //            This allows us to directly reuse the writeFilesInternal() function, which creates a certain
        //            number of files per directory. If number of threads is equal to number of directories, then
        //            we are essentially creating a certain number of files per thread, which is what we want.
        assert(directories != null);

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
        } else {
            // Use the entire directories list. We'll generate a bunch of random writes using the full list.
        }

        System.out.print("Number of writes per thread? \n> ");
        int writesPerThread = Integer.parseInt(scanner.nextLine());

        int minLength = 0;
        System.out.print("Min string length (default " + minLength + "):\n> ");
        try {
            minLength = Integer.parseInt(scanner.nextLine());
        } catch (NumberFormatException ex) {
            LOG.info("Defaulting to " + minLength + ".");
        }

        int maxLength = 0;
        System.out.print("Max string length (default " + maxLength + "):\n> ");
        try {
            maxLength = Integer.parseInt(scanner.nextLine());
        } catch (NumberFormatException ex) {
            LOG.info("Defaulting to " + maxLength + ".");
        }

        String operationId = UUID.randomUUID().toString();
        int numDistributedResults = followers.size();
        if (followers.size() > 0) {
            JsonObject payload = new JsonObject();
            payload.addProperty(OPERATION, OP_STRONG_SCALING_READS);
            payload.addProperty(OPERATION_ID, operationId);
            payload.addProperty("n", writesPerThread);
            payload.addProperty("minLength", minLength);
            payload.addProperty("maxLength", maxLength);
            payload.addProperty("numberOfThreads", numberOfThreads);
            payload.addProperty("randomWrites", directoryChoice == 3);

            JsonArray directoriesJson = new JsonArray();
            for (String dir : directories)
                directoriesJson.add(dir);

            payload.add("directories", directoriesJson);

            issueCommandToFollowers("Write n Files with n Threads (Weak Scaling - Write)", operationId, payload);
        }

        LOG.info("Each thread should be writing " + writesPerThread + " files...");

        DistributedBenchmarkResult localResult =
                Commands.writeFilesInternal(writesPerThread, minLength, maxLength, numberOfThreads, directories,
                        sharedHdfs, OP_STRONG_SCALING_READS, hdfsConfiguration, nameNodeEndpoint, (directoryChoice == 3));
        localResult.setOperationId(operationId);
        localResult.setOperation(OP_WEAK_SCALING_WRITES);

        //LOG.info("LOCAL result of weak scaling benchmark: " + localResult);
        localResult.setOperationId(operationId);

        // Wait for followers' results if we had followers when we first started the operation.
        if (numDistributedResults > 0)
            waitForDistributedResult(numDistributedResults, operationId, localResult);
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

            try {
                metricsString = String.format("%f %d %d %f %f %f %f", localResult.getOpsPerSecond(),
                        localResult.cacheHits, localResult.cacheMisses,
                        ((double)localResult.cacheHits / (double)(localResult.cacheHits + localResult.cacheMisses)), localResult.tcpLatencyStatistics.getMean(),
                        localResult.httpLatencyStatistics.getMean(), (localResult.tcpLatencyStatistics.getMean() + localResult.httpLatencyStatistics.getMean()) / 2.0);
            } catch (NullPointerException ex) {
                LOG.warn("Could not generate metrics string due to NPE.");
            }

            return new AggregatedResult(localResult.getOpsPerSecond(), localResult.cacheHits, localResult.cacheMisses, metricsString);
        }

        LOG.debug("Waiting for " + numDistributedResults + " distributed result(s).");
        BlockingQueue<DistributedBenchmarkResult> resultQueue = resultQueues.get(operationId);
        assert(resultQueue != null);

        while (resultQueue.size() < numDistributedResults) {
            Thread.sleep(50);
        }

        DescriptiveStatistics opsPerformed = new DescriptiveStatistics();
        DescriptiveStatistics duration = new DescriptiveStatistics();
        DescriptiveStatistics throughput = new DescriptiveStatistics();
        DescriptiveStatistics cacheHits = new DescriptiveStatistics();
        DescriptiveStatistics cacheMisses = new DescriptiveStatistics();

        opsPerformed.addValue(localResult.numOpsPerformed);
        duration.addValue(localResult.durationSeconds);
        throughput.addValue(localResult.getOpsPerSecond());
        cacheHits.addValue(localResult.cacheHits);
        cacheMisses.addValue(localResult.cacheMisses);

        LOG.debug("========== LOCAL RESULT ==========");
        LOG.debug("Num Ops Performed   : " + localResult.numOpsPerformed);
        LOG.debug("Duration (sec)      : " + localResult.durationSeconds);
        LOG.debug("Cache hits          : " + localResult.cacheHits);
        LOG.debug("Cache misses        : " + localResult.cacheMisses);
        if (localResult.cacheHits + localResult.cacheMisses > 0)
            LOG.debug("Cache hit percentage: " + (localResult.cacheHits/(localResult.cacheHits + localResult.cacheMisses)));
        else
            LOG.debug("Cache hit percentage: N/A");
        LOG.debug("Throughput          : " + localResult.getOpsPerSecond());

        double trialAvgTcpLatency = localResult.tcpLatencyStatistics.getMean();
        double trialAvgHttpLatency = localResult.httpLatencyStatistics.getMean();

        for (DistributedBenchmarkResult res : resultQueue) {
            LOG.debug("========== RECEIVED RESULT FROM " + res.jvmId + " ==========");
            LOG.debug("Num Ops Performed   : " + res.numOpsPerformed);
            LOG.debug("Duration (sec)      : " + res.durationSeconds);
            LOG.debug("Cache hits          : " + res.cacheHits);
            LOG.debug("Cache misses        : " + res.cacheMisses);
            if (res.cacheHits + res.cacheMisses > 0)
                LOG.debug("Cache hit percentage: " + (res.cacheHits/(res.cacheHits + res.cacheMisses)));
            else
                LOG.debug("Cache hit percentage: N/A");
            LOG.debug("Throughput          : " + res.getOpsPerSecond());

            opsPerformed.addValue(res.numOpsPerformed);
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

                trialAvgTcpLatency += latencyTcp.getMean();
                trialAvgHttpLatency += latencyHttp.getMean();
            }

            if (res.opsPerformed != null)
                primaryHdfs.addOperationPerformeds(res.opsPerformed);

            if (res.txEvents != null) {
                for (HashMap<String, List<TransactionEvent>> txEvents : res.txEvents) {
                    //LOG.info("Merging " + txEvents.size() + " new transaction event(s) into master/shared HDFS object.");
                    primaryHdfs.mergeTransactionEvents(txEvents, true);
                }
            }
        }

        trialAvgTcpLatency = trialAvgTcpLatency / (1 + numDistributedResults);   // Add 1 to account for local result.
        trialAvgHttpLatency = trialAvgHttpLatency / (1 + numDistributedResults); // Add 1 to account for local result.
        double aggregateThroughput = (opsPerformed.getSum() / duration.getMean());

        LOG.info("==== AGGREGATED RESULTS ====");
        LOG.info("Average Duration: " + duration.getMean() * 1000.0 + " ms.");
        LOG.info("Cache hits: " + cacheHits.getSum());
        LOG.info("Cache misses: " + cacheMisses.getSum());
        LOG.info("Cache hit percentage: " + (cacheHits.getSum()/(cacheHits.getSum() + cacheMisses.getSum())));
        LOG.info("Average TCP latency: " + trialAvgTcpLatency + " ms");
        LOG.info("Average HTTP latency: " + trialAvgHttpLatency + " ms");
        LOG.info("Average combined latency: " + (trialAvgTcpLatency + trialAvgHttpLatency) / 2.0 + " ms");
        LOG.info("Aggregate Throughput (ops/sec): " + aggregateThroughput);

        String metricsString = String.format("%f %f %f %f %f %f %f", aggregateThroughput, cacheHits.getSum(), cacheMisses.getSum(),
                (cacheHits.getSum()/(cacheHits.getSum() + cacheMisses.getSum())), trialAvgTcpLatency,
                trialAvgHttpLatency, (trialAvgTcpLatency + trialAvgHttpLatency) / 2.0);

        LOG.info(metricsString);

        return new AggregatedResult(aggregateThroughput, (int)cacheHits.getSum(), (int)cacheMisses.getSum(), metricsString);
    }

    /**
     * Weak scaling, reads.
     *
     * Query the user for:
     *  - An integer `n`, the number of threads.
     *  - The path to a local file containing HopsFS file paths.
     *  - The number of files each thread should read.
     */
    private void weakScalingReadOperationV2(final Configuration configuration,
                                            final DistributedFileSystem sharedHdfs,
                                            final String nameNodeEndpoint)
            throws InterruptedException, FileNotFoundException {
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

        int currentTrial = 0;
        AggregatedResult[] aggregatedResults = new AggregatedResult[numTrials];
        Double[] results = new Double[numTrials];
        Integer[] cacheHits = new Integer[numTrials];
        Integer[] cacheMisses = new Integer[numTrials];
        while (currentTrial < numTrials) {
            LOG.info("|====| TRIAL #" + currentTrial + " |====|");
            String operationId = UUID.randomUUID().toString();
            int numDistributedResults = followers.size();
            if (followers.size() > 0) {
                JsonObject payload = new JsonObject();
                payload.addProperty(OPERATION, OP_WEAK_SCALING_READS_V2);
                payload.addProperty(OPERATION_ID, operationId);
                payload.addProperty("n", numThreads);
                payload.addProperty("filesPerThread", filesPerThread);
                payload.addProperty("inputPath", inputPath);
                payload.addProperty("shuffle", shuffle);

                issueCommandToFollowers("Read n Files with n Threads (Weak Scaling - Read)", operationId, payload);
            }

            // TODO: Make this return some sort of 'result' object encapsulating the result.
            //       Then, if we have followers, we'll wait for their results to be sent to us, then we'll merge them.
            DistributedBenchmarkResult localResult =
                    Commands.weakScalingBenchmarkV2(configuration, sharedHdfs, nameNodeEndpoint, numThreads,
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
            cacheHits[currentTrial] = aggregatedResult.cacheHits;
            cacheMisses[currentTrial] = aggregatedResult.cacheMisses;
            currentTrial++;

            if (!(currentTrial >= numTrials)) {
                LOG.info("Trial " + currentTrial + "/" + numTrials + " completed. Performing GC and sleeping for " +
                        postTrialSleepInterval + " ms.");
                performClientVMGarbageCollection();
                Thread.sleep(postTrialSleepInterval);
            }
        }

        System.out.println("[THROUGHPUT]");
        for (double throughputResult : results) {
            System.out.println(throughputResult);
        }
        System.out.println("\n[CACHE HITS] [CACHE MISSES] [HIT RATE]");
        String formatString = "%-12s %-14s %10f";
        for (int i = 0; i < numTrials; i++) {
            System.out.printf((formatString) + "%n", cacheHits[i], cacheMisses[i], ((double)cacheHits[i] / (cacheHits[i] + cacheMisses[i])));
        }

        for (AggregatedResult result : aggregatedResults)
            System.out.println(result.metricsString);
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
    private void weakScalingReadOperation(final Configuration configuration,
                                          final DistributedFileSystem sharedHdfs,
                                          final String nameNodeEndpoint)
            throws InterruptedException, FileNotFoundException {
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

        int currentTrial = 0;
        AggregatedResult[] aggregatedResults = new AggregatedResult[numTrials];
        Double[] results = new Double[numTrials];
        Integer[] cacheHits = new Integer[numTrials];
        Integer[] cacheMisses = new Integer[numTrials];
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
                    Commands.weakScalingReadsV1(configuration, sharedHdfs, nameNodeEndpoint, n, readsPerFile,
                            inputPath, shuffle, OP_WEAK_SCALING_READS);

            if (localResult == null) {
                LOG.warn("Local result is null. Aborting.");
                return;
            }

            //LOG.info("LOCAL result of weak scaling benchmark: " + localResult);
            localResult.setOperationId(operationId);

            localResult.setOperationId(operationId);
            double throughput = 0;
            int aggregatedCacheMisses = 0;
            int aggregatedCacheHits = 0;
            // Wait for followers' results if we had followers when we first started the operation.
            AggregatedResult aggregatedResult = waitForDistributedResult(numDistributedResults, operationId, localResult);
            throughput = aggregatedResult.throughput;
            aggregatedCacheHits = aggregatedResult.cacheHits;
            aggregatedCacheMisses = aggregatedResult.cacheMisses;
            aggregatedResults[currentTrial] = aggregatedResult;

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

        System.out.println("[THROUGHPUT]");
        for (double throughputResult : results) {
            System.out.println(throughputResult);
        }
        System.out.println("\n[CACHE HITS] [CACHE MISSES] [HIT RATE]");
        String formatString = "%-12s %-14s %10f";
        for (int i = 0; i < numTrials; i++) {
            System.out.printf((formatString) + "%n", cacheHits[i], cacheMisses[i], ((double)cacheHits[i] / (cacheHits[i] + cacheMisses[i])));
        }

        for (AggregatedResult result : aggregatedResults)
            System.out.println(result.metricsString);
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
     * @param nameNodeEndpoint Where HTTP requests are directed.
     * @param creatingPrimary Are we creating the primary/shared instance?
     */
    public static DistributedFileSystem initDfsClient(DistributedFileSystem primaryHdfs,
                                                      String nameNodeEndpoint,
                                                      boolean creatingPrimary) {
        LOG.debug("Creating HDFS client now...");
        Configuration hdfsConfiguration = Utils.getConfiguration(hdfsConfigFilePath);
        try {
            hdfsConfiguration.addResource(new File(hdfsConfigFilePath).toURI().toURL());
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

        // For the HDFS instance we're creating, toggle the consistency protocol + benchmark mode
        // based on whether the client has toggled those options within the benchmarking application.
        hdfs.setConsistencyProtocolEnabled(consistencyEnabled);
        hdfs.setBenchmarkModeEnabled(Commands.BENCHMARKING_MODE);

        // The primary HDFS instance should use whatever the default log level is for the HDFS instance we create,
        // as HopsFS has a default log level. If we're creating a non-primary HDFS instance, then we just assign it
        // whatever our primary instance has been set to (as it can change dynamically).
        if (creatingPrimary) {
            serverlessLogLevel = hdfs.getServerlessFunctionLogLevel();
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
        public long name = -1; // Hides super type.

        /**
         * Default constructor.
         */
        public FollowerConnection() {

        }

        @Override
        public String toString() {
            return this.name != -1 ? String.valueOf(this.name) : super.toString();
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
            conn.setKeepAliveTCP(6000);
            conn.setTimeout(12000);
            followers.add(conn);

            JsonObject registrationPayload = new JsonObject();
            registrationPayload.addProperty(OPERATION, OP_REGISTRATION);
            registrationPayload.addProperty(NAMENODE_ENDPOINT, nameNodeEndpoint);
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
            if (object instanceof String) {
                JsonObject body = new JsonParser().parse((String)object).getAsJsonObject();
                LOG.debug("Received message from follower: " + body);
            }
            else if (object instanceof DistributedBenchmarkResult) {
                DistributedBenchmarkResult result = (DistributedBenchmarkResult)object;

                LOG.info("Received result from follower: " + result);

                String opId = result.opId;

                BlockingQueue<DistributedBenchmarkResult> resultQueue = resultQueues.get(opId);
                resultQueue.add(result);
            }
            else if (object instanceof FrameworkMessage.KeepAlive) {
                // Do nothing...
            }
            else {
                LOG.error("Received object of unexpected/unsupported type " + object.getClass().getSimpleName());
            }
        }

        public void disconnected(Connection conn) {
            LOG.info("Lost connection to follower.");
            followers.remove(conn);
        }
    }

    private static void printMenu() {
        System.out.println("");
        System.out.println("====== MENU ======");
        System.out.println("Debug Operations:");
        System.out.println(
                "(-11) Toggle 'Benchmarking Mode' in self and followers\n" +
                "(-10) Toggle 'OperationPerformed' tracking in followers\n" +
                "(-9)  Perform client VM garbage collection\n" +
                "(-8)  Print/modify post-trial sleep interval\n" +
                "(-7)  Print currently active NameNodes\n" +
                "(-6)  Get/set consistency protocol enabled flag.\n(-5) Get/set serverless log4j debug level.\n" +
                "(-4)  Clear statistics\n(-3) Output statistics packages to CSV\n" +
                "(-2)  Output operations performed + write to file\n(-1) Print TCP debug information.");
        System.out.println("\nStandard Operations:");
        System.out.println("(0) Exit\n(1) Create file\n(2) Create directory\n(3) Read contents of file.\n(4) Rename" +
                "\n(5) Delete\n(6) List directory\n(7) Append\n(8) Create Subtree.\n(9) Ping\n(10) Prewarm" +
                "\n(11) Write Files to Directory\n(12) Read files\n(13) Delete files\n(14) Write Files to Directories" +
                "\n(15) Read n Files with n Threads (Weak Scaling - Read)\n(16) Read n Files y Times with z Threads (Strong Scaling - Read)" +
                "\n(17) Write n Files with n Threads (Weak Scaling - Write)\n(18) Write n Files y Times with z Threads (Strong Scaling - Write)" +
                "\n(19) Create directories.\n(20) Weak Scaling Reads v2\n(21) File Stat Benchmark\n");
        System.out.println("==================");
        System.out.println("");
        System.out.println("What would you like to do?");
        System.out.print("> ");
    }

    public static class AggregatedResult {
        public double throughput;
        public int cacheHits;
        public int cacheMisses;
        public String metricsString; // All the metrics I'd want formatted so I can copy and paste into Excel.

        public AggregatedResult(double throughput, int cacheHits, int cacheMisses, String metricsString) {
            this.throughput = throughput;
            this.cacheHits = cacheHits;
            this.cacheMisses = cacheMisses;
            this.metricsString = metricsString;
        }
    }
}
