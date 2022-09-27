package com.gmail.benrcarver.distributed;

import com.esotericsoftware.kryonet.Client;
import com.esotericsoftware.kryonet.Connection;
import com.esotericsoftware.kryonet.Listener;
import com.gmail.benrcarver.distributed.coin.BMConfiguration;
import com.gmail.benrcarver.distributed.util.Utils;
import com.gmail.benrcarver.distributed.workload.RandomlyGeneratedWorkload;
import com.gmail.benrcarver.distributed.workload.WorkloadResponse;
import com.google.gson.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DistributedFileSystem;

import java.io.*;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.net.*;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static com.gmail.benrcarver.distributed.Commands.hdfsClients;
import static com.gmail.benrcarver.distributed.Constants.*;

/**
 * Runs HopsFS benchmarks as directed by a {@link Commander}.
 */
public class Follower {
    public static final Logger LOG = LoggerFactory.getLogger(Follower.class);

    private static final int CONN_TIMEOUT_MILLISECONDS = 5000;

    private String nameNodeEndpoint;

    private final Client client;

    private final String masterIp;
    private final int masterPort;

    private String hdfsConfigFilePath;
    private Configuration hdfsConfiguration;
    private DistributedFileSystem hdfs;

    private RandomlyGeneratedWorkload activeWorkload;

    /**
     * The approximate number of collections that occurred.
     */
    private long numGarbageCollections = 0L;

    /**
     * The approximate time, in milliseconds, that has elapsed during GCs
     */
    private long garbageCollectionTime = 0L;

    // TODO: Make it so we can change these dynamically.
    private String serverlessLogLevel;
    private boolean consistencyEnabled = true;

    public static final int FOLLOWER_TCP_BUFFER_SIZES = (int)128e6;

    public synchronized void waitUntilDone() throws InterruptedException {
        this.wait();
    }

    public Follower(String masterIp, int masterPort, boolean disableConsistency) {
        client = new Client(FOLLOWER_TCP_BUFFER_SIZES, FOLLOWER_TCP_BUFFER_SIZES);
        this.masterIp = masterIp;
        this.masterPort = masterPort;
        this.consistencyEnabled = !disableConsistency;

        Commands.IS_FOLLOWER = true;

        client.addListener(new Listener.ThreadedListener(new Listener() {
            /**
             * This listener is responsible for handling messages received from HopsFS clients. These messages will
             * generally be file system operation requests/directions. We will extract the information about the
             * operation, create a task, submit the task to be executed by our worker thread, then return the result
             * back to the HopsFS client.
             * @param connection The connection with the HopsFS client.
             * @param object The object that the client sent to us.
             */
            public void received(Connection connection, Object object) {
                // If we received a JsonObject, then add it to the queue for processing.
                if (object instanceof String) {
                    LOG.debug("Received message from Leader.");
                    JsonObject message = new JsonParser().parse((String) object).getAsJsonObject();
                    try {
                        handleMessageFromLeader(message, hdfs);
                    } catch (IOException e) {
                        LOG.error("Encountered IOException while handling message from Leader:", e);
                    } catch (InterruptedException e) {
                        LOG.error("Encountered InterruptedException while handling message from Leader:", e);
                    } catch (ExecutionException e) {
                        LOG.error("Encountered ExecutionException while handling message from Leader:", e);
                    }
                }
            }

            public void disconnected(Connection connection) {
                LOG.error("Follower lost connection to the Leader. Exiting...");
                System.exit(1);
            }
        }));
    }

    private void stopClient() {
        client.stop();
        this.notifyAll();
    }

    private DistributedFileSystem initDfsClient() {
        LOG.debug("Creating HDFS client now...");
        hdfsConfiguration = Utils.getConfiguration(hdfsConfigFilePath);
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

        hdfs.setConsistencyProtocolEnabled(consistencyEnabled);

        return hdfs;
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

    private void handleMessageFromLeader(JsonObject message, DistributedFileSystem hdfs) throws IOException, InterruptedException, ExecutionException {
        int operation = message.getAsJsonPrimitive(OPERATION).getAsInt();

        updateGCMetrics();
        long currentGCs = numGarbageCollections;
        long currentGCTime = garbageCollectionTime;

        String operationId = "N/A";
        if (message.has(OPERATION_ID)) {
            operationId = message.getAsJsonPrimitive(OPERATION_ID).getAsString();
            LOG.info("Received operation " + operationId + " from Leader.");
            LOG.debug("Current Number of GCs: " + numGarbageCollections);
            LOG.debug("Time spent GC-ing: " + garbageCollectionTime + " ms");
        }

        switch(operation) {
            case OP_SET_HTTP_TCP_REPLACEMENT_CHANCE:
                double chance = message.getAsJsonPrimitive("CHANCE").getAsDouble();

                hdfs.setHttpTcpReplacementChance(chance);

                for (DistributedFileSystem dfs : hdfsClients) {
                    dfs.setHttpTcpReplacementChance(chance);
                }
                break;
            case OP_SET_CONSISTENCY_PROTOCOL_ENABLED:
                boolean toggle = message.getAsJsonPrimitive("TOGGLE").getAsBoolean();

                hdfs.setConsistencyProtocolEnabled(toggle);

                for (DistributedFileSystem dfs : hdfsClients) {
                    dfs.setConsistencyProtocolEnabled(toggle);
                }
                break;
            case OP_SET_LOG_LEVEL:
                String newLogLevel = message.getAsJsonPrimitive("LOG_LEVEL").getAsString();
                hdfs.setServerlessFunctionLogLevel(newLogLevel);

                for (DistributedFileSystem dfs : hdfsClients) {
                    dfs.setServerlessFunctionLogLevel(newLogLevel);
                }
                break;
            case OP_TOGGLE_BENCHMARK_MODE:
                boolean benchmarkModeEnabled = message.getAsJsonPrimitive(BENCHMARK_MODE).getAsBoolean();

                if (benchmarkModeEnabled)
                    LOG.debug("ENABLING Benchmark Mode (and thus disabling OperationPerformed tracking).");
                else
                    LOG.debug("DISABLING Benchmark Mode.");

                Commands.BENCHMARKING_MODE = benchmarkModeEnabled;
                hdfs.setBenchmarkModeEnabled(benchmarkModeEnabled);

                if (benchmarkModeEnabled)
                    Commands.TRACK_OP_PERFORMED = false;

                break;
            case OP_TOGGLE_OPS_PERFORMED_FOLLOWERS:
                toggle = message.getAsJsonPrimitive(TRACK_OP_PERFORMED).getAsBoolean();

                if (toggle)
                    LOG.debug("ENABLING OperationPerformed tracking.");
                else
                    LOG.debug("DISABLING OperationPerformed tracking.");

                Commands.TRACK_OP_PERFORMED = toggle;
                break;
            case OP_TRIGGER_CLIENT_GC:
                LOG.debug("We've been instructed to perform a garbage collection!");

                if (Commands.BENCHMARKING_MODE) {
                    LOG.debug("Clearing statistics packages first since Benchmarking Mode is enabled.");
                    Commands.clearMetricDataNoPrompt(hdfs);
                }

                LOG.debug("Calling System.gc() now!");
                System.gc();
                break;
            case OP_REGISTRATION:
                handleRegistration(message);
                break;
            case OP_CLEAR_METRIC_DATA:
                LOG.info("Clearing statistics packages...");
                Commands.clearMetricData(hdfs);
                break;
            case OP_WRITE_STATISTICS:
                LOG.info("Writing statistics packages to files...");
                LOG.info("");
                hdfs.dumpStatisticsPackages(true);
                break;
            case OP_PRINT_OPS_PERFORMED:
                LOG.info("Printing operations performed...");
                LOG.info("");
                Commands.printOperationsPerformed(hdfs);
                break;
            case OP_PRINT_TCP_DEBUG:
                LOG.info("Printing TCP debug information...");
                LOG.info("");
                hdfs.printDebugInformation();
                break;
            case OP_EXIT:
                LOG.info("Received 'STOP' operation from Leader. Shutting down primary HDFS connection now.");
                try {
                    hdfs.close();
                } catch (IOException ex) {
                    LOG.info("Encountered exception while closing file system...");
                    ex.printStackTrace();
                }
                LOG.info("Stopping TCP client now.");
                stopClient();
                LOG.info("Exiting now... goodbye!");
                System.exit(0);
            case OP_CREATE_FILE:
                LOG.info("CREATE FILE selected!");
                Commands.createFileOperation(hdfs);
                break;
            case OP_MKDIR:
                LOG.info("MAKE DIRECTORY selected!");
                Commands.mkdirOperation(hdfs);;
                break;
            case OP_READ_FILE:
                LOG.info("READ FILE selected!");
                Commands.readOperation(hdfs);
                break;
            case OP_RENAME:
                LOG.info("RENAME selected!");
                Commands.renameOperation(hdfs);
                break;
            case OP_DELETE:
                LOG.info("DELETE selected!");
                Commands.deleteOperation(hdfs);
                break;
            case OP_LIST:
                LOG.info("LIST selected!");
                Commands.listOperation(hdfs);
                break;
            case OP_APPEND:
                LOG.info("APPEND selected!");
                Commands.appendOperation(hdfs);
                break;
            case OP_CREATE_SUBTREE:
                LOG.info("CREATE SUBTREE selected!");
                Commands.createSubtree(hdfs);
                break;
            case OP_PING:
                LOG.info("PING selected!");
                Commands.pingOperation(hdfs);
                break;
            case OP_PREWARM:
                LOG.info("PREWARM selected!");
                Commands.prewarmOperation(hdfs);
                break;
            case OP_WRITE_FILES_TO_DIR:
                LOG.info("WRITE FILES TO DIRECTORY selected!");
                Commands.writeFilesToDirectory(hdfs);
                break;
            case OP_READ_FILES:
                LOG.info("READ FILES selected!");
                Commands.readFilesOperation(hdfs, OP_READ_FILES);
                break;
            case OP_DELETE_FILES:
                LOG.info("DELETE FILES selected!");
                Commands.deleteFilesOperation(hdfs);
                break;
            case OP_WRITE_FILES_TO_DIRS:
                LOG.info("WRITE FILES TO DIRECTORIES selected!");
                Commands.writeFilesToDirectories(hdfs);
                break;
            case OP_WEAK_SCALING_READS:
                LOG.info("'Read n Files with n Threads (Weak Scaling - Read)' selected!");
                DistributedBenchmarkResult result = Commands.weakScalingReadsV1(hdfs,
                        message.getAsJsonPrimitive("n").getAsInt(),
                        message.getAsJsonPrimitive("readsPerFile").getAsInt(),
                        message.getAsJsonPrimitive("inputPath").getAsString(),
                        message.getAsJsonPrimitive("shuffle").getAsBoolean(),
                        OP_WEAK_SCALING_READS);
                assert result != null;
                result.setOperationId(operationId);
                LOG.info("Obtained local result for WEAK SCALING (READ) benchmark: " + result);
                sendResultToLeader(result);
                break;
            case OP_STRONG_SCALING_READS:
                LOG.info("'Read n Files y Times with z Threads (Strong Scaling - Read)' selected!");
                result = Commands.strongScalingBenchmark(hdfs,
                        message.getAsJsonPrimitive("n").getAsInt(),
                        message.getAsJsonPrimitive("readsPerFile").getAsInt(),
                        message.getAsJsonPrimitive("numThreads").getAsInt(),
                        message.getAsJsonPrimitive("inputPath").getAsString());
                assert result != null;
                result.setOperationId(operationId);
                LOG.info("Obtained local result for STRONG SCALING (READ) benchmark: " + result);
                sendResultToLeader(result);
                break;
            case OP_WEAK_SCALING_WRITES:
                LOG.info("'Write n Files with n Threads (Weak Scaling - Write)' selected!");
                JsonArray directoriesJson = message.getAsJsonArray("directories");
                List<String> directories = new ArrayList<>();
                for (JsonElement elem : directoriesJson) {
                    directories.add(elem.getAsString());
                }
                result = Commands.writeFilesInternal(
                        message.getAsJsonPrimitive("n").getAsInt(),
                        message.getAsJsonPrimitive("numberOfThreads").getAsInt(),
                        directories, hdfs, OP_WEAK_SCALING_WRITES,
                        message.getAsJsonPrimitive("randomWrites").getAsBoolean());
                result.setOperationId(operationId);
                LOG.info("Obtained local result for WEAK SCALING (WRITE) benchmark: " + result);
                sendResultToLeader(result);
                break;
            case OP_STRONG_SCALING_WRITES:
                LOG.info("'Write n Files y Times with z Threads (Strong Scaling - Write)' selected!");

                directoriesJson = message.getAsJsonPrimitive("directories").getAsJsonArray();
                directories = new ArrayList<>();
                for (JsonElement elem : directoriesJson) {
                    directories.add(elem.getAsString());
                }
                result = Commands.writeFilesInternal(
                        message.getAsJsonPrimitive("n").getAsInt(),
                        message.getAsJsonPrimitive("numThreads").getAsInt(),
                        directories, hdfs, OP_STRONG_SCALING_WRITES,
                        false);
                result.setOperationId(operationId);
                LOG.info("Obtained local result for STRONG SCALING (WRITE) benchmark: " + result);
                sendResultToLeader(result);
                break;
            case OP_WEAK_SCALING_READS_V2:
                LOG.info("OP_WEAK_SCALING_READS_V2 selected!");
                result = Commands.weakScalingBenchmarkV2(hdfs,
                        message.getAsJsonPrimitive("numThreads").getAsInt(),
                        message.getAsJsonPrimitive("filesPerThread").getAsInt(),
                        message.getAsJsonPrimitive("inputPath").getAsString(),
                        message.getAsJsonPrimitive("shuffle").getAsBoolean(), OP_WEAK_SCALING_READS_V2);
                assert result != null;
                result.setOperationId(operationId);
                LOG.info("Obtained local result for OP_WEAK_SCALING_READS_V2 benchmark: " + result);
                sendResultToLeader(result);
                break;
            case OP_GET_FILE_STATUS:
                LOG.info("OP_GET_FILE_STATUS selected!");
                result = Commands.getFileStatusOperation(hdfs, nameNodeEndpoint);
                assert result != null;
                result.setOperationId(operationId);
                LOG.info("Obtained local result for OP_GET_FILE_DIR_INFO benchmark: " + result);
                sendResultToLeader(result);
                break;
            case OP_LIST_DIRECTORIES_FROM_FILE:
                LOG.info("LIST DIRECTORIES FROM FILE selected!");
                result = Commands.listDirectoryWeakScaling(hdfs,
                        message.getAsJsonPrimitive("n").getAsInt(),
                        message.getAsJsonPrimitive("listsPerFile").getAsInt(),
                        message.getAsJsonPrimitive("inputPath").getAsString(),
                        message.getAsJsonPrimitive("shuffle").getAsBoolean(), OP_LIST_DIRECTORIES_FROM_FILE);

                result.setOperationId(operationId);
                LOG.info("Obtained local result for LIST DIRECTORIES FROM FILE benchmark: " + result);
                sendResultToLeader(result);
                break;
            case OP_STAT_FILES_WEAK_SCALING:
                LOG.info("STAT FILES WEAK SCALING selected!");
                result = Commands.statFilesWeakScaling(hdfs,
                        message.getAsJsonPrimitive("numThreads").getAsInt(),
                        message.getAsJsonPrimitive("filesPerThread").getAsInt(),
                        message.getAsJsonPrimitive("inputPath").getAsString(),
                        message.getAsJsonPrimitive("shuffle").getAsBoolean(), OP_STAT_FILES_WEAK_SCALING);
                result.setOperationId(operationId);
                LOG.info("Obtained local result for STAT FILES WEAK SCALING benchmark: " + result);
                sendResultToLeader(result);
                break;
            case OP_MKDIR_WEAK_SCALING:
                LOG.info("MKDIR WEAK SCALING selected!");
                directoriesJson = message.getAsJsonArray("directories");
                directories = new ArrayList<>();
                for (JsonElement elem : directoriesJson)
                    directories.add(elem.getAsString());

                result = Commands.mkdirWeakScaling(
                        hdfs, message.getAsJsonPrimitive("n").getAsInt(),
                        message.getAsJsonPrimitive("numberOfThreads").getAsInt(),
                        directories, OP_MKDIR_WEAK_SCALING,
                        message.getAsJsonPrimitive("randomMkdirs").getAsBoolean());
                result.setOperationId(operationId);
                LOG.info("Obtained local result for WEAK SCALING (MKDIR) benchmark: " + result);
                sendResultToLeader(result);
                break;
            case OP_PREPARE_GENERATED_WORKLOAD:
                LOG.info("RANDOMLY-GENERATED WORKLOAD selected!");

                String base64Config = message.getAsJsonPrimitive("configuration").getAsString();
                byte[] configBytes = Base64.getDecoder().decode(base64Config);

                ByteArrayInputStream bis = new ByteArrayInputStream(configBytes);
                ObjectInput in = null;
                BMConfiguration configuration = null;
                try {
                    in = new ObjectInputStream(bis);
                    configuration = (BMConfiguration) in.readObject();
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                } finally {
                    try {
                        if (in != null) {
                            in.close();
                        }
                    } catch (IOException ex) {
                        // ignore close exception
                    }
                }

                if (configuration == null) {
                    LOG.error("ERROR: Could not deserialize BMConfiguration object.");
                    sendResultToLeader(new WorkloadResponse(true, null), operationId);
                } else {
                    activeWorkload = new RandomlyGeneratedWorkload(configuration, hdfs);
                    sendResultToLeader(new WorkloadResponse(false, null), operationId);
                }
                break;
            case OP_DO_WARMUP_FOR_PREPARED_WORKLOAD:
                if (activeWorkload == null || activeWorkload.getCurrentState() != RandomlyGeneratedWorkload.WorkloadState.CREATED) {
                    LOG.error("We do not have an already-created workload.");
                    sendResultToLeader(new WorkloadResponse(true, null), operationId);
                } else {
                    activeWorkload.doWarmup();

                    if (activeWorkload.getCurrentState() == RandomlyGeneratedWorkload.WorkloadState.READY) {
                        LOG.debug("Successfully warmed-up random workload.");
                        sendResultToLeader(new WorkloadResponse(false, null), operationId);
                    }
                    else {
                        LOG.error("Failed to warm-up random workload.");
                        sendResultToLeader(new WorkloadResponse(true, null), operationId);
                    }
                }
                break;
            case OP_DO_RANDOM_WORKLOAD:
                if (activeWorkload == null || activeWorkload.getCurrentState() != RandomlyGeneratedWorkload.WorkloadState.READY) {
                    LOG.error("We do not have an already-created workload.");

                    sendResultToLeader(new WorkloadResponse(true, null), operationId);
                } else {
                    result = activeWorkload.doWorkload(operationId);

                    if (activeWorkload.getCurrentState() == RandomlyGeneratedWorkload.WorkloadState.FINISHED) {
                        LOG.debug("Successfully executed random workload.");
                        sendResultToLeader(result);
                    }
                    else {
                        LOG.error("Failed to execute random workload.");
                        sendResultToLeader(new WorkloadResponse(true, null), operationId);
                    }
                }

                break;
            case OP_ABORT_RANDOM_WORKLOAD:
                if (activeWorkload == null) {
                    LOG.error("We do not have a random workload to abort.");

                    sendResultToLeader(new WorkloadResponse(true, null), operationId);
                } else {
                    LOG.warn("Aborting randomly-generated workload.");
                    activeWorkload = null;
                    sendResultToLeader(new WorkloadResponse(false, null), operationId);
                }
                break;
            default:
                LOG.info("ERROR: Unknown or invalid operation specified: " + operation);
                break;
        }

        updateGCMetrics();
        long numGCsPerformedDuringLastOp = numGarbageCollections - currentGCs;
        long timeSpentInGCDuringLastOp = garbageCollectionTime - currentGCTime;

        LOG.debug("Performed " + numGCsPerformedDuringLastOp + " garbage collection(s) during last operation.");
        if (numGCsPerformedDuringLastOp > 0)
            LOG.debug("Spent " + timeSpentInGCDuringLastOp + " ms garbage collecting during the last operation.");
    }

    private void sendResultToLeader(DistributedBenchmarkResult result) {
        assert result != null;
        LOG.debug("Sending result for operation " + result.opId + " now...");
        int bytesSent = this.client.sendTCP(result);
        LOG.debug("Successfully sent " + bytesSent + " byte(s) to leader.");
    }

    private void sendResultToLeader(WorkloadResponse resp, String opId) {
        LOG.debug("Sending result for operation " + opId + " now...");
        int bytesSent = this.client.sendTCP(resp);
        LOG.debug("Successfully sent " + bytesSent + " byte(s) to leader.");
    }

    private void handleRegistration(JsonObject message) {
        LOG.debug("Received REGISTRATION message from Leader.");
        nameNodeEndpoint = message.getAsJsonPrimitive(NAMENODE_ENDPOINT).getAsString();
        hdfsConfigFilePath = message.getAsJsonPrimitive(HDFS_CONFIG_PATH).getAsString();
        Commands.TRACK_OP_PERFORMED = message.getAsJsonPrimitive(TRACK_OP_PERFORMED).getAsBoolean();

        if (Commands.TRACK_OP_PERFORMED)
            LOG.debug("ENABLING OperationPerformed tracking.");
        else
            LOG.debug("DISABLING OperationPerformed tracking.");

        // The initDfsClient() function in the Commander file uses the Commander's static 'hdfsConfigFilePath'
        // variable. This is basically a hack, pretty gross.
        Commander.hdfsConfigFilePath = hdfsConfigFilePath;
        LOG.debug("NameNode Endpoint: " + nameNodeEndpoint);
        LOG.debug("hdfsConfigFilePath: " + hdfsConfigFilePath);

        hdfs = initDfsClient();
        serverlessLogLevel = "INFO";
        hdfs.setServerlessFunctionLogLevel(serverlessLogLevel);
    }

    public void connect() {
        Network.register(client);

        Thread connectThread = new Thread(() -> {
            client.start();

            try {
                client.connect(CONN_TIMEOUT_MILLISECONDS, masterIp, masterPort, masterPort + 1);
            } catch (IOException e) {
                LOG.error("Failed to connect to Leader.", e);
            }
        });
        connectThread.start();

        client.setKeepAliveTCP(5000);
        client.setTimeout(30000);
        // client.setKeepAliveUDP(7500);

        try {
            connectThread.join();
        } catch (InterruptedException ex) {
            LOG.warn("InterruptedException encountered while trying to connect to HopsFS Client via TCP:", ex);
        }

        if (client.isConnected())
            LOG.info("Successfully connected to master at " + masterIp + ":" + masterPort + ".");
        else {
            LOG.error("Failed to connect to master at " + masterIp + ":" + masterPort + ".");
            System.exit(1);
        }

        String message;

        try {
            message = "Hello Commander. I am Follower " + InetAddress.getLocalHost().getHostAddress() + ".";
        } catch (UnknownHostException e) {
            message = "Hello Commander. I am one of your followers.";
        }

        // Basically serves as a registration.
        client.sendTCP(message);
    }
}
