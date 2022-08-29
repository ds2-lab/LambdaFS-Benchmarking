package com.gmail.benrcarver.distributed;

import com.esotericsoftware.kryonet.Client;
import com.esotericsoftware.kryonet.Connection;
import com.esotericsoftware.kryonet.Listener;
import com.gmail.benrcarver.distributed.util.Utils;
import com.google.gson.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DistributedFileSystem;

import java.io.File;
import java.io.IOException;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.net.*;
import java.util.ArrayList;
import java.util.List;

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

    /**
     * The approximate number of collections that occurred.
     */
    private long numGarbageCollections = 0L;

    /**
     * The approximate time, in milliseconds, that has elapsed during GCs
     */
    private long garbageCollectionTime = 0L;

    public static final int FOLLOWER_TCP_BUFFER_SIZES = (int)128e6;

    public synchronized void waitUntilDone() throws InterruptedException {
        this.wait();
    }

    public Follower(String masterIp, int masterPort) {
        client = new Client(FOLLOWER_TCP_BUFFER_SIZES, FOLLOWER_TCP_BUFFER_SIZES);
        this.masterIp = masterIp;
        this.masterPort = masterPort;

        LOG.debug("Follower being created. Commander IP: " + masterIp + ", commander port: " + masterPort);

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
                    } catch (Exception e) {
                        LOG.error("Unexpected exception while handling message from Leader:", e);
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

    private void handleMessageFromLeader(JsonObject message, DistributedFileSystem hdfs) throws IOException, InterruptedException {
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
            case OP_TOGGLE_BENCHMARK_MODE:
                boolean benchmarkModeEnabled = message.getAsJsonPrimitive(BENCHMARK_MODE).getAsBoolean();

                if (benchmarkModeEnabled)
                    LOG.debug("ENABLING Benchmark Mode (and thus disabling OperationPerformed tracking).");
                else
                    LOG.debug("DISABLING Benchmark Mode.");

                Commands.BENCHMARKING_MODE = benchmarkModeEnabled;

                if (benchmarkModeEnabled)
                    Commands.TRACK_OP_PERFORMED = false;

                break;
            case OP_TOGGLE_OPS_PERFORMED_FOLLOWERS:
                boolean toggle = message.getAsJsonPrimitive(TRACK_OP_PERFORMED).getAsBoolean();

                if (toggle)
                    LOG.debug("ENABLING OperationPerformed tracking.");
                else
                    LOG.debug("DISABLING OperationPerformed tracking.");

                Commands.TRACK_OP_PERFORMED = toggle;
                break;
            case OP_TRIGGER_CLIENT_GC:
                LOG.debug("We've been instructed to perform a garbage collection. Calling System.gc() now!");
                System.gc();
                break;
            case OP_REGISTRATION:
                handleRegistration(message);
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
                Commands.createFileOperation(hdfs, nameNodeEndpoint);
                break;
            case OP_MKDIR:
                LOG.info("MAKE DIRECTORY selected!");
                Commands.mkdirOperation(hdfs, nameNodeEndpoint);;
                break;
            case OP_READ_FILE:
                LOG.info("READ FILE selected!");
                Commands.readOperation(hdfs, nameNodeEndpoint);
                break;
            case OP_RENAME:
                LOG.info("RENAME selected!");
                Commands.renameOperation(hdfs, nameNodeEndpoint);
                break;
            case OP_DELETE:
                LOG.info("DELETE selected!");
                Commands.deleteOperation(hdfs, nameNodeEndpoint);
                break;
            case OP_LIST:
                LOG.info("LIST selected!");
                Commands.listOperation(hdfs, nameNodeEndpoint);
                break;
            case OP_APPEND:
                LOG.info("APPEND selected!");
                Commands.appendOperation(hdfs, nameNodeEndpoint);
                break;
            case OP_CREATE_SUBTREE:
                LOG.info("CREATE SUBTREE selected!");
                Commands.createSubtree(hdfs, nameNodeEndpoint);
                break;
            case OP_PING:
                LOG.warn("PING operation is not supported for Vanilla HopsFS.");
                break;
            case OP_PREWARM:
                LOG.warn("The PREWARM operation is NOT supported for Vanilla HopsFS.");
                break;
            case OP_WRITE_FILES_TO_DIR:
                LOG.info("WRITE FILES TO DIRECTORY selected!");
                Commands.writeFilesToDirectory(hdfsConfiguration, nameNodeEndpoint);
                break;
            case OP_READ_FILES:
                LOG.info("READ FILES selected!");
                Commands.readFilesOperation(hdfsConfiguration, nameNodeEndpoint, OP_READ_FILES);
                break;
            case OP_DELETE_FILES:
                LOG.info("DELETE FILES selected!");
                Commands.deleteFilesOperation(hdfs, nameNodeEndpoint);
                break;
            case OP_WRITE_FILES_TO_DIRS:
                LOG.info("WRITE FILES TO DIRECTORIES selected!");
                Commands.writeFilesToDirectories(hdfsConfiguration, nameNodeEndpoint);
                break;
            case OP_WEAK_SCALING_READS:
                LOG.info("'Read n Files with n Threads (Weak Scaling - Read)' selected!");
                DistributedBenchmarkResult result = Commands.weakScalingReadsV1(hdfsConfiguration, nameNodeEndpoint,
                        message.getAsJsonPrimitive("n").getAsInt(),
                        message.getAsJsonPrimitive("readsPerFile").getAsInt(),
                        message.getAsJsonPrimitive("inputPath").getAsString(),
                        message.getAsJsonPrimitive("shuffle").getAsBoolean(),
                        OP_WEAK_SCALING_READS);
                result.setOperationId(operationId);
                LOG.info("Obtained local result for WEAK SCALING (READ) benchmark: " + result);
                sendResultToLeader(result);
                break;
            case OP_STRONG_SCALING_READS:
                LOG.info("'Read n Files y Times with z Threads (Strong Scaling - Read)' selected!");
                result = Commands.strongScalingBenchmark(hdfsConfiguration, nameNodeEndpoint,
                        message.getAsJsonPrimitive("n").getAsInt(),
                        message.getAsJsonPrimitive("readsPerFile").getAsInt(),
                        message.getAsJsonPrimitive("numThreads").getAsInt(),
                        message.getAsJsonPrimitive("inputPath").getAsString());
                result.setOperationId(operationId);
                LOG.info("Obtained local result for STRONG SCALING (READ) benchmark: " + result);
                sendResultToLeader(result);
                break;
            case OP_WEAK_SCALING_WRITES:
                LOG.info("'Write n Files with n Threads (Weak Scaling - Write)' selected!");
                JsonArray directoriesJson = message.getAsJsonPrimitive("directories").getAsJsonArray();
                List<String> directories = new ArrayList<>();
                for (JsonElement elem : directoriesJson) {
                    directories.add(elem.getAsString());
                }
                result = Commands.writeFilesInternal(
                        message.getAsJsonPrimitive("n").getAsInt(),
                        message.getAsJsonPrimitive("numThreads").getAsInt(),
                        directories, OP_WEAK_SCALING_WRITES, hdfsConfiguration, nameNodeEndpoint,
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
                        directories, OP_STRONG_SCALING_WRITES, hdfsConfiguration,
                        nameNodeEndpoint, false);
                result.setOperationId(operationId);
                LOG.info("Obtained local result for STRONG SCALING (WRITE) benchmark: " + result);
                sendResultToLeader(result);
                break;
            case OP_WEAK_SCALING_READS_V2:
                LOG.info("OP_WEAK_SCALING_READS_V2 selected!");
                result = Commands.weakScalingBenchmarkV2(hdfsConfiguration, nameNodeEndpoint,
                        message.getAsJsonPrimitive("numThreads").getAsInt(),
                        message.getAsJsonPrimitive("filesPerThread").getAsInt(),
                        message.getAsJsonPrimitive("inputPath").getAsString(),
                        message.getAsJsonPrimitive("shuffle").getAsBoolean(), OP_WEAK_SCALING_READS_V2);
                result.setOperationId(operationId);
                LOG.info("Obtained local result for OP_WEAK_SCALING_READS_V2 benchmark: " + result);
                sendResultToLeader(result);
                break;
            case OP_GET_FILE_STATUS:
                LOG.info("OP_GET_FILE_STATUS selected!");
                result = Commands.getFileStatusOperation(hdfsConfiguration, nameNodeEndpoint);
                result.setOperationId(operationId);
                LOG.info("Obtained local result for OP_GET_FILE_DIR_INFO benchmark: " + result);
                sendResultToLeader(result);
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

    private void sendMessageToLeader(JsonObject payload) {
        assert payload != null;
        LOG.debug("Sending message to Leader now...");
        int bytesSent = this.client.sendTCP(new Gson().toJson(payload));
        LOG.debug("Successfully sent " + bytesSent + " byte(s) to leader.");
    }

    private void handleRegistration(JsonObject message) {
        LOG.debug("Received REGISTRATION message from Leader.");
        nameNodeEndpoint = message.getAsJsonPrimitive(NAMENODE_ENDPOINT).getAsString();
        hdfsConfigFilePath = message.getAsJsonPrimitive(HDFS_CONFIG_PATH).getAsString();

        // The initDfsClient() function in the Commander file uses the Commander's static 'hdfsConfigFilePath'
        // variable. This is basically a hack, pretty gross.
        Commander.hdfsConfigFilePath = hdfsConfigFilePath;
        LOG.debug("NameNode Endpoint: " + nameNodeEndpoint);
        LOG.debug("hdfsConfigFilePath: " + hdfsConfigFilePath);

        hdfs = initDfsClient();
    }

    public void connect() {
        Network.register(client);

        Thread connectThread = new Thread(() -> {
            client.start();

            LOG.debug("Trying to connect to Commander at " + masterIp + ":" + masterPort + ".");

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
