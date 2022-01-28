package com.gmail.benrcarver.distributed;

import com.esotericsoftware.kryonet.Client;
import com.esotericsoftware.kryonet.Connection;
import com.esotericsoftware.kryonet.Listener;
import com.gmail.benrcarver.distributed.util.Utils;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import io.hops.metrics.TransactionEvent;
import io.hops.metrics.TransactionAttempt;
import io.hops.transaction.context.TransactionsStats;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.fs.FileStatus;
import io.hops.metrics.OperationPerformed;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;

import static com.gmail.benrcarver.distributed.Constants.*;

/**
 * Runs HopsFS benchmarks as directed by a {@link Commander}.
 */
public class Follower {
    public static final Log LOG = LogFactory.getLog(Follower.class);

    private static final int CONN_TIMEOUT_MILLISECONDS = 5000;

    private String nameNodeEndpoint;

    private final Client client;

    private final String masterIp;
    private final int masterPort;

    private String hdfsConfigFilePath;
    private Configuration hdfsConfiguration;
    private DistributedFileSystem hdfs;

    public synchronized void waitUntilDone() throws InterruptedException {
        this.wait();
    }

    public Follower(String masterIp, int masterPort) {
        client = new Client(32000, 32000);
        this.masterIp = masterIp;
        this.masterPort = masterPort;

        client.addListener(new Listener() {
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
                    }
                }
            }
        });
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

    private void handleMessageFromLeader(JsonObject message, DistributedFileSystem hdfs) throws IOException, InterruptedException {
        int operation = message.getAsJsonPrimitive(OPERATION).getAsInt();

        String operationId = "N/A";
        if (message.has(OPERATION_ID)) {
            operationId = message.getAsJsonPrimitive(OPERATION_ID).getAsString();
            LOG.info("Received operation " + operationId + " from Leader.");
        }

        switch(operation) {
            case OP_REGISTRATION:
                handleRegistration(message);
                break;
            case OP_CLEAR_STATISTICS:
                LOG.info("Clearing statistics packages...");
                Commands.clearStatisticsPackages(hdfs);
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
                LOG.info("PING selected!");
                Commands.pingOperation(hdfs);
                break;
            case OP_PREWARM:
                LOG.info("PREWARM selected!");
                Commands.prewarmOperation(hdfs);
                break;
            case OP_WRITE_FILES_TO_DIR:
                LOG.info("WRITE FILES TO DIRECTORY selected!");
                Commands.writeFilesToDirectory(hdfs, hdfsConfiguration, nameNodeEndpoint);
                break;
            case OP_READ_FILES:
                LOG.info("READ FILES selected!");
                Commands.readFilesOperation(hdfsConfiguration, hdfs, nameNodeEndpoint);
                break;
            case OP_DELETE_FILES:
                LOG.info("DELETE FILES selected!");
                Commands.deleteFilesOperation(hdfs, nameNodeEndpoint);
                break;
            case OP_WRITE_FILS_TO_DIRS:
                LOG.info("WRITE FILES TO DIRECTORIES selected!");
                Commands.writeFilesToDirectories(hdfs, hdfsConfiguration, nameNodeEndpoint);
                break;
            case OP_WEAK_SCALING:
                LOG.info("'Read n Files with n Threads (Weak Scaling)' selected!");
                DistributedBenchmarkResult result = Commands.readNFiles(hdfsConfiguration,
                        hdfs, nameNodeEndpoint,
                        message.getAsJsonPrimitive("n").getAsInt(),
                        message.getAsJsonPrimitive("readsPerFile").getAsInt(),
                        message.getAsJsonPrimitive("inputPath").getAsString());
                result.setOperationId(operationId);
                LOG.info("Obtained local result for WEAK SCALING benchmark: " + result);
                sendResultToLeader(result);
                break;
            case OP_STRONG_SCALING:
                LOG.info("'Read n Files y Times with z Threads (Strong Scaling)' selected!");
                result = Commands.strongScalingBenchmark(hdfsConfiguration,
                        hdfs, nameNodeEndpoint,
                        message.getAsJsonPrimitive("n").getAsInt(),
                        message.getAsJsonPrimitive("readsPerFile").getAsInt(),
                        message.getAsJsonPrimitive("numThreads").getAsInt(),
                        message.getAsJsonPrimitive("inputPath").getAsString());
                result.setOperationId(operationId);
                LOG.info("Obtained local result for STRONG SCALING benchmark: " + result);
                sendResultToLeader(result);
                break;
            default:
                LOG.info("ERROR: Unknown or invalid operation specified: " + operation);
                break;
        }
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
        LOG.debug("NameNode Endpoint: " + nameNodeEndpoint);

        hdfs = initDfsClient();
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
    }
}