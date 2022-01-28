package com.gmail.benrcarver.distributed;

import com.esotericsoftware.kryonet.Client;
import com.esotericsoftware.kryonet.Connection;
import com.esotericsoftware.kryonet.Listener;
import com.gmail.benrcarver.distributed.util.Utils;
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

    public Follower(String masterIp, int masterPort) {
        client = new Client();
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

        switch (operation) {
            case OP_REGISTRATION:
                handleRegistration(message);
                break;
            case -4:
                LOG.info("Clearing statistics packages...");
                Commands.clearStatisticsPackages(hdfs);
                break;
            case -3:
                LOG.info("Writing statistics packages to files...");
                LOG.info("");
                hdfs.dumpStatisticsPackages(true);
                break;
            case -2:
                LOG.info("Printing operations performed...");
                LOG.info("");
                Commands.printOperationsPerformed(hdfs);
                break;
            case -1:
                LOG.info("Printing TCP debug information...");
                LOG.info("");
                hdfs.printDebugInformation();
                break;
            case 0:
                LOG.info("Exiting now... goodbye!");
                try {
                    hdfs.close();
                } catch (IOException ex) {
                    LOG.info("Encountered exception while closing file system...");
                    ex.printStackTrace();
                }
                client.stop();
                System.exit(0);
            case 1:
                LOG.info("CREATE FILE selected!");
                Commands.createFileOperation(hdfs, nameNodeEndpoint);
                break;
            case 2:
                LOG.info("MAKE DIRECTORY selected!");
                Commands.mkdirOperation(hdfs, nameNodeEndpoint);;
                break;
            case 3:
                LOG.info("READ FILE selected!");
                Commands.readOperation(hdfs, nameNodeEndpoint);
                break;
            case 4:
                LOG.info("RENAME selected!");
                Commands.renameOperation(hdfs, nameNodeEndpoint);
                break;
            case 5:
                LOG.info("DELETE selected!");
                Commands.deleteOperation(hdfs, nameNodeEndpoint);
                break;
            case 6:
                LOG.info("LIST selected!");
                Commands.listOperation(hdfs, nameNodeEndpoint);
                break;
            case 7:
                LOG.info("APPEND selected!");
                Commands.appendOperation(hdfs, nameNodeEndpoint);
                break;
            case 8:
                LOG.info("CREATE SUBTREE selected!");
                Commands.createSubtree(hdfs, nameNodeEndpoint);
                break;
            case 9:
                LOG.info("PING selected!");
                Commands.pingOperation(hdfs);
                break;
            case 10:
                LOG.info("PREWARM selected!");
                Commands.prewarmOperation(hdfs);
                break;
            case 11:
                LOG.info("WRITE FILES TO DIRECTORY selected!");
                Commands.writeFilesToDirectory(hdfs, hdfsConfiguration, nameNodeEndpoint);
                break;
            case 12:
                LOG.info("READ FILES selected!");
                Commands.readFilesOperation(hdfsConfiguration, hdfs, nameNodeEndpoint);
                break;
            case 13:
                LOG.info("DELETE FILES selected!");
                Commands.deleteFilesOperation(hdfs, nameNodeEndpoint);
                break;
            case 14:
                LOG.info("WRITE FILES TO DIRECTORIES selected!");
                Commands.writeFilesToDirectories(hdfs, hdfsConfiguration, nameNodeEndpoint);
                break;
            case 15:
                LOG.info("'Read n Files with n Threads (Weak Scaling)' selected!");
                Commands.readNFilesOperation(hdfsConfiguration, hdfs, nameNodeEndpoint);
                break;
            case 16:
                LOG.info("'Read n Files y Times with z Threads (Strong Scaling)' selected!");
                Commands.strongScalingBenchmark(hdfsConfiguration, hdfs, nameNodeEndpoint);
                break;
            default:
                LOG.warn("Received unknown operation from Leader: '" + operation + "'");
        }
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
                client.connect(CONN_TIMEOUT_MILLISECONDS, masterIp, masterPort);
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
