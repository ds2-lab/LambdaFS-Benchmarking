package com.gmail.benrcarver.distributed;

import com.esotericsoftware.kryonet.Connection;
import com.esotericsoftware.kryonet.Listener;
import com.esotericsoftware.kryonet.Server;
import com.gmail.benrcarver.distributed.util.Utils;
import com.gmail.benrcarver.distributed.util.TreeNode;
import net.schmizz.sshj.SSHClient;
import net.schmizz.sshj.common.IOUtils;
import net.schmizz.sshj.connection.channel.direct.Session;
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
import org.yaml.snakeyaml.Yaml;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.ArrayBlockingQueue;
import java.io.*;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Controls a fleet of distributed machines. Executes HopsFS benchmarks based on user input/commands.
 */
public class Commander {
    public static final Log LOG = LogFactory.getLog(Commander.class);
    private static final Console con = System.console();

    private static final String LEADER_PREFIX = "[LEADER TCP SERVER]";

    /**
     * Use with String.format(LAUNCH_FOLLOWER_CMD, leader_ip, leader_port)
     */
    private static final String LAUNCH_FOLLOWER_CMD = "source ~/.bashrc & java -cp \".:target/HopsFSBenchmark-1.0-jar-with-dependencies.jar:/home/ubuntu/repos/hops/hadoop-dist/target/hadoop-3.2.0.3-SNAPSHOT/share/hadoop/hdfs/lib/*:/home/ubuntu/repos/hops/hadoop-dist/target/hadoop-3.2.0-SNAPSHOT/share/hadoop/common/lib/*:/home/ubuntu/repos/hops/hadoop-hdfs-project/hadoop-hdfs-client/target/hadoop-hdfs-client-3.2.0.3-SNAPSHOT.jar:/home/ubuntu/repos/hops/hops-leader-election/target/hops-leader-election-3.2.0.3-SNAPSHOT.jar:/home/ben/openwhisk-runtime-java/core/java8/libs/*:/home/ubuntu/repos/hops/hadoop-hdfs-project/hadoop-hdfs/target/hadoop-hdfs-3.2.0.3-SNAPSHOT.jar:/home/ubuntu/repos/hops/hadoop-common-project/hadoop-common/target/hadoop-common-3.2.0.3-SNAPSHOT.jar\" com.gmail.benrcarver.distributed.InteractiveTest --worker --leader_ip %s --leader_port %d";

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
    private final List<FollowerConfig> followerConfigs;

    /**
     * Fully-qualified path of hdfs-site.xml configuration file.
     */
    private final String hdfsConfigFilePath;

    /**
     * The hdfs-site.xml configuration file.
     */
    private Configuration hdfsConfiguration;

    /**
     * Map from follower IP to the associated SSH client.
     */
    private HashMap<String, SSHClient> sshClients;

    public Commander(String ip, int port, String yamlPath) throws IOException {
        this.ip = ip;
        this.port = port;

        tcpServer = new Server() {
            @Override
            protected Connection newConnection() {
                LOG.debug(LEADER_PREFIX + " Creating new FollowerConnection.");
                return new FollowerConnection();
            }
        };

        tcpServer.addListener(new ServerListener());

        Yaml yaml = new Yaml();
        try (InputStream in = Files.newInputStream(Paths.get(yamlPath))) {
            LocalConfiguration config = yaml.loadAs(in, LocalConfiguration.class);

            nameNodeEndpoint = config.getNamenodeEndpoint();
            followerConfigs = config.getFollowers();
            hdfsConfigFilePath = config.getHdfsConfigFile();

            LOG.info("Loaded configuration!");
            LOG.info(config);
        }
    }

    public void start() throws IOException, InterruptedException {
        startServer();
        launchFollowers();
        interactiveLoop();
    }

    /**
     * Using SSH, launch the follower processes.
     */
    private void launchFollowers() throws IOException {
        final String fullCommand = String.format(LAUNCH_FOLLOWER_CMD, ip, port);

        for (FollowerConfig config : followerConfigs) {
            LOG.info("Starting follower at " + config.getUser() + "@" + config.getIp() + " now.");

            SSHClient ssh = new SSHClient();
            ssh.loadKnownHosts();
            ssh.connect(config.getIp());

            LOG.debug("Connected to follower at " + config.getUser() + "@" + config.getIp() + " now.");

            Session session = null;

            try {
                ssh.authPublickey(config.getUser());

                LOG.debug("Authenticated with follower at " + config.getUser() + "@" + config.getIp() + " now.");

                session = ssh.startSession();

                LOG.debug("Started session with follower at " + config.getUser() + "@" + config.getIp() + " now.");

                Session.Command cmd = session.exec(fullCommand);

                LOG.debug("Executed command: " + fullCommand);

                ByteArrayOutputStream inputStream = IOUtils.readFully(cmd.getInputStream());
                LOG.debug("Output: " + inputStream);

                con.writer().print(inputStream);
                cmd.join(5, TimeUnit.SECONDS);
                con.writer().print("\n** exit status: " + cmd.getExitStatus());
                LOG.debug("Exit status: " + cmd.getExitStatus());
            } finally {
                if (session != null)
                    session.close();

                ssh.disconnect();
            }
        }
    }

    private void startServer() throws IOException {
        tcpServer.start();
        tcpServer.bind(port);
    }

    /**
     * Stop the TCP server. Also sends 'STOP' commands to all the followers.
     */
    private void stopServer() {
        // TODO: Send 'STOP' commands to each follower.
        tcpServer.stop();
    }

    private void interactiveLoop() throws InterruptedException, IOException {
        LOG.info("Beginning execution as LEADER now.");

        DistributedFileSystem hdfs = initDfsClient();

        while (true) {
            Thread.sleep(250);
            printMenu();
            int op = getNextOperation();

            switch(op) {
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
                    stopServer();
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
                    Commands.writeFilesToDirectories(hdfs, hdfsConfiguration);
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
                    LOG.info("ERROR: Unknown or invalid operation specified: " + op);
                    break;
            }
        }
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

    private static class ServerListener extends Listener {
        /**
         * Listener handles connection establishment with remote NameNodes.
         */
        public void connected(Connection conn) {
            LOG.debug(LEADER_PREFIX + " Connection established with remote NameNode at "
                    + conn.getRemoteAddressTCP());
            conn.setKeepAliveTCP(6000);
            conn.setTimeout(12000);
        }
    }

    private static void printMenu() {
        System.out.println("");
        System.out.println("====== MENU ======");
        System.out.println("Debug Operations:");
        System.out.println("\n(-4) Clear statistics\n(-3) Output statistics packages to CSV\n" +
                "(-2) Output operations performed + write to file\n(-1) Print TCP debug information.");
        System.out.println("\nStandard Operations:");
        System.out.println("(0) Exit\n(1) Create file\n(2) Create directory\n(3) Read contents of file.\n(4) Rename" +
                "\n(5) Delete\n(6) List directory\n(7) Append\n(8) Create Subtree.\n(9) Ping\n(10) Prewarm" +
                "\n(11) Write Files to Directory\n(12) Read files\n(13) Delete files\n(14) Write Files to Directories" +
                "\n(15) Read n Files with n Threads (Weak Scaling)\n(16) Read n Files y Times with z Threads (Strong Scaling)");
        System.out.println("==================");
        System.out.println("");
        System.out.println("What would you like to do?");
        System.out.print("> ");
    }
}
