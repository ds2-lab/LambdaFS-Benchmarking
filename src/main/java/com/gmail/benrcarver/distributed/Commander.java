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
import net.schmizz.sshj.SSHClient;
import net.schmizz.sshj.common.IOUtils;
import net.schmizz.sshj.connection.channel.direct.Session;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import io.hops.metrics.TransactionEvent;
import io.hops.metrics.TransactionAttempt;
import io.hops.transaction.context.TransactionsStats;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
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
import java.io.*;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static com.gmail.benrcarver.distributed.Constants.*;

/**
 * Controls a fleet of distributed machines. Executes HopsFS benchmarks based on user input/commands.
 */
public class Commander {
    public static final Log LOG = LogFactory.getLog(Commander.class);
    private static final Console con = System.console();

    private List<Connection> followers;

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
    public static String hdfsConfigFilePath;

    /**
     * The hdfs-site.xml configuration file.
     */
    private Configuration hdfsConfiguration;

    /**
     * Map from follower IP to the associated SSH client.
     */
    private HashMap<String, SSHClient> sshClients;

    /**
     * Map from operation ID to the queue in which distributed results should be placed by the TCP server.
     */
    private ConcurrentHashMap<String, BlockingQueue<DistributedBenchmarkResult>> resultQueues;

    private final boolean nondistributed;

    private static String serverlessLogLevel = "DEBUG";
    private static boolean consistencyEnabled = true;

    private static Commander instanace;

    public static Commander getOrCreateCommander(String ip, int port, String yamlPath, boolean nondistributed,
                                          String logLevel, boolean disableConsistency) throws IOException {
        if (instanace == null) {
            serverlessLogLevel = logLevel;
            consistencyEnabled = !disableConsistency;
            instanace = new Commander(ip, port, yamlPath, nondistributed);
        }

        return instanace;
    }

    private Commander(String ip, int port, String yamlPath, boolean nondistributed) throws IOException {
        this.ip = ip;
        this.port = port;
        this.nondistributed = nondistributed;
        // TODO: Maybe do book-keeping or fault-tolerance here.
        this.followers = new ArrayList<>();
        this.resultQueues = new ConcurrentHashMap<>();

        tcpServer = new Server(32000, 32000) {
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

            LOG.info("Loaded configuration: " + config.toString());

            nameNodeEndpoint = config.getNamenodeEndpoint();
            followerConfigs = config.getFollowers();
            hdfsConfigFilePath = config.getHdfsConfigFile();

            LOG.info("Loaded configuration!");
            LOG.info(config);
        }
    }

    public void start() throws IOException, InterruptedException {
        if (!nondistributed) {
            LOG.info("Commander is operating in DISTRIBUTED mode.");
            startServer();
            launchFollowers();
        }
        LOG.info("Commander is operating in NON-DISTRIBUTED mode.");
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

    private void interactiveLoop() throws InterruptedException, IOException {
        LOG.info("Beginning execution as LEADER now.");

        DistributedFileSystem hdfs = initDfsClient(nameNodeEndpoint);

        while (true) {
            Thread.sleep(250);
            printMenu();
            int op = getNextOperation();

            switch(op) {
                case OP_SET_CONSISTENCY_PROTOCOL_ENABLED:
                    handleSetConsistencyProtocolEnabled(hdfs);
                    break;
                case OP_SET_LOG_LEVEL:
                    handleSetLogLevel(hdfs);
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
                    LOG.info("Exiting now... goodbye!");
                    try {
                        hdfs.close();
                    } catch (IOException ex) {
                        LOG.info("Encountered exception while closing file system...");
                        ex.printStackTrace();
                    }
                    stopServer();
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
                case OP_WRITE_FILES_TO_DIRS:
                    LOG.info("WRITE FILES TO DIRECTORIES selected!");
                    Commands.writeFilesToDirectories(hdfs, hdfsConfiguration, nameNodeEndpoint);
                    break;
                case OP_WEAK_SCALING_READS:
                    LOG.info("'Read n Files with n Threads (Weak Scaling - Read)' selected!");
                    weakScalingReadOperation(hdfsConfiguration, hdfs, nameNodeEndpoint);
                    break;
                case OP_STRONG_SCALING_READS:
                    LOG.info("'Read n Files y Times with z Threads (Strong Scaling - Read)' selected!");
                    strongScalingReadOperation(hdfsConfiguration, hdfs, nameNodeEndpoint);
                    break;
                case OP_WEAK_SCALING_WRITES:
                    LOG.info("'Write n Files with n Threads (Weak Scaling - Write)' selected!");
                    weakScalingWriteOperation(hdfsConfiguration, hdfs, nameNodeEndpoint);
                    break;
                case OP_STRONG_SCALING_WRITES:
                    LOG.info("'Write n Files y Times with z Threads (Strong Scaling - Write)' selected!");
                    throw new NotImplementedException("Not yet implemented.");
                case OP_CREATE_DIRECTORIES:
                    LOG.info("CREATE DIRECTORIES selected!");
                    Commands.createDirectories(hdfs, nameNodeEndpoint);
                    break;
                default:
                    LOG.info("ERROR: Unknown or invalid operation specified: " + op);
                    break;
            }
        }
    }

    private void handleSetLogLevel(DistributedFileSystem hdfs) {
        String currentLogLevel = hdfs.getServerlessFunctionLogLevel();
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
            hdfs.setServerlessFunctionLogLevel(newLogLevel);
        }
    }

    private void handleSetConsistencyProtocolEnabled(DistributedFileSystem hdfs) {
        boolean currentFlag = hdfs.getConsistencyProtocolEnabled();
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

            hdfs.setConsistencyProtocolEnabled(true);
            consistencyEnabled = true;
        }
        else if (newFlag.equalsIgnoreCase("f") || newFlag.equalsIgnoreCase("n")) {
            if (currentFlag)
                LOG.info("DISABLING consistency protocol.");
            else
                LOG.info("Consistency protocol is already disabled.");

            hdfs.setConsistencyProtocolEnabled(false);
            consistencyEnabled = false;
        }
    }

    /**
     * Issue a command to all our followers.
     * @param opName The name of the command.
     * @param operationId Unique ID of this operation.
     * @param payload Contains the command and necessary arguments.
     */
    private void issueCommandToFollowers(String opName, String operationId, JsonObject payload) {
        LOG.debug("Issuing '" + opName + "' (id=" + operationId + ") command to " +
                followers.size() + " follower(s).");

        BlockingQueue<DistributedBenchmarkResult> resultQueue = new
                ArrayBlockingQueue<>(followers.size());
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

        return input.equalsIgnoreCase("y") || input.equalsIgnoreCase("yes") ||
                input.equalsIgnoreCase("t") || input.equalsIgnoreCase("true") ||
                input.equalsIgnoreCase("1");
    }

    private int getIntFromUser(String prompt) {
        System.out.print(prompt + "\n> ");
        return Integer.parseInt(scanner.nextLine());
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
                        sharedHdfs, hdfsConfiguration, nameNodeEndpoint);
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
        int n = getIntFromUser("How many files should be read by each thread?");

        int readsPerFile = getIntFromUser("How many times should each file be read?");

        int numThreads = getIntFromUser("Number of threads");

        System.out.print("Please provide a path to a local file containing at least " + n + " HopsFS file " +
                (n == 1 ? "path.\n> " : "paths.\n> "));
        String inputPath = scanner.nextLine();

        String operationId = UUID.randomUUID().toString();
        int numDistributedResults = followers.size();
        if (followers.size() > 0) {
            JsonObject payload = new JsonObject();
            payload.addProperty(OPERATION, OP_STRONG_SCALING_READS);
            payload.addProperty(OPERATION_ID, operationId);
            payload.addProperty("n", n);
            payload.addProperty("readsPerFile", readsPerFile);
            payload.addProperty("numThreads", numThreads);
            payload.addProperty("inputPath", inputPath);

            issueCommandToFollowers("Read n Files y Times with z Threads (Strong Scaling)", operationId, payload);
        }

        DistributedBenchmarkResult localResult =
                Commands.strongScalingBenchmark(configuration, sharedHdfs, nameNodeEndpoint, n, readsPerFile,
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
        System.out.print("Should the threads write their files to the SAME DIRECTORY [1] or DIFFERENT DIRECTORIES [2]?\n> ");
        int directoryChoice = Integer.parseInt(scanner.nextLine());

        // Validate input.
        if (directoryChoice < 1 || directoryChoice > 2) {
            LOG.error("Invalid argument specified. Should be \"1\" for same directory or \"2\" for different directories. " +
                    "Instead, got \"" + directoryChoice + "\"");
            return;
        }

        System.out.print("Manually input (comma-separated list) [1], or specify file containing directories [2]? \n> ");
        int dirInputMethodChoice = Integer.parseInt(scanner.nextLine());

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
        } else {
            Collections.shuffle(directories);
            directories = directories.subList(0, numberOfThreads);
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

            JsonArray directoriesJson = new JsonArray();
            for (String dir : directories)
                directoriesJson.add(dir);

            payload.add("directories", directoriesJson);

            issueCommandToFollowers("Write n Files with n Threads (Weak Scaling - Write)", operationId, payload);
        }

        LOG.info("Each thread should be writing " + writesPerThread + " files...");

        DistributedBenchmarkResult localResult =
                Commands.writeFilesInternal(writesPerThread, minLength, maxLength, numberOfThreads, directories,
                        sharedHdfs, hdfsConfiguration, nameNodeEndpoint);
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
    private double waitForDistributedResult(
            int numDistributedResults,
            String operationId,
            DistributedBenchmarkResult localResult) throws InterruptedException {
        if (numDistributedResults < 1) {
            // LOG.warn("The number of distributed results is 1. We have nothing to wait for.");
            return localResult.getOpsPerSecond();
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

        opsPerformed.addValue(localResult.numOpsPerformed);
        duration.addValue(localResult.durationSeconds);
        throughput.addValue(localResult.getOpsPerSecond());

        for (DistributedBenchmarkResult res : resultQueue) {
            LOG.debug("Received result: " + res);

            opsPerformed.addValue(res.numOpsPerformed);
            duration.addValue(res.durationSeconds);
            throughput.addValue(res.getOpsPerSecond());
        }

        double aggregateThroughput = (opsPerformed.getSum() / duration.getMean());

        LOG.info("==== RESULTS ====");
        LOG.info("Average Duration: " + duration.getMean() * 1000.0 + " ms.");
        LOG.info("Aggregate Throughput (ops/sec): " + aggregateThroughput);
        LOG.info("Average Non-Aggregate Throughput (op/sec): " + throughput.getMean());

        return aggregateThroughput;
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
        Double[] results = new Double[numTrials];
        while (currentTrial < numTrials) {
            LOG.info("|====| TRIAL #%" + currentTrial + " |====|");
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
                    Commands.readNFiles(configuration, sharedHdfs, nameNodeEndpoint, n, readsPerFile, inputPath, shuffle);

            if (localResult == null) {
                LOG.warn("Local result is null. Aborting.");
                return;
            }

            //LOG.info("LOCAL result of weak scaling benchmark: " + localResult);
            localResult.setOperationId(operationId);

            double throughput = 0.0;
            // Wait for followers' results if we had followers when we first started the operation.
            if (numDistributedResults > 0) {
                throughput = waitForDistributedResult(numDistributedResults, operationId, localResult);
            } else {
                throughput = localResult.getOpsPerSecond();
            }

            results[currentTrial] = throughput;
            currentTrial++;
        }

        for (double throughputResult : results) {
            System.out.println(throughputResult);
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

    public static DistributedFileSystem initDfsClient(String nameNodeEndpoint) {
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

        hdfs.setConsistencyProtocolEnabled(consistencyEnabled);
        hdfs.setServerlessFunctionLogLevel(serverlessLogLevel);

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
        }
    }

    private static void printMenu() {
        System.out.println("");
        System.out.println("====== MENU ======");
        System.out.println("Debug Operations:");
        System.out.println(
                "(-6) Get/set consistency protocol enabled flag.\n(-5) Get/set serverless log4j debug level.\n" +
                "(-4) Clear statistics\n(-3) Output statistics packages to CSV\n" +
                "(-2) Output operations performed + write to file\n(-1) Print TCP debug information.");
        System.out.println("\nStandard Operations:");
        System.out.println("(0) Exit\n(1) Create file\n(2) Create directory\n(3) Read contents of file.\n(4) Rename" +
                "\n(5) Delete\n(6) List directory\n(7) Append\n(8) Create Subtree.\n(9) Ping\n(10) Prewarm" +
                "\n(11) Write Files to Directory\n(12) Read files\n(13) Delete files\n(14) Write Files to Directories" +
                "\n(15) Read n Files with n Threads (Weak Scaling - Read)\n(16) Read n Files y Times with z Threads (Strong Scaling - Read)" +
                "\n(17) Write n Files with n Threads (Weak Scaling - Write)\n(18) Write n Files y Times with z Threads (Strong Scaling - Write)" +
                "\n(19) Create directories.");
        System.out.println("==================");
        System.out.println("");
        System.out.println("What would you like to do?");
        System.out.print("> ");
    }
}
