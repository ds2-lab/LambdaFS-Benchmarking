package hopsfs;

import com.google.gson.JsonObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import sun.tools.jar.CommandLine;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

public class HopsFSTest {

    private static final String DATA_SOURCE = "DATA_SOURCE";
    private static final String FROM_CACHE = "FROM_CACHE";
    private static final String FROM_NDB = "FROM_NDB";
    private static final String RESULT = "RESULT";
    private static final String NUM_QUERIES = "NUM_QUERIES";
    private static final String CONNECTION_URL = "CONNECTION_URL";
    private static final String OPERATION_TYPE = "OPERATION_TYPE";
    private static final String QUERY = "QUERY";
    private static final String CLUSTERJ = "CLUSTERJ";
    private static final String ID = "ID";
    private static final String FIRST_NAME = "FIRST_NAME";
    private static final String LAST_NAME = "LAST_NAME";
    private static final String POSITION = "POSITION";
    private static final String DEPARTMENT = "DEPARTMENT";
    private static final String CONFIG_PATH = "ndb-config.properties";
    private static final String DEFAULT_DATA_SOURCE = "FROM_NDB";

    public static void main(String[] args) {
        Options options = new Options();

        Option connectionUrlOption = new Option("url", CONNECTION_URL, true, "The connection URL for a query.");
        connectionUrlOption.setRequired(false);

        Option queryOperationOption = new Option("q", QUERY, true, "Perform a query operation.");
        queryOperationOption.setRequired(false);

        Option dataSourceOpt = new Option("src", DATA_SOURCE, true, "The source from which the requested data should be retrieved.");
        dataSourceOpt.setRequired(false);

        Option idOpt = new Option("id", ID, true, "The ID of the desired user.");
        idOpt.setRequired(false);

        Option firstNameOpt = new Option("fn", FIRST_NAME, true, "The first name of the desired user.");
        Option lastNameOpt = new Option("ln", LAST_NAME, true, "The last name of the desired user.");

        Option numQueriesOpt = new Option("nq", "NUM_QUERIES", true, "The number of queries to make.");

        options.addOption(connectionUrlOption);
        options.addOption(queryOperationOption);
        options.addOption(dataSourceOpt);
        options.addOption(idOpt);
        options.addOption(firstNameOpt);
        options.addOption(lastNameOpt);
        options.addOption(numQueriesOpt);

        CommandLineParser parser = new GnuParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd = null;

        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            formatter.printHelp("utility-name", options);

            System.exit(1);
        }

        String connectionUrl = "jdbc:mysql://10.150.0.11:3306/world";
        String dataSource = "FROM_NDB";
        String query = "SELECT * FROM users WHERE ID = 1";
        int id = 1;
        int numQueries = 1;

        if (cmd.hasOption(CONNECTION_URL))
            connectionUrl = cmd.getOptionValue(CONNECTION_URL);

        if (cmd.hasOption(DATA_SOURCE))
            dataSource = cmd.getOptionValue(DATA_SOURCE);

        if (cmd.hasOption(QUERY))
            query = cmd.getOptionValue(QUERY);

        if (cmd.hasOption(ID))
            id = Integer.parseInt(cmd.getOptionValue(ID));

        if (cmd.hasOption("NUM_QUERIES"))
            numQueries = Integer.parseInt(cmd.getOptionValue("NUM_QUERIES"));

        System.out.println("Passing the following arguments to the latency benchmark:");
        System.out.println("connection url = " + connectionUrl);
        System.out.println("data source = " + dataSource);
        System.out.println("query = " + query);
        System.out.println("id = " + id);

        testLatencyBenchmark(connectionUrl, dataSource, query, id, numQueries);
    }

    private static void testLatencyBenchmark(String connectionUrl, String dataSource, String query, int id, int numQueries) {
        System.out.println("Starting HdfsTest now.");
        Configuration configuration = new Configuration();
        System.out.println("Created configuration.");
        DistributedFileSystem dfs = new DistributedFileSystem();
        System.out.println("Created DistributedFileSystem object.");

        try {
            dfs.initialize(new URI("hdfs://10.150.0.6:9000"), configuration);
            System.out.println("Called initialize() successfully.");
        } catch (URISyntaxException | IOException ex) {
            ex.printStackTrace();
        }

        System.out.println("Attempting to call Latency Benchmark now...");

        ArrayList<Double> results = new ArrayList<Double>();

        try {
            for (int i = 0; i < numQueries; i++) {
                long startTime = System.nanoTime();
                JsonObject result = dfs.latencyBenchmark(connectionUrl, dataSource, query, id);
                long endTime = System.nanoTime();
                long durationInNano = (endTime - startTime);
                double durationInMillis = (double)TimeUnit.NANOSECONDS.toMillis(durationInNano);
                results.add(durationInMillis / 1000);
                System.out.println("Finished latency benchmark #" + i + " in " + (durationInMillis / 1000) + " seconds.");
                System.out.println(result);
            }
        } catch (SQLException | IOException ex) {
            System.out.println("\n[ERROR] Encountered exception while attempting latency benchmark...");
            ex.printStackTrace();
        }

        double sum = results.stream().mapToDouble(Double::doubleValue).sum();
        double average = (double)sum / results.size();
        double max = Collections.max(results);
        double min = Collections.min(results);
        System.out.println("Average/Min/Max/All:\n" + average + "\n" + min + "\n" + max + "\n" + results.toString());
    }

    private static void testWriteFile() {
        System.out.println("Starting HdfsTest now.");
        Configuration configuration = new Configuration();
        System.out.println("Created configuration.");
        DistributedFileSystem hdfs = new DistributedFileSystem();
        System.out.println("Created DistributedFileSystem object.");

        try {
            hdfs.initialize(new URI("hdfs://10.150.0.6:9000"), configuration);
            System.out.println("Called initialize() successfully.");
        } catch (URISyntaxException | IOException ex) {
            ex.printStackTrace();
        }

        Path filePath = new Path("hdfs://10.150.0.6:9000/helloWorld.txt");
        System.out.println("Created Path object.");

        try {
            FSDataOutputStream outputStream = hdfs.create(filePath);
            System.out.println("Called create() successfully.");
            BufferedWriter br = new BufferedWriter( new OutputStreamWriter( outputStream, "UTF-8" ) );
            System.out.println("Created BufferedWriter object.");
            br.write("Hello World");
            System.out.println("Wrote 'Hello World!' using BufferedWriter.");
            br.close();
            System.out.println("Closed BufferedWriter.");
            hdfs.close();
            System.out.println("Closed DistributedFileSystem object.");
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }
}