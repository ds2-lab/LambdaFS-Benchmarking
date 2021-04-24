package com.gmail.benrcarver;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.CollectionType;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.apache.commons.cli.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

public class HopsFSBenchmarkDriver {
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
    private static final String NUMBER_OF_QUERIES = "NUM_QUERIES";
    private static final String NUMBER_OF_THREADS = "NUM_THREADS";

    private static final String DEFAULT_CONFIG_LOCATION = "./benchmark.yaml";

    private static ArrayList<HopsFSClient> clients = new ArrayList<>();

    private static CommandLine parseArguments(String[] args) {
        Options options = new Options();

        Option yamlFileLocationOpt = new Option("i", "input", true, "The path to the YAMl file containing the benchmark definition.");
        yamlFileLocationOpt.setRequired(false);

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

        return cmd;
    }

    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {
        CommandLine cmd = parseArguments(args);

        String configFileLocation = DEFAULT_CONFIG_LOCATION;
        if (cmd.hasOption("input")) {
            configFileLocation = cmd.getOptionValue("input");
        }

        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        CollectionType listType = mapper.getTypeFactory().constructCollectionType(ArrayList.class, HopsFSNameNode.class);
        mapper.findAndRegisterModules();

        ArrayList<HopsFSNameNode> nameNodesForBenchmark = mapper.readValue(new File(configFileLocation), listType);

        System.out.println("Parsing NameNode definitions now...");
        System.out.println("There are " + nameNodesForBenchmark.size() + " NameNode(s) to process.");
        for (HopsFSNameNode hopsFSNameNode : nameNodesForBenchmark) {
            System.out.println("Creating " + hopsFSNameNode.getNumThreads() + " com.gmail.benrcarver.com.gmail.benrcarver.HopsFSClient objects for NameNode at " + hopsFSNameNode.getNameNodeUri());
            for (int i = 0; i < hopsFSNameNode.getNumThreads(); i++) {
                HopsFSClient client = new HopsFSClient(
                        hopsFSNameNode.getNumRpc(),
                        hopsFSNameNode.getNumQueries(),
                        hopsFSNameNode.getId(),
                        hopsFSNameNode.getNameNodeUri(),
                        hopsFSNameNode.getQuery(),
                        hopsFSNameNode.getDataSource(),
                        hopsFSNameNode.getNdbConnectionUri()
                );

                clients.add(client);
            }
        }

        // Example: https://howtodoinjava.com/java/multi-threading/java-callable-future-example/
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newCachedThreadPool();
        List<Future<BenchmarkResult>> resultsList = new ArrayList<>();

        System.out.println("Starting com.gmail.benrcarver.com.gmail.benrcarver.HopsFSClient objects now.");
        for (HopsFSClient client : clients) {
            Future<BenchmarkResult> timeResult = executor.submit(client);
            resultsList.add(timeResult);
        }

        // Keep track of the results per NameNode.
        HashMap<String, List<Double>> resultsPerNameNode = new HashMap<>();

        // All of the time results obtained during the benchmark.
        List<Double> allTimes = new ArrayList<>();

        // Iterate over all of the futures and record the results.
        for (Future<BenchmarkResult> future : resultsList) {
            BenchmarkResult result = future.get();

            // The times generated by this future.
            ArrayList<Double> timeResults = result.getTimeResults();

            // This URI identifies the target NameNode of this particular future/result.
            String associatedNameNodeUri = result.getAssociatedNameNodeUri();

            // Get the list of times associated with this NameNode so we can add the
            // latest results to the list.
            List<Double> associatedNameNodeTimes = resultsPerNameNode.get(associatedNameNodeUri);

            if (associatedNameNodeTimes == null) {
                associatedNameNodeTimes = new ArrayList<Double>(timeResults);
                resultsPerNameNode.put(associatedNameNodeUri, associatedNameNodeTimes);
                allTimes.addAll(timeResults);
            } else {
                // Record the times.
                associatedNameNodeTimes.addAll(timeResults);
                allTimes.addAll(timeResults);
            }
        }

        // Compute the total average and the per-NameNode average.
        for (Map.Entry<String, List<Double>> entry : resultsPerNameNode.entrySet()) {
            String nameNodeUri = entry.getKey();
            List<Double> times = entry.getValue();

            // Compute the sum, with which we will compute the average. Also
            // compute the min and the max.
            double sumofTimes = times.stream().reduce(0.0, Double::sum);
            double minTime = times.stream().reduce(0.0, Double::min);
            double maxTime = times.stream().reduce(0.0, Double::max);

            // Compute the average.
            double average = sumofTimes / times.size();

            System.out.println("NameNode at " + nameNodeUri + " (AVG/MIN/MAX):\n" + average + "\n" + minTime
                    + "\n" + maxTime + "\n");
        }

        double sumOfAllTimes = allTimes.stream().reduce(0.0, Double::sum);
        double minTime = allTimes.stream().reduce(0.0, Double::min);
        double maxTime = allTimes.stream().reduce(0.0, Double::max);
        double average = sumOfAllTimes / allTimes.size();

        System.out.println("HopsFS Aggregate Results (AVG/MIN/MAX):\n" + average + "\n" + minTime
                + "\n" + maxTime + "\n");
    }
}
