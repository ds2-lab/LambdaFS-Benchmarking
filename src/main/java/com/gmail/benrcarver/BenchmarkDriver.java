package com.gmail.benrcarver;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.CollectionType;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.apache.commons.cli.*;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * The actual driver for the benchmark.
 *
 * TODO: Add functionality to specify the name of the BenchmarkClient class to be used.
 * TODO: Figure out how to pass arbitrary arguments to the BenchmarkClient in question.
 */
public class BenchmarkDriver {
    private static final String DEFAULT_CONFIG_LOCATION = "./benchmark.yaml";

    private static final int DEFAULT_NUM_BUCKETS = 1000;

    private static final ArrayList<BenchmarkClient> clients = new ArrayList<>();

    /**
     * Specify the range of latencies to track in the histogram.
     */
    private static int buckets;

    /**
     * Groups operations in discrete blocks of 1ms width.
     */
        private static long[] histogram;

    /**
     * Counts all operations outside the histogram's range.
     */
    private static long histogramOverflow;

    /**
     * The total number of reported operations.
     */
    private static long operations;

    /**
     * The sum of each latency measurement squared over all operations.
     * Used to calculate variance of latency.
     * Calculated in ms.
     */
    private static double totalSquaredLatency;

    /**
     * Parse command-line arguments. Currently, there is possibly just one, which would
     * be the path to the YAML file defining the benchmark.
     * @param args The command-line arguments from the main method.
     */
    private static CommandLine parseArguments(String[] args) {
        Options options = new Options();

        Option yamlFileLocationOpt = new Option("i", "input", true, "The path to the YAMl file containing the benchmark definition.");
        yamlFileLocationOpt.setRequired(false);

        Option histogramBucketsOpt = new Option("b", "buckets", true, "Specify the range of latencies to track in the histogram.");
        histogramBucketsOpt.setRequired(false);

        Option clientClassOpt = new Option("c", "client", true, "Fully-qualified classname of the client class to use for the benchmark.");
        clientClassOpt.setRequired(true);

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

    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException, InvocationTargetException, InstantiationException, IllegalAccessException {
        // Parse command-line arguments.
        CommandLine cmd = parseArguments(args);

        // Load the YAML file from the default location or the user-specified location, if the user specified one.
        String configFileLocation = DEFAULT_CONFIG_LOCATION;
        if (cmd.hasOption("input")) {
            configFileLocation = cmd.getOptionValue("input");
        }

        if (cmd.hasOption("buckets")) {
            buckets = Integer.parseInt(cmd.getOptionValue("buckets"));
        }
        else {
            buckets = DEFAULT_NUM_BUCKETS;
        }

        System.out.println("Creating histogram with " + buckets + " buckets.");

        histogram = new long[buckets + 1];

        histogramOverflow = 0;
        operations = 0;
        totalSquaredLatency = 0;

        // Parse the YAML file.
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        CollectionType listType = mapper.getTypeFactory().constructCollectionType(ArrayList.class, HopsFSNameNode.class);
        mapper.findAndRegisterModules();

        ArrayList<HopsFSNameNode> nameNodesForBenchmark = mapper.readValue(new File(configFileLocation), listType);

        System.out.println("Parsing NameNode definitions now...");
        System.out.println("There are " + nameNodesForBenchmark.size() + " NameNode(s) to process.");

        // Create the HopsFSClient objects in preparation for the benchmark.
        for (HopsFSNameNode hopsFSNameNode : nameNodesForBenchmark) {
            System.out.println("Creating " + hopsFSNameNode.getNumThreads() + " com.gmail.benrcarver.com.gmail.benrcarver.HopsFSClient objects for NameNode at " + hopsFSNameNode.getNameNodeUri());
            for (int i = 0; i < hopsFSNameNode.getNumThreads(); i++) {
                BenchmarkClient client = (BenchmarkClient) HopsFSNameNode.DRIVER_CLASS.getConstructors()[0].newInstance(
                    hopsFSNameNode.getNumRpc(),
                    hopsFSNameNode.getNumQueries(),
                    hopsFSNameNode.getId(),
                    hopsFSNameNode.getNameNodeUri(),
                    hopsFSNameNode.getQuery(),
                    hopsFSNameNode.getDataSource(),
                    hopsFSNameNode.getNdbConnectionUri());
                clients.add(client);
            }
        }

        // Use a ThreadPoolExecutor to drive the benchmark.
        // Example: https://howtodoinjava.com/java/multi-threading/java-callable-future-example/
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newCachedThreadPool();
        List<Future<BenchmarkResult>> resultsList = new ArrayList<>();

        System.out.println("Starting HopsFSClient objects now.");
        // Fire 'em off.
        for (BenchmarkClient client : clients) {
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
            } else {
                // Record the times.
                associatedNameNodeTimes.addAll(timeResults);
            }
            allTimes.addAll(timeResults);
        }

        // Compute the total average and the per-NameNode average.
        for (Map.Entry<String, List<Double>> entry : resultsPerNameNode.entrySet()) {
            String nameNodeUri = entry.getKey();
            List<Double> times = entry.getValue();

            // Compute the sum, with which we will compute the average. Also
            // compute the min and the max.
            double sumofTimes = times.stream().reduce(0.0, Double::sum);
            double maxTime = times.stream().reduce(0.0, Double::max);
            double minTime = times.stream().reduce(maxTime, Double::min);

            // Compute the average.
            double average = sumofTimes / times.size();

            System.out.println("Number of times for NameNode at " + nameNodeUri + ": " + times.size());
            System.out.println("NameNode at " + nameNodeUri + " (AVG/MIN/MAX/ALL):\n" + average + "\n" + minTime
                    + "\n" + maxTime + "\n" + times.toString() + "\n");
        }

        double sumOfAllTimes = allTimes.stream().reduce(0.0, Double::sum);
        double maxTime = allTimes.stream().reduce(0.0, Double::max);
        double minTime = allTimes.stream().reduce(maxTime, Double::min);
        double average = sumOfAllTimes / allTimes.size();
        
        for (double latency : allTimes) {
            measure(latency);
        }

        BufferedWriter outputWriter = null;
        outputWriter = new BufferedWriter(new FileWriter("output.txt"));
        for (int i = 0; i < allTimes.size(); i++) {
            // Maybe:
            outputWriter.write(allTimes.get(i)+"");
            outputWriter.newLine();
        }
        outputWriter.flush();
        outputWriter.close();

        System.out.println("Total number of times collected: " + allTimes.size());
        System.out.println("HopsFS Aggregate Results (AVG/MIN/MAX):\n" + average + "\n" + minTime
                + "\n" + maxTime + "\n");

        double variance = totalSquaredLatency / ((double) operations) - (average * average);
        System.out.println("Operations: " + operations);
        System.out.println("LatencyVariance(us): " + variance);

        long opcounter=0;
        boolean done95th = false;
        for (int i = 0; i < buckets; i++) {
            opcounter += histogram[i];
            if ((!done95th) && (((double) opcounter) / ((double) operations) >= 0.95)) {
                System.out.println("95thPercentileLatency(us): " + i * 1000);
                done95th = true;
            }
            if (((double) opcounter) / ((double) operations) >= 0.99) {
                System.out.println("99thPercentileLatency(us): " + i * 1000);
                break;
            }
        }

        for (int i = 0; i < buckets; i++) {
            System.out.println(i + ": " + histogram[i]);
        }

        System.out.println("> " + buckets + ": " + histogramOverflow);
    }

    public static synchronized void measure(double latencySeconds) {
        double latencyMilliseconds = latencySeconds * 1000;

        // Latency reported in us and collected in bucket by ms.
        if ((int)latencyMilliseconds >= buckets) {
            histogramOverflow++;
            histogram[buckets]++;
        } else {
            histogram[(int)latencyMilliseconds]++;
        }
        operations++;
        totalSquaredLatency += (latencyMilliseconds * latencyMilliseconds);
    }
}
