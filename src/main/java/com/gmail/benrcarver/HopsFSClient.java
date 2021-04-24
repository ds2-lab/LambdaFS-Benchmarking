package com.gmail.benrcarver;

import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.Callable;

public class HopsFSClient implements Callable<BenchmarkResult>{
    /**
     * The number of RPC calls to make.
     */
    private final int numRPC;

    /**
     * Current HopsFS API requires an ID, which is the ID of the desired user in the NDB database.
     */
    private final int id;

    /**
     * The URI of the NameNode we'll be contacting.
     */
    private final String nameNodeUri;

    /**
     * The query to be issued via the RPC call.
     */
    private final String query;

    /**
     * Indicates whether the NameNode should retrieve requested "metadata" from NDB or its local storage.
     */
    private final String dataSource;

    /**
     * The NDB URI that the NameNode should issue NDB requests to.
     */
    private final String ndbConnectionUri;

    /**
     * The number of queries the NameNode should execute PER RPC REQUEST.
     *
     * The total number of queries executed by this NameNode will be equal to:
     *
     *      numThreads * numRpc * numQueries,
     *
     * assuming the NameNode is directed to issue NDB queries rather than retrieve
     * data from its local memory/cache).
     */
    private final int numQueries;

    public HopsFSClient(int numRpc, int numQueries, int id, String nameNodeUri, String query,
                        String dataSource, String ndbConnectionUri) {
        this.numRPC = numRpc;
        this.numQueries = numQueries;
        this.id = id;
        this.nameNodeUri = nameNodeUri;
        this.query = query;
        this.dataSource = dataSource;
        this.ndbConnectionUri = ndbConnectionUri;
    }

    @Override
    public BenchmarkResult call() throws URISyntaxException, IOException, SQLException {
        // Steps:
        // 1) Connect to HopsFS.
        // 2) Issue RPC requests.
        // 3) Return timing results to driver.

        // Collect time results here.
        ArrayList<Double> times = new ArrayList<>();

        // Step 1: Connect to HopsFS
        Configuration configuration = new Configuration();
        DistributedFileSystem dfs = new DistributedFileSystem();

        dfs.initialize(new URI(nameNodeUri), configuration);

        // Issue the specified number of RPC calls, collecting results as we go.
        for (int i = 0; i < numRPC; i++) {
            long startTime = System.nanoTime();
            dfs.latencyBenchmark(ndbConnectionUri, dataSource, query, id);
            long endTime = System.nanoTime();
            long durationInNano = (endTime - startTime);
            long durationInMillis = TimeUnit.NANOSECONDS.toMillis(durationInNano);
            times.add(durationInMillis / 1000.0);
        }

        BenchmarkResult result = new BenchmarkResult(nameNodeUri, times);

        return result;
    }
}