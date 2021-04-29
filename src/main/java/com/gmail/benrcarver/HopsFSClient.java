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

/**
 * Executed as a thread by a thread pool executor. Issues RPC requests to a HopsFS NameNode.
 */
public class HopsFSClient extends BenchmarkClient {
    /**
     * The NDB URI that the NameNode should issue NDB requests to.
     */
    private final String ndbConnectionUri;

    /**
     * The query to be issued via the RPC call.
     */
    protected final String query;

    /**
     * Indicates whether the NameNode should retrieve requested "metadata" from NDB or its local storage.
     */
    protected final String dataSource;

    /**
     *
     * @param numRpc The number of RPC calls executed by each worker/thread.
     * @param numQueries Currently unused.
     * @param id Currently unused.
     * @param nameNodeUri This is the HopsFS NameNode URI to which this client will issue RPC requests.
     * @param query This is the NDB query that the HopsFS NameNode will be executing.
     * @param dataSource This is the source from which the HopsFS NameNode will retrieve data.
     * @param ndbConnectionUri This is the URI that the NameNode will use to connect to NDB.
     */
    public HopsFSClient(int numRpc, int numQueries, int id, String nameNodeUri, String query,
                        String dataSource, String ndbConnectionUri) {
        super(numRpc, numQueries, id, nameNodeUri);

        this.ndbConnectionUri = ndbConnectionUri;
        this.query = query;
        this.dataSource = dataSource;
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

        dfs.initialize(new URI(this.rpcEndpointUri), configuration);

        // Issue the specified number of RPC calls, collecting results as we go.
        for (int i = 0; i < numRPC; i++) {
            long startTime = System.nanoTime();
            dfs.latencyBenchmark(this.ndbConnectionUri, this.dataSource, this.query, this.id);
            long endTime = System.nanoTime();
            long durationInNano = (endTime - startTime);
            long durationInMillis = TimeUnit.NANOSECONDS.toMillis(durationInNano);
            times.add(durationInMillis / 1000.0);
        }

        BenchmarkResult result = new BenchmarkResult(this.rpcEndpointUri, times);

        return result;
    }
}