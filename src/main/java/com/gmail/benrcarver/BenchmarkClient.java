package com.gmail.benrcarver;

import java.util.concurrent.Callable;

/**
 * Base class implementing a benchmarking client.
 *
 * Subclass this class in order to create your own benchmark client.
 *
 * See HopsFSClient as an example.
 */
public abstract class BenchmarkClient implements Callable<BenchmarkResult> {
    /**
     * The number of RPC calls to make.
     */
    protected final int numRPC;

    /**
     * Current HopsFS API requires an ID, which is the ID of the desired user in the NDB database.
     */
    protected final int id;

    /**
     * We will be sending RPC requests here.
     */
    protected final String rpcEndpointUri;

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
    protected final int numQueries;

    /**
     * Default constructor. Sub-classes should call super() to access this cluster.
     *
     * @param numRpc The number of RPC calls executed by each worker/thread.
     * @param numQueries Currently unused.
     * @param id Currently unused.
     * @param rpcEndpointUri This is the endpoint to which this client will issue RPC requests.
     */
    public BenchmarkClient(int numRpc, int numQueries, int id, String rpcEndpointUri) {
        this.numRPC = numRpc;
        this.numQueries = numQueries;
        this.id = id;
        this.rpcEndpointUri = rpcEndpointUri;
    }
}
