# HopsFS Latency Benchmark Utility

<img src="https://i.imgur.com/USMkTD7.png" width="192" height="192" />

I wrote this utility to simplify the latency benchmarking process for HopsFS.

This software is written in Java and built/compiled with Maven. 

## Features

- Automatically exports latencies to a text file at the end of the benchmark.
- Reports the results of the benchmark as a histogram in text format; easy importing to Microsoft Excel.
- Can specify the number of Histogram buckets via the command-line (buckets are at a millisecond granularity).
- Easily define benchmarks via YAML.

### Define a Benchmark　

```yaml
- dataSource: FROM_NDB                                      # Can be "FROM_NDB" or "LOCAL_CACHE"
   ndbConnectionUri: jdbc:mysql://10.150.0.11:3306/world    # IP address of your MySQL Cluster (NDB) MySQL server.
   query: "SELECT * FROM users WHERE ID = 1"                # MySQL query that the NameNode should execute.
   nameNodeUri: "hdfs://10.150.0.6:9000"                    # Endpoint of the desired NameNode.
   numQueries: 1                                            # Currently unused.  
   numRpc: 1536                                             # Number of RPC requests issued PER CLIENT (i.e., PER THREAD).
   id: 1                                                    # Currently unused.  
   numThreads: 15                                           # Number of HopsFS clients (one client per thread).
# Define a second NameNode to issue RPC requests to during the benchmark.
- dataSource: FROM_NDB  
   ndbConnectionUri: jdbc:mysql://10.150.0.11:3306/world
   query: "SELECT * FROM users WHERE ID = 1"
   nameNodeUri: "hdfs://10.150.0.13:9000"
   numQueries: 1
   numRpc: 1536
   id: 1
   numThreads: 15
```
