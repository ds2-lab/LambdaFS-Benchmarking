# HopsFS Latency Benchmark Utility

![](https://i.imgur.com/mZ6D7gD.png)

I wrote this utility to simplify the latency benchmarking process for HopsFS.

This software is written in Java and built/compiled with Maven. 

## Features

- Automatically exports latencies to a text file at the end of the benchmark.
- Reports the results of the benchmark as a histogram in text format; easy importing to Microsoft Excel.
- Can specify the number of Histogram buckets via the command-line (buckets are at a millisecond granularity).
- Easily define benchmarks via YAML.

### Define a Benchmark　

```
- dataSource: FROM_NDB
   ndbConnectionUri: jdbc:mysql://10.150.0.11:3306/world
   query: "SELECT * FROM users WHERE ID = 1"
   nameNodeUri: "hdfs://10.150.0.6:9000"
   numQueries: 1
   numRpc: 1536
   id: 1
   numThreads: 15
- dataSource: FROM_NDB
   ndbConnectionUri: jdbc:mysql://10.150.0.11:3306/world
   query: "SELECT * FROM users WHERE ID = 1"
   nameNodeUri: "hdfs://10.150.0.13:9000"
   numQueries: 1
   numRpc: 1536
   id: 1
   numThreads: 15
```
