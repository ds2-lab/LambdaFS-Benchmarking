# Real-World Workloads

This software also drives simulations of the HDFS Spotify workload described in the paper. This option can be selected from the interactive menu along with all of the other experiments. The real-world workload expects there to be a `workload.yaml` file in the root of the repository on the primary client (i.e., experiment driver). The following is a description of the available parameters.

## General Config Parameters for the Real-World Spotify Workload
- `num.worker.threads` (`int`): The total number of clients that each individual worker node should use. If this is set to `128` and there are 8 worker nodes used in the experiment, then there will be a total of 1,024 clients.
- `files.to.create.in.warmup.phase` (`int`): The number of files that each individual client should create at the very beginning of the experiment. These files are used to perform `move`, `delete`, and `rename` operations.
- `warmup.phase.wait.time` (`int`): How long to wait at the beginning for all "warm-up files" to be created before moving onto the actual experiment.
- `interleaved.bm.duration` (`int`): How long the real-world experiment should last (in *milliseconds*). 
- `interleaved.bm.iat.unit` (`int`) (**recommended:** `15`): How long, in seconds, the current randomly-generated throughput value should last before a new value is generated. 
- `interleaved.bm.iat.skipunit` (`int`) (**recommended:** `0`): Skips rate-limiting for this number of ticks. Recommended to leave this at 0. 
- `interleaved.bm.iat.distribution` (`string`) (**recommended:** `PARETO`): Defines the distribution to use when randomly generating file system operations. Options include `"UNIFORM"`, `"PARETO"` (default/recommended), `"POISSON"`, and `"ZIPF"`.
- `interleaved.bm.iat.pareto.alpha`(`int`): (**recommended:** `2`): Shape parameter of the `Pareto` distribution.
- `interleaved.bm.iat.pareto.location` (`int`): (**recommended:** `10000`): Used as a parameter to the `Pareto` distribution. 

## File System Operation Distribution Parameters
- `interleaved.create.files.percentage`(**recommended:** `1.09`): Percentage of `CREATE-FILE` operations.
- `interleaved.rename.files.percentage`(**recommended:** `0.55`): Percentage of `RENAME-FILE` operations.
- `interleaved.delete.files.percentage`(**recommended:** `0.34`): Percentage of `DELETE-FILE` operations.
- `interleaved.mkdir.percentage`(**recommended:** `0.02`): Percentage of `MKDIR` operations.
- `interleaved.read.files.percentage`(**recommended:** `71.84`): Percentage of `READ-FILE` operations.
- `interleaved.ls.dirs.percentage`(**recommended:** `8.17`): Percentage of `LIST-DIRECTORY` operations.
- `interleaved.ls.files.percentage`(**recommended:** `0.68`): Percentage of `LIST-FILE` operations.
- `interleaved.file.getInfo.percentage`(**recommended:** `13.54`): Percentage of `STAT-FILE` operations.
- `interleaved.dir.getInfo.percentage`(**recommended:** `3.77`): Percentage of `STAT-DIRECTORY` operations.