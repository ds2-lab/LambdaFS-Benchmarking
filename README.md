# Micro-Benchmarking & Real-World Workload Driver for λFS and HopsFS 

![Logo](https://github.com/ds2-lab/ds2-lab.github.io/blob/master/docs/images/lfs_logo.png)

This utility is designed to simplify the testing, debugging, and benchmarking process for λFS and HopsFS.

The official repository for λFS (as well as the fork of HopsFS that is compatible with this software) can be found [here](https://github.com/ds2-lab/LambdaFS).

_This software is in no way affiliated with HopsFS or its developers._

# Building this Software

This software was compiled and tested using the following software versions:

- OpenJDK Version 1.8.0_382
  - OpenJDK 64-Bit Server VM (build 25.382-b05, mixed mode) and Maven 3.6.3 on Ubuntu 
  - OpenJDK Runtime Environment (build 1.8.0_382-8u382-ga-1~22.04.1-b05)
- Maven 3.6.3
- Ubuntu 22.04.1 LTS

## Install Required JARs

You must build and compile the λFS and HopsFS source code and install the generated JARs to your local Maven repository. 

The λFS source code can be found [here](https://github.com/ds2-lab/LambdaFS) (the default branch of the `ds2/LambdaFS` GitHub repository, `serverless-namenode-aws`).

The version of HopsFS modified to work with this benchmarking software can be found [here](https://github.com/ds2-lab/LambdaFS/tree/3.2.0.2-caching) (the `3.2.0.2-caching` branch of the `ds2/LambdaFS` GitHub repository).

After compiling the λFS and HopsFS source code, you can install the required JARs into your local Maven repository as follows:

### **λFS**
``` sh
mvn install:install-file -Dfile=<PATH TO LOCAL λFS REPOSITORY>/hadoop-hdfs-project/hadoop-hdfs/target/hadoop-hdfs-3.2.0.3-SNAPSHOT.jar -DgroupId=io.hops -DartifactId=hadoop-hdfs -Dversion=3.2.0.3-SNAPSHOT -Dpackaging=jar -DgeneratePom=true

mvn install:install-file -Dfile=<PATH TO LOCAL HopsFS REPOSITORY>/hadoop-common-project/hadoop-common/target/hadoop-common-3.2.0.3-SNAPSHOT.jar -DgroupId=io.hops -DartifactId=hadoop-common -Dversion=3.2.0.3-SNAPSHOT -Dpackaging=jar -DgeneratePom=true
```

### **HopsFS**
``` sh
mvn install:install-file -Dfile=<PATH TO LOCAL λFS REPOSITORY>/hadoop-hdfs-project/hadoop-hdfs/target/hadoop-hdfs-3.2.0.2-RC0.jar -DgroupId=io.hops -DartifactId=hadoop-hdfs -Dversion=3.2.0.3-SNAPSHOT -Dpackaging=jar -DgeneratePom=true

mvn install:install-file -Dfile=<PATH TO LOCAL HopsFS REPOSITORY>/hadoop-common-project/hadoop-common/target/hadoop-common-3.2.0.2-RC0.jar -DgroupId=io.hops -DartifactId=hadoop-common -Dversion=3.2.0.3-SNAPSHOT -Dpackaging=jar -DgeneratePom=true
```

Make sure to replace the `<PATH TO LOCAL λFS REPOSITORY>` with the appropriate path when executing the commands shown above.

## Build the Application

To compile/build the benchmarking application, please execute the following command from the root directory:
``` sh
mvn clean compile assembly:single
```

# Configuration

This application expects a `config.yaml` file to be present in the root directory of the GitHub repository. There is a sample `config.yaml` already provided in the repository. When creating a `config.yaml` file, there are several configuration parameters to set:

- `hdfsConfigFile`: The path to the `hdfs-site.xml` configuration file associated with your local λFS or HopsFS installation.
- `namenodeEndpoint`: This is the endpoint of the local NameNode; this is relevant only when using this application with HopsFS (as opposed to λFS, in which case this configuration parameter is ignored).

## Distributed Mode

The remaining configuration is used only when running in `distributed` mode. As described above, `distributed` mode is enabled by default but can be disabled by passing the `-n` flag, which is recommended for basic testing and debugging.

- `commanderExecutesToo`: Determines whether the experiment driver also hosts actual file system clients that execute file system operations during benchmarks. This is `true` by default; it hasn't been fully tested when set to `false`.

Lastly, there is the `followers` parameter. This is expected to be a list of the form:

``` yaml
followers:
        -
                ip: 10.0.0.2
                user: ubuntu
        -
                ip: 10.0.0.3
                user: ubuntu
        -
                ip: 10.0.0.4
                user: ubuntu
```

For each "follower" (i.e., other machine on which you'd like to run the benchmarking software), you must add an entry to the `followers` list using the format shown above. If deployed on AWS EC2 within a VPC, then the `ip` is the private IPv4 of the EC2 VM. For `user`, specify the OS username that should be used when SSH-ing to the machine. If using our provided EC2 AMIs, then this will be `ubuntu`. The `user` configuration parameter is the username that should be used when using `SSH` or `SFTP` to start/stop the client automatically and to copy configuration files to the client VM.

## Automated Configuration

There are two scripts to help setup the configuration file for you. These are `scripts/get_client_ips.sh` and `scripts/create_benchmark_config.py`. 

### **The `get_client_ips.sh` Script**

The `get_client_ips.sh` script is called by `create_benchmark_config.py`; you shouldn't need to execute `get_client_ips.sh` yourself. That being said, the `get_client_ips.sh` expects a single command-line argument: the name of the EC2 autoscaling group associated with the other client VMs for your λFS or HopsFS deployment. This autoscaling group is created automatically by the `create_aws_infrastrucutre.py` script available in the λFS repository. If you do not specify the name of the autoscaling group when executing `get_client_ips.sh`, it will default to `"lambdafs_clients_ag"`. 

### **The `create_benchmark_config.py` Script**

This script was created and tested using Python 3.10.12. It generates a complete `config.yaml` file for you automatically, populated with the private IPv4s of any already-running client VMs from your EC2 autoscaling group.

**Script Arguments:**
``` 
-o OUTPUT, --output OUTPUT
                    Path of the `config.yaml` output file. Default: "config.yaml"

-u USER, --user USER 
                    Username to include in the config file. Default: "ubuntu".

-c HDFS_SITE_CONFIG_FILE_PATH, --hdfs-config-file HDFS_SITE_CONFIG_FILE_PATH
                    Path to the hdfs-site configuration file. Default: "/home/ubuntu/repos/hops/hadoop-dist/target/hadoop-3.2.0.3-SNAPSHOT/etc/hadoop/hdfs-site.xml"
-i PRIVATE_IP, --private-ip PRIVATE_IP
                    Private IPv4 of the primary client/experiment driver. This script does not check that a specified IP is actually valid. By default, the script attempts to resolve this automatically.
-a AUTOSCALING_GROUP_NAME, --autoscaling-group-name AUTOSCALING_GROUP_NAME
                    The name of the autoscaling group for the client VMs.
```

# Executing this Software

## The `HADOOP_HOME` Environment Variable

Because this software interfaces with the client API of either λFS or HopsFS, it requires many of the same dependencies. We can easily include all of these dependencies by including on the classpath the following two directories: `$HADOOP_HOME/share/hadoop/hdfs/lib/` and `$HADOOP_HOME/share/hadoop/common/lib/`, where the `$HADOOP_HOME` environment variable contains the file path to the λFS or HopsFS installation directory.

For example, on an Ubuntu virtual machine where the λFS local repository is in the `~/repos/LambdaFS` directory, the value of `$HADOOP_HOME` should be `/home/ubuntu/repos/LambdaFS/hadoop-dist/target/hadoop-3.2.0.3-SNAPSHOT`. For HopsFS, it would instead be `/home/ubuntu/repos/LambdaFS/hadoop-dist/target/hadoop-3.2.0.2-RC0`.

## Running the Application

All (Java) commands related to executing the benchmarking utility must be executed from the root directory of the local repository. On the publicly available AMIs, this is the `/home/ubuntu/repos/LambdaFS-BenchmarkingUtility` directory. 

This software can be run in two modes: `distributed` and `non-distributed` mode. `distributed` mode is enabled by default but can be disabled by passing the `-n` flag, which is recommended for basic testing and debugging. All of the commands below include the `-n` flag, but the same exact commands could be used with the `-n` flag ommitted to run the application in `distributed` mode.

### **The General Command Format**

This software can be executed with the following command (from the root of the benchmarking repository):
``` sh
java -Dlog4j.configuration=file:<PATH TO LOCAL LambdaFS-Benchmark-Utility REPO>/src/main/resources/log4j.properties \
-Dsun.io.serialization.extendedDebugInfo=true -Xmx2g -Xms2g -XX:+UseConcMarkSweepGC -XX:+UnlockDiagnosticVMOptions \
-XX:ParGCCardsPerStrideChunk=512 -XX:+CMSScavengeBeforeRemark -XX:MaxGCPauseMillis=350 -XX:MaxTenuringThreshold=2 \
-XX:MaxNewSize=1000m -XX:+CMSClassUnloadingEnabled -XX:+ScavengeBeforeFullGC \
-cp ".:target/HopsFSBenchmark-1.0-jar-with-dependencies.jar:$HADOOP_HOME/share/hadoop/hdfs/lib/*:$HADOOP_HOME/share/hadoop/common/lib/*" \
com.gmail.benrcarver.distributed.InteractiveTest --leader_ip <PRIVATE IPv4 OF VM> --leader_port 8000 --yaml_path <PATH TO>/config.yaml -n
```

Make sure to replace the `<PATH TO LOCAL LambdaFS-Benchmark-Utility REPO>` with the appropriate path when executing the commands shown above. Likewise, do the same for the `<PATH TO>/config.yaml` file.

### **Specific, Realistic Example**

If you were to run this software on an Ubuntu VM with private IPv4 `10.0.0.1` using the `ubuntu` user, and the local repository were to be located in `~/repos/`, then the command would look like:
``` sh
java -Dlog4j.configuration=file:/home/ubuntu/repos/LambdaFS-Benchmark-Utility/src/main/resources/log4j.properties \
-Dsun.io.serialization.extendedDebugInfo=true -Xmx2g -Xms2g -XX:+UseConcMarkSweepGC -XX:+UnlockDiagnosticVMOptions \
-XX:ParGCCardsPerStrideChunk=512 -XX:+CMSScavengeBeforeRemark -XX:MaxGCPauseMillis=350 -XX:MaxTenuringThreshold=2 \
-XX:MaxNewSize=1000m -XX:+CMSClassUnloadingEnabled -XX:+ScavengeBeforeFullGC \
-cp ".:target/HopsFSBenchmark-1.0-jar-with-dependencies.jar:$HADOOP_HOME/share/hadoop/hdfs/lib/*:$HADOOP_HOME/share/hadoop/common/lib/*" \
com.gmail.benrcarver.distributed.InteractiveTest --leader_ip 10.0.0.1 --leader_port 8000 --yaml_path /home/ubuntu/repos/LambdaFS-Benchmark-Utility/config.yaml -n
```

You can optionally add the `-n` flag to run the benchmarking application in `non-distributed` mode. The application will not attempt to start other instances of itself on other virtual machines as configured in its `config.yaml` file when in `non-distributed` mode.

We're setting the JVM heap size to 2GB in the above command via the flags `-Xmx2g -Xms2g`. If you're using a VM with less than 2GB of RAM, then you should adjust this value accordingly. We're also specifying several other garbage-collection-related JVM arguments in that of `-XX:ParGCCardsPerStrideChunk` and `-XX:MaxNewSize`. If you reduce the JVM heap size (via the `-Xmx` and `-Xms` flags), then you should also adjust these other GC-related flags accordingly.

Likewise, you should adjust the `-Xmx` and `-Xms` arguments according to how much RAM we have available. For our resource/hardware recommendations, see the next section of this README. 

### **Simplest Example**

Without the recommended GC and JVM arguments, execution the application in the same context as above would look like:
``` sh
java -cp ".:target/HopsFSBenchmark-1.0-jar-with-dependencies.jar:$HADOOP_HOME/share/hadoop/hdfs/lib/*:$HADOOP_HOME/share/hadoop/common/lib/*" \
com.gmail.benrcarver.distributed.InteractiveTest --leader_ip 10.0.0.1 --leader_port 8000 --yaml_path /home/ubuntu/repos/LambdaFS-Benchmark-Utility/config.yaml -n
```

Again, the above Java commands must be executed from the root directory of the benchmarking repository. 

## Recommended Hardware 

We recommend at least 8GB of RAM; however, we performed our λFS and HopsFS evaluations with the JVM heap set to 100GB for the benchmark application. In particular, we used AWS EC2 `r5.4xlarge` virtual machines for all client VMs, which have 16 vCPU and 128GB RAM. (Each client VM runs an instance of the benchmarking application.)

## Full List of Available Command-Line Arguments

The following is the full list of available command-line arguments for the λFS Benchmarking Utility.
```
-w  --worker                      [no value] [default: false]
  If passed/set, then run the application as a "worker", listening to commands provided by a remote leader.

-l  --leader_ip                   [string] [required]
  The IP address of the Leader. Only used when this process is designated as a worker.
  When running on AWS EC2 within a VPC, this should be the private IPv4 of the leader's VM.

-p  --leader_port                 [int] [required]
  The port of the Leader. Only used when this process is designated as a worker.

-n  --nondistributed              [no value] [default: false]
  Run in non-distributed mode, meaning we don't launch any followers.

-f  --num_followers               [int] [default: -1]
  Start only the first 'f' followers listed in the config.

-j  --scp_jars                    [no value] [default: false]
  The commander should SCP the JAR files to each follower.

-c  --scp_config                  [no value] [default: false]
  The command should SCP the hdfs-site.xml config file to each follower.

-m  --manually_launch_followers   [no value] [default: false]
```

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

# Reproducing the Experiments from the ASPLOS'23 Paper

For documentation concerning how to reproduce the experiments used in the ASPLOS'23 paper, please refer to the documentation contained within the `./documentation/asplos23_experiments.md` file.

## Plots

The scripts used to generate the plots found in the ASPLOS'23 paper are located within the `./plots` directory.

The scripts with `_yaml` in their name are the most up-to-date. They expect a `.YAML` file specifying each of the inputs for each of the different file systems. We provide some sample `.YAML` configuration files in the `./plots/sample_yaml_configs/` directory.

# Associated Publications

This software was used to evaluate both λFS and HopsFS for the paper, *λFS: A Scalable and Elastic Distributed File System Metadata Service using Serverless Functions*. This paper can be found [here](https://arxiv.org/abs/2306.11877) and is set to appear in the proceedings of ASPLOS'23.

**BibTeX Citation (for arXiv preprint)**:
``` TeX
@misc{
    lambdafs_asplos23,
    title={$\lambda$FS: A Scalable and Elastic Distributed File System Metadata Service using Serverless Functions}, 
    author={Benjamin Carver and Runzhou Han and Jingyaun Zhang and Mai Zheng and Yue Cheng},
    year={2023},
    eprint={2306.11877},
    archivePrefix={arXiv},
    primaryClass={cs.DC}
}
```

This citation will be updated once the paper is officially published in the proceedings of ASPLOS'23. 