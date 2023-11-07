# Reproducing the Experiments from the ASPLOS'23 Paper

## High-Level Guide

The client-driven and resource-driven scaling experiments were performed using the following options (of the software's interactive menu):
- 17: Write `n` Files with `n` Threads
- 20: Weak Scaling Reads v2
- 21: File Stat Benchmark
- 23: List Directories from File
- 24: Stat File
- 25: Weak Scaling (MKDIR)

The real-world workload can be executed using option 26 "Randomly-generated workload". The provided `workload.yaml` file in the root of this repository contains matching configuration (to the ASPLOS'23 paper).

## Specifics

We recommend first generating a bunch of directories and then writing a bunch of random files to those directories. This documentation will describe how this can be done using the benchmarking software.

### **Generating Directories**

In order to supply files for the experiments, we provide a utility within the benchmarking software to generate a directory subtree (of configurable breadth and depth) with a configurable number of files written to each directory.

This can be accessed via option (8) `Create Subtree`. (To specify this option, simply enter the numerical value `8` when prompted.) You will be asked the following prompts (paraphrased here) after selecting option (8):
1. In what directory should the subtree root be created? `Recommended answer: "/"`
2. Subtree depth? 
3. How many subdirectories should be created for eahc directory in the tree (up to the specified depth)? 

After answering these prompts, the number of directories that will be created will be shown, and you will be asked if this is acceptable. If you respond with `y` or `yes`, then a subtree will be created according to the configuration you provided. If you respond with `n` or `no`, then the creation of the subtree will be aborted, and you will be returned to the main menu.

Once the subtree has been created, the software will write all of the paths of all of the directories created within the subtree to a file: `./output/createSubtree-<current unix epoch milliseconds>.txt`. This file can be used as input to *another* command, which will write a configurable number of files to each of the directories specified within the input file.

We recommend copying that file out of the `./output/` directory and placing it somewhere else, such as in the project root directory, or in another folder located in the root directory (e.g., a `./directories/` folder that you create), and assigning the file a different name. This is simply so that it's easier to remember which paths are contained within the file.

The file containing all of the directory paths can be used for experiments that target directories (rather than files), such as `list dir`.

### **Generating Files**

Once you've generated a bunch of directories, you can generate a bunch of random files within those directories. To do this, specify the `(14) Write Files to Directories` option from the main menu of the software. (To specify this option, simply enter the numerical value `14` when prompted.)

The first prompt you will be shown will read: `"Manually input (comma-separated list) [1], or specify file containing directories [2]?"`

If you followed the steps above, then you will want to respond with `2`, as you should have a file containing a bunch of λFS/HopsFS directories available. Otherwise, you could manually specify directories as a comma-separated list (e.g., `/dir1,/dir2,/dir3/subdir`). 

If you opt to enter the directories as a comma-separated list (by responding with `1`), then you will be shown another prompt `"Please enter the directories as a comma-separated list:"` to which you should respond with a comma-separated list of directories.

If you instead elect to provide a path to a file, then you will be shown the prompt `"Please provide path to file containing HopsFS directories:"`. You should respond with a path to a file stored within your *local file system*. The paths contained within the file will be λFS/HopsFS paths, but the file should be stored locally (i.e., not in λFS/HopsFS). 

In either case, the software will let you know how many directories were specified. Next, you will next be asked how many threads you'd like to use. This is up to you. We recommend anywhere from 1 - 32 threads.

Next, you'll be asked how many files should be created per directory. This is once again up to you.

The software will calculate the total number of files that will be created. You will *not* be prompted for confirmation at this point, so specify your value carefully.

Once specified, the software will begin generating files within the specified directories. The software will create a file on your local file system `./output/writeToDirectoryPaths-<current unix epoch milliseconds>.txt`. This file will contain the fully-qualified paths of all of the files that were generated. This is the file that should be used for any experiments that target files (rather than directories).

### **Client-Driven Experiments**

The client-driven `read` benchmark can be executed via Option 20: `Weak Scaling Reads v2`.

The prompts are as follows:
1. What benchmark should be performed? `20`
2. How many threads should be used? `However many you want, e.g., 1 - 128`
3. How many files should each thread read? `3072`
4. Please provide a path to a local file containing at least `<answer to prompt #3>` HopsFS path(s): `Path to file on your local file system as described in the sections above`.
5. Shuffle paths? `true`
6. How many trials should be performed? `However many you want, e.g., 6`

```
15
1
500
109200
true
6
```

### **Resource-Driven Experiments**