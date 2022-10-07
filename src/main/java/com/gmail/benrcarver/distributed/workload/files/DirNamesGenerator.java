package com.gmail.benrcarver.distributed.workload.files;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Scanner;
import java.util.Set;

public class DirNamesGenerator {

    private static final int DEFAULT_DIR_PER_DIR = 32;
    private final int[] pathIndices = new int[40]; //level
    private final String baseDir;
    private String currentDir;
    private final int dirPerDir;

    /**
     * Set of leaf directories (i.e., directories with no child directories).
     */
    private final Set<String> leafDirectories;

    DirNamesGenerator(String baseDir) {
        this(baseDir, DEFAULT_DIR_PER_DIR);
    }

    DirNamesGenerator(String baseDir, int dirPerDir) {
        this.baseDir = baseDir;
        this.dirPerDir = dirPerDir;
        this.leafDirectories = new HashSet<>();
        reset();
    }

    String getNextDirName(String prefix) {
        int depth = 0;
        while (pathIndices[depth] >= 0) {
            depth++;
        }

        int level;
        for (level = depth - 1; level >= 0 && pathIndices[level] == dirPerDir - 1; level--) {
            pathIndices[level] = 0;
        }
        if (level < 0) {
            pathIndices[depth] = 0;
        } else {
            pathIndices[level]++;
        }
        level = 0;
        StringBuilder next = new StringBuilder(baseDir);
        while (pathIndices[level] >= 0) {
            int childNum = pathIndices[level];

            // The first condition checks that we're on the last iteration of the loop.
            // The second condition checks if we're creating the first child directory or not.
            // If we are creating the first child directory, then remove the parent from the set of leaf directories.
            if (pathIndices[level + 1] < 0) {
                System.out.println("childNum: " + childNum);
                System.out.println("Parent directory: " + next.toString());
                // The parent directory will be the current form of the string in the string builder.
                leafDirectories.remove(next.toString());
            }

            next.append("/").append(prefix).append(childNum);

            level += 1;
        }

        String newDirectory = next.toString();
        leafDirectories.add(newDirectory);
        return newDirectory;
    }

    protected Set<String> getLeafDirectories() { return this.leafDirectories; }
    
    private synchronized void reset() {
        Arrays.fill(pathIndices, -1);
        currentDir = "";
    }

    synchronized int getFilesPerDirectory() {
        return dirPerDir;
    }

    synchronized String getCurrentDir() {
        return currentDir;
    }
}
