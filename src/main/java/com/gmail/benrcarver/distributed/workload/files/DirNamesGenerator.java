package com.gmail.benrcarver.distributed.workload.files;

import java.util.Arrays;

public class DirNamesGenerator {

    private static final int DEFAULT_DIR_PER_DIR = 32;
    private int[] pathIndecies = new int[40]; //level
    private String baseDir;
    private String currentDir;
    private int dirPerDir;

    DirNamesGenerator(String baseDir) {
        this(baseDir, DEFAULT_DIR_PER_DIR);
    }

    DirNamesGenerator(String baseDir, int filesPerDir) {
        this.baseDir = baseDir;
        this.dirPerDir = filesPerDir;
        reset();
    }

    String getNextDirName(String prefix) {
        int depth = 0;
        while (pathIndecies[depth] >= 0) {
            depth++;
        }
        int level;
        for (level = depth - 1;
             level >= 0 && pathIndecies[level] == dirPerDir - 1; level--) {
            pathIndecies[level] = 0;
        }
        if (level < 0) {
            pathIndecies[depth] = 0;
        } else {
            pathIndecies[level]++;
        }
        level = 0;
        String next = baseDir;
        while (pathIndecies[level] >= 0) {
            next = next + "/" + prefix + pathIndecies[level++];
        }
        return next;
    }

    private synchronized void reset() {
        Arrays.fill(pathIndecies, -1);
        currentDir = "";
    }

    synchronized int getFilesPerDirectory() {
        return dirPerDir;
    }

    synchronized String getCurrentDir() {
        return currentDir;
    }
}
