package com.gmail.benrcarver.distributed.workload.files;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

public class NameSpaceGenerator {
    private int filesPerDirCount;
    private final List<String> allDirs;
    private final List<String> allDirectoriesEverCreated;
    private final int FILES_PER_DIR;
    private final int DIR_PER_DIR;
    private int fileCounter;
    private final DirNamesGenerator dirGenerator;
    private static String DIR_PREFIX = "hops_dir";
    private static String FILE_PREFIX = "hops_file";

    public NameSpaceGenerator(String baseDir, int filesPerDir, int dirPerDir) {
        this.allDirs = new LinkedList<>();
        this.allDirectoriesEverCreated = new ArrayList<>();
        this.FILES_PER_DIR = filesPerDir;
        this.DIR_PER_DIR = dirPerDir;

        this.fileCounter = 0;
        this.dirGenerator = new DirNamesGenerator(baseDir, DIR_PER_DIR);
        this.filesPerDirCount = 0;
    }

    /**
     * Return a leaf directory.
     */
    public String getRandomLeafDirectory() {
        return null;
    }

    public String generateNewDirPath(){
        String path = dirGenerator.getNextDirName(DIR_PREFIX);
        allDirs.add(path);
        allDirectoriesEverCreated.add(path);
        return path;
    }

    public String getFileToCreate() {
        if (allDirs.isEmpty()) {
            generateNewDirPath();
        }

        assert filesPerDirCount < FILES_PER_DIR;
        filesPerDirCount++;
        String filePath = allDirs.get(0)+"/"+FILE_PREFIX+"_"+(fileCounter++);

        if (filesPerDirCount >= FILES_PER_DIR) {
            allDirs.remove(0);
            filesPerDirCount = 0;
        }
        return filePath;
    }
}
