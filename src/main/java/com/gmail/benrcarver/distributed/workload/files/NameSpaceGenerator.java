package com.gmail.benrcarver.distributed.workload.files;

import java.util.LinkedList;
import java.util.List;
import java.util.Random;

public class NameSpaceGenerator {
    private int filesPerDirCount;
    private List<String> allDirs;
    private final int FILES_PER_DIR;
    private final int DIR_PER_DIR;
    private int fileCounter;
    private DirNamesGenerator dirGenerator;
    private String DIR_PREFIX = "hops_dir";
    private String FILE_PREFIX = "hops_file";
    private static Random rand = new Random(System.currentTimeMillis());

    public NameSpaceGenerator(String baseDir, int filesPerDir, int dirPerDir) {
        this.allDirs = new LinkedList<String>();
        this.FILES_PER_DIR = filesPerDir;
        this.DIR_PER_DIR = dirPerDir;

        this.fileCounter = 0;
        this.dirGenerator = new DirNamesGenerator(baseDir,DIR_PER_DIR);
        this.filesPerDirCount = 0;
    }

    public String generateNewDirPath(){
        String path = dirGenerator.getNextDirName(DIR_PREFIX);
        allDirs.add(path);
        return path;
    }

    public String getFileToCreate() {

        if(allDirs.isEmpty()){
            generateNewDirPath();
        }

        assert filesPerDirCount < FILES_PER_DIR;
        filesPerDirCount++;
        String filePath = allDirs.get(0)+"/"+FILE_PREFIX+"_"+(fileCounter++);

        if(filesPerDirCount >= FILES_PER_DIR) {
            allDirs.remove(0);
            filesPerDirCount = 0;
        }
        return filePath;
    }
}
