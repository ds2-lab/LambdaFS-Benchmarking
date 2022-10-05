package com.gmail.benrcarver.distributed.workload.files;

public class FixeDepthFileTreeGenerator extends FileTreeGenerator {
    public FixeDepthFileTreeGenerator(String baseDir, int treeDepth) {
        super(baseDir, Integer.MAX_VALUE, Integer.MAX_VALUE, treeDepth);
    }
}