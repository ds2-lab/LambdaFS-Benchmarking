package com.gmail.benrcarver.distributed;

import org.apache.commons.lang3.StringUtils;

import java.util.List;

/**
 * Named `Local`Configuration to avoid conflict with HopsFS/HDFS Configuration class.
 */
public class LocalConfiguration {
    private String namenodeEndpoint;
    private List<FollowerConfig> followers;
    private String hdfsConfigFile;
    private boolean isServerless = true;
    private boolean commanderExecutesToo = true;
    private int maxNewSizeMb = 64000;
    private int maxHeapSizeGb = 96;
    private int minHeapSizeGb = 96;

    public boolean getIsServerless() { return isServerless; }
    public void setIsServerless(boolean isServerless) { this.isServerless = isServerless; }

    public String getNamenodeEndpoint() {
        return namenodeEndpoint;
    }

    public void setNamenodeEndpoint(String namenodeEndpoint) {
        this.namenodeEndpoint = namenodeEndpoint;
    }

    public List<FollowerConfig> getFollowers() {
        return followers;
    }

    public void setFollowers(List<FollowerConfig> followers) {
        this.followers = followers;
    }

    @Override
    public String toString() {
        return "LocalConfiguration(namenodeEndpoint=" + namenodeEndpoint + ", hdfsConfigFile=" + hdfsConfigFile +
                ", followers=" + StringUtils.join(followers, ",") + ")";
    }

    public String getHdfsConfigFile() {
        return hdfsConfigFile;
    }

    public void setHdfsConfigFile(String hdfsConfigFile) {
        this.hdfsConfigFile = hdfsConfigFile;
    }

    public boolean getCommanderExecutesToo() {
        return commanderExecutesToo;
    }

    public void setCommanderExecutesToo(boolean commanderExecutesToo) {
        this.commanderExecutesToo = commanderExecutesToo;
    }

    public int getMinHeapSizeGb() {
        return minHeapSizeGb;
    }

    public void setMinHeapSizeGb(int minHeapSizeGb) {
        this.minHeapSizeGb = minHeapSizeGb;
    }

    public int getMaxHeapSizeGb() {
        return maxHeapSizeGb;
    }

    public void setMaxHeapSizeGb(int maxHeapSizeGb) {
        this.maxHeapSizeGb = maxHeapSizeGb;
    }

    public int getMaxNewSizeMb() {
        return maxNewSizeMb;
    }

    public void setMaxNewSizeMb(int maxNewSizeMb) {
        this.maxNewSizeMb = maxNewSizeMb;
    }
}
