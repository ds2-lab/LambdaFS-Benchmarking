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

    private String coreSiteConfigFile;
    private boolean isServerless = false;

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

    public String getCoreSiteConfigFile() {
        return coreSiteConfigFile;
    }

    public void setCoreSiteConfigFile(String coreSiteConfigFile) {
        this.coreSiteConfigFile = coreSiteConfigFile;
    }

    public String getHdfsConfigFile() {
        return hdfsConfigFile;
    }

    public void setHdfsConfigFile(String hdfsConfigFile) {
        this.hdfsConfigFile = hdfsConfigFile;
    }

    @Override
    public String toString() {
        return "LocalConfiguration(namenodeEndpoint=" + namenodeEndpoint + ", hdfsConfigFile=" + hdfsConfigFile +
                ", followers=" + StringUtils.join(followers, ",") + ")";
    }
}
