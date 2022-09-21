package com.gmail.benrcarver.distributed.workload.files;

import java.io.IOException;

public interface FilePool {
    public String getDirToCreate();

    public String getFileToCreate();

    public void fileCreationSucceeded(String file);

    public String getFileToRead();

    public String getFileToStat();

    public String getDirToStat();

    public String getFileToInfo();

    public String getDirToInfo();

    public String getFileToRename();

    public void fileRenamed(String from, String to);

    public String getFileToDelete();

    public boolean hasMoreFilesToWrite();

    public Object clone() throws CloneNotSupportedException;
}
