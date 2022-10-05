package com.gmail.benrcarver.distributed.workload.files;

import com.gmail.benrcarver.distributed.Commander;
import com.gmail.benrcarver.distributed.coin.FileSizeMultiFaceCoin;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

public class FileTreeGenerator implements FilePool {
    public static final Logger LOG = LoggerFactory.getLogger(FileTreeGenerator.class);
    private final Random rand1;
    protected List<String> allThreadFiles;
    protected List<String> allThreadDirs;
    protected String threadDir;
    private NameSpaceGenerator nameSpaceGenerator;
    private final int THRESHOLD = 3;
    private int currIndex = -1;
    private long currentFileSize = -1;
    private long currentFileDataRead = -1;

    public static String[] getPathNames(String path) {
        if (path != null && path.startsWith("/")) {
            return split(path, '/');
        } else {
            throw new AssertionError("Absolute path required");
        }
    }

    public static String[] split(String str, char separator) {
        if (str.isEmpty()) {
            return new String[]{""};
        } else {
            ArrayList<String> strList = new ArrayList();
            int startIndex = 0;

            int nextIndex;
            for(boolean var4 = false; (nextIndex = str.indexOf(separator, startIndex)) != -1; startIndex = nextIndex + 1) {
                strList.add(str.substring(startIndex, nextIndex));
            }

            strList.add(str.substring(startIndex));
            int last = strList.size();

            while(true) {
                --last;
                if (last < 0 || !"".equals(strList.get(last))) {
                    return (String[])strList.toArray(new String[strList.size()]);
                }

                strList.remove(last);
            }
        }
    }

    public FileTreeGenerator(String baseDir, int filesPerDir,
                             int dirPerDir, int initialTreeDepth) {

        this.allThreadFiles = new ArrayList<String>(10000);
        this.allThreadDirs = new ArrayList<String>(10000);
        this.rand1 = new Random(System.currentTimeMillis());
        UUID uuid = UUID.randomUUID();

        String machineName = "";
        try {
            machineName = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            machineName = "Client_Machine+" + rand1.nextInt();
        }

        baseDir = baseDir.trim();
        if (!baseDir.endsWith("/")) {
            baseDir = baseDir + "/";
        }

        if(baseDir.compareTo("/")==0){
            threadDir = baseDir + machineName+"_"+ uuid;
        }else{
            threadDir = baseDir + machineName+"/"+ uuid;
        }

        String[] comp = getPathNames(threadDir);

        if (initialTreeDepth - comp.length > 0) {
            for (int i = comp.length; i < (initialTreeDepth); i++) {
                threadDir += "/added_depth_" + i;
            }
        }

        nameSpaceGenerator = new NameSpaceGenerator(threadDir, filesPerDir, dirPerDir);
    }

    @Override
    public String getDirToCreate() {
        String path = nameSpaceGenerator.generateNewDirPath();
        allThreadDirs.add(path);
        return path;
    }

    @Override
    public String getFileToCreate() {
        String path = nameSpaceGenerator.getFileToCreate();
        return path;
    }

    @Override
    public void fileCreationSucceeded(String file) {
        allThreadFiles.add(file);
    }

    @Override
    public String getFileToRead() {
        return getRandomFile();
    }

    @Override
    public String getFileToRename() {
        if (allThreadFiles.isEmpty()) {
            return null;
        }

        for (int i = 0; i < allThreadFiles.size(); i++) {
            currIndex = rand1.nextInt(allThreadFiles.size());
            String path = allThreadFiles.get(currIndex);
            if (getPathLength(path) < THRESHOLD) {
                continue;
            }
            return path;
        }

        return null;
    }

    @Override
    public void fileRenamed(String previousName, String newName) {
        String currentFile = allThreadFiles.get(currIndex);
        if (!currentFile.equals(previousName))
            throw new IllegalStateException("Renamed file with old name '" + previousName +
                    "' not found. New name is '" + newName + "'. Current name is: '" + currentFile + "'");
        allThreadFiles.set(currIndex, newName);
    }

    @Override
    public String getFileToDelete() {
        if (allThreadFiles.isEmpty()) {
            return null;
        }

        currIndex = allThreadFiles.size()-1;
        String fileToDelete = allThreadFiles.remove(currIndex);

        if (LOG.isDebugEnabled())
            LOG.debug("Returning file '" + fileToDelete + "' for deletion. File pool size: " + allThreadFiles.size());

        return fileToDelete;

    }

    @Override
    public String getDirToStat() {
        return getRandomDir();
    }

    @Override
    public String getFileToStat() {
        return getRandomFile();
    }

    @Override
    public String getFileToInfo() {
        return getRandomFile();
    }

    @Override
    public String getDirToInfo() {
        return getRandomDir();
    }

    @Override
    public boolean hasMoreFilesToWrite(){
        return true;
    }

    @Override
    public Object clone() throws CloneNotSupportedException {
        return null;
    }


    private String getRandomFile() {
        if (!allThreadFiles.isEmpty()) {
            for (int i = 0; i < allThreadFiles.size(); i++) {
                currIndex = rand1.nextInt(allThreadFiles.size());
                String path = allThreadFiles.get(currIndex);
                if (getPathLength(path) < THRESHOLD) {
                    continue;
                }
//        System.out.println("Path "+path);
                return path;
            }
        }

        LOG.error("Error: Unable to getRandomFile from file pool: "+this+" PoolSize is: "+allThreadFiles.size());
        return null;
    }

    private int getPathLength(String path){
//    return PathUtils.getPathNames(path).length;
        return StringUtils.countMatches(path,"/");
    }

    public String getRandomDir() {
        if (!allThreadFiles.isEmpty()) {
            for (int i = 0; i < allThreadFiles.size(); i++) {
                currIndex = rand1.nextInt(allThreadFiles.size());
                String path = allThreadFiles.get(currIndex);
                int dirIndex = path.lastIndexOf("/");
                path = path.substring(0, dirIndex);
                if (getPathLength(path) < THRESHOLD) {
                    continue;
                }
//        System.out.println("Path "+path+ " after retires: "+i);
                return path;
            }
        }

        LOG.error("Error: Unable to getRandomDir from file pool: "+this+" PoolSize is: "+allThreadFiles.size());
        return null;
    }

}
