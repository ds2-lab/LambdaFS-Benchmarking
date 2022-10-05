package com.gmail.benrcarver.distributed.workload.files;

import com.gmail.benrcarver.distributed.util.Utils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

public class ExistingSubtreeFileGenerator implements FilePool {
    public static final Log LOG = LogFactory.getLog(ExistingSubtreeFileGenerator.class);

    /**
     * The base name used for randomly-generated directories.
     */
    public static final String DIR_NAME = "DirLambda";

    /**
     * The base name used for randomly-generated files.
     */
    public static final String FILE_NAME = "FileLambda";

    /**
     * All the files in this pool.
     */
    protected List<String> filesInPool;

    /**
     * The directories in this pool. These are disjoint from the {@code baseDirectories} set of directories.
     */
    protected List<String> directoriesInPool;

    /**
     * The directories of the starting subtree.
     *
     * We do not modify these, but we can create children within these directories.
     */
    protected List<String> baseDirectories;

    private final Random random;

    /**
     * Keeps track of the number of directories we've created.
     * Used when creating names for randomly-generated directories.
     */
    private int numDirectoriesCreated;

    /**
     * Keeps track of the number of directories we've created.
     * Used when creating names for randomly-generated files.
     */
    private int numFilesCreated;

    /**
     * Index of the file we're modifying.
     */
    private int lastModifiedIndex = 0;

    /**
     * Directories that will be created when a `create file` operation succeeds.
     * We wait until that op succeeds before adding the directories to the pool of directories.
     */
    protected HashMap<String, String> dirsToBeCreated = new HashMap<>();

    /**
     * @param pathToInitialDirectories Path to a file on-disk that contains all the initial directories
     *                                 to add to this file pool.
     */
    public ExistingSubtreeFileGenerator(String pathToInitialDirectories) throws FileNotFoundException {
        this.baseDirectories = Utils.getFilePathsFromFile(pathToInitialDirectories);
        this.filesInPool = new ArrayList<>();
        this.directoriesInPool = new ArrayList<>();
        this.random = new Random();
    }

    /**
     * Return a random directory from the initial subtree.
     */
    private String getRandomBaseDirectory() {
        int idx = random.nextInt(baseDirectories.size());
        return baseDirectories.get(idx);
    }

    /**
     * Return a random, existing file.
     */
    private String getRandomFile() {
        if (!filesInPool.isEmpty()) {
            int idx = random.nextInt(filesInPool.size());
            return filesInPool.get(idx);
        }

        LOG.error("Unable to getRandomFile from file pool: " + this + ".");
        return null;
    }

    /**
     * Return a random directory from all directories we've created and the base subtree.
     */
    private String getRandomDirectory() {
        // Select a directory from both the base subtree and any directories we've since created.
        int idx = random.nextInt(baseDirectories.size() + directoriesInPool.size() - 1);

        // If the index is greater than the size of the base subtree, then we're picking a directory we've generated.
        if (idx >= baseDirectories.size())
            return directoriesInPool.get(idx - baseDirectories.size());

        return baseDirectories.get(idx);
    }

    /**
     * Randomly pick and return a directory from the set of directories created during this workload.
     */
    private String getRandomNonExistingSubtreeDirectory() {
        int idx = random.nextInt(directoriesInPool.size());
        return directoriesInPool.get(idx);
    }

    /**
     * Generates a random directory name of the form:
     * [random-existing-directory]/DirLambda-[random-6-char-string]-[num-dirs-created]
     */
    @Override
    public String getDirToCreate() {
        return getDirToCreate(true);
    }

    private String getDirToCreate(boolean addToPool) {
        // All of these should contain the trailing '/' symbol.
        String directory = getRandomDirectory() + "/" + DIR_NAME + "-" + RandomStringUtils.randomAlphabetic(6) +
                "-" + numDirectoriesCreated++;
        if (addToPool)
            directoriesInPool.add(directory);
        return directory;
    }

    /**
     * Generates a random file name of the form:
     * [random-existing-directory]/FileLambda-[random-6-char-string]-[num-files-created]
     */
    @Override
    public String getFileToCreate() {
        // Create a directory if there aren't any.
        String newDir = null;
        // This ensures that each thread creates at least one new directory.
        // At first, there are no directories in the pool, so we always create one.
        // Then, we randomly create files across the entire subtree.
        if (directoriesInPool.isEmpty()) {
            newDir = getDirToCreate(false);
        }

        if (newDir != null) {
            String newFile = newDir + "/" + FILE_NAME + RandomStringUtils.randomAlphabetic(6) + numFilesCreated++;
            dirsToBeCreated.put(newFile, newDir);
            return newFile;
        }
        else {
            return getRandomDirectory() + "/" + FILE_NAME +
                    RandomStringUtils.randomAlphabetic(6) + numFilesCreated++;
        }
    }

    @Override
    public void fileCreationSucceeded(String file) {
        if (LOG.isDebugEnabled())
            LOG.debug("Successfully created file: '" + file + "'");

        filesInPool.add(file);

        if (dirsToBeCreated.containsKey(file)) {
            String dir = dirsToBeCreated.remove(file);
            directoriesInPool.add(dir);
        }
    }

    @Override
    public String getFileToRead() {
        return getRandomFile();
    }

    @Override
    public String getFileToStat() {
        return getRandomFile();
    }

    @Override
    public String getDirToStat() {
        return getRandomNonExistingSubtreeDirectory();
    }

    @Override
    public String getFileToInfo() {
        return getRandomFile();
    }

    @Override
    public String getDirToInfo() {
        return getRandomDirectory();
    }

    @Override
    public String getFileToRename() {
        if (filesInPool.isEmpty()) {
            return null;
        }

        lastModifiedIndex = random.nextInt(filesInPool.size());
        return filesInPool.get(lastModifiedIndex);
    }

    @Override
    public void fileRenamed(String previousName, String newName) {
        String currentFile = filesInPool.get(lastModifiedIndex);

        if (!currentFile.equals(previousName))
            throw new IllegalStateException("Renamed file with old name '" + previousName +
                    "' not found. New name is " + newName);

        filesInPool.set(lastModifiedIndex, newName);
    }

    @Override
    public String getFileToDelete() {
        if (filesInPool.isEmpty()) {
            return null;
        }

        lastModifiedIndex = filesInPool.size() - 1;
        return filesInPool.remove(lastModifiedIndex);
    }

    @Override
    public boolean hasMoreFilesToWrite() {
        return true;
    }

    @Override
    public String toString() {
        return "ExistingSubtreeFileGenerator(numBaseDirs=" + baseDirectories.size() + ", numGenDirs=" +
                directoriesInPool.size() + ", numGenFiles=" + filesInPool.size() + ")";
    }
}
