package com.gmail.benrcarver;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;

import java.io.*;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Utils {
    /**
     * Create and return an HDFS Configuration object with the hdfs-site.xml file added as a resource.
     */
    public static Configuration getConfiguration() {
        Configuration configuration = new Configuration();
        try {
            configuration.addResource(new File("/home/ubuntu/repos/hops/hadoop-dist/target/hadoop-3.2.0.3-SNAPSHOT/etc/hadoop/hdfs-site.xml").toURI().toURL());
        } catch (MalformedURLException ex) {
            ex.printStackTrace();
        }
        return configuration;
    }

    /**
     * Read a file containing HopsFS file paths. Return a list containing those paths.
     * @param path Path to file on local FS containing HopsFS file paths.
     * @return List of HopsFS file paths read in from the specified local file.
     */
    public static List<String> getFilePathsFromFile(String path) {
        List<String> filePaths = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(path))) {
            String line = br.readLine();

            while (line != null) {
                filePaths.add(line);
                line = br.readLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return filePaths;
    }

    public static void write(String filename, Object[] x) throws IOException {
        BufferedWriter outputWriter = null;
        outputWriter = new BufferedWriter(new FileWriter(filename));
        for (Object o : x) {
            outputWriter.write(o.toString());
            outputWriter.newLine();
        }
        outputWriter.flush();
        outputWriter.close();
    }

    /**
     * https://stackoverflow.com/a/39788851/5937661
     */
    public static Object[][] splitArray(Object[] arrayToSplit, int chunkSize){
        if(chunkSize<=0){
            return null;  // just in case :)
        }
        // first we have to check if the array can be split in multiple
        // arrays of equal 'chunk' size
        int rest = arrayToSplit.length % chunkSize;  // if rest>0 then our last array will have less elements than the others
        // then we check in how many arrays we can split our input array
        int chunks = arrayToSplit.length / chunkSize + (rest > 0 ? 1 : 0); // we may have to add an additional array for the 'rest'
        // now we know how many arrays we need and create our result array
        Object[][] arrays = new Object[chunks][];
        // we create our resulting arrays by copying the corresponding
        // part from the input array. If we have a rest (rest>0), then
        // the last array will have less elements than the others. This
        // needs to be handled separately, so we iterate 1 times less.
        for(int i = 0; i < (rest > 0 ? chunks - 1 : chunks); i++){
            // this copies 'chunk' times 'chunkSize' elements into a new array
            arrays[i] = Arrays.copyOfRange(arrayToSplit, i * chunkSize, i * chunkSize + chunkSize);
        }
        if(rest > 0){ // only when we have a rest
            // we copy the remaining elements into the last chunk
            arrays[chunks - 1] = Arrays.copyOfRange(arrayToSplit, (chunks - 1) * chunkSize, (chunks - 1) * chunkSize + rest);
        }
        return arrays; // that's it
    }

    /**
     * Randomly generate n strings of length l to be used as file contents during a write operation.
     *
     * @param n The number of files to be written. We'll generate this many strings.
     * @param l The length of each randomly-generated string.
     *
     * @return An array of size `numberOfFiles` containing randomly-generated strings of length `stringLength`.
     */
    public static String[] getFixedLengthRandomStrings(int n, int l) {
        String[] fileContents = new String[n];

        for (int i = 0; i < n; i++) {
            fileContents[i] = RandomStringUtils.randomAlphabetic(l);
        }

        return fileContents;
    }

    /**
     * Randomly generate n strings to be used as file contents during a write operation.
     *
     * @param numberOfFiles The number of files to be written. We'll generate this many strings.
     * @param minLength Minimum length of a randomly-generated string (inclusive).
     * @param maxLength Maximum length of a randomly-generated string (exclusive).
     *
     * @return An array of size `numberOfFiles` containing randomly-generated strings of length `stringLength`.
     */
    public static String[] getVariableLengthRandomStrings(int numberOfFiles, int minLength, int maxLength) {
        String[] fileContents = new String[numberOfFiles];

        for (int i = 0; i < numberOfFiles; i++) {
            fileContents[i] = RandomStringUtils.randomAlphabetic(minLength, maxLength);
        }

        return fileContents;
    }
}
