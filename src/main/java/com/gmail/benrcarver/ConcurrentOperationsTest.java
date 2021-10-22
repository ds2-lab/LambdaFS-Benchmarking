package com.gmail.benrcarver;

import com.google.gson.JsonObject;
import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.fs.FileStatus;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class ConcurrentOperationsTest {
    private Reader[] readers;

    private Writer[] writers;

    public ConcurrentOperationsTest(Reader[] readers, Writer[] writers) {
        this.readers = readers;
        this.writers = writers;
    }

    public void doTest() {
        Thread[] readerThreads = new Thread[readers.length];
        Thread[] writerThreads = new Thread[writers.length];

        for (int i = 0; i < readers.length; i++) {
            Thread readerThread = new Thread(readers[i]);
            readerThreads[i] = readerThread;
        }

        for (int i = 0; i < writers.length; i++) {
            Thread writerThread = new Thread(writers[i]);
            writerThreads[i] = writerThread;
        }

        int maxLength = Math.max(readerThreads.length, writerThreads.length);
        for (int i = 0; i < maxLength; i++) {
            if (i < readerThreads.length)
                readerThreads[i].start();

            if (i < writers.length)
                writerThreads[i].start();
        }

        for (int i = 0; i < maxLength; i++) {
            if (i < readerThreads.length) {
                try {
                    readerThreads[i].join();
                } catch (InterruptedException ex) {
                    ex.printStackTrace();
                }
            }

            if (i < writers.length) {
                try {
                    writerThreads[i].join();
                } catch (InterruptedException ex) {
                    ex.printStackTrace();
                }
            }
        }
    }

    public class Reader implements Runnable {
        private final String[] filePaths;
        private final int id;

        public Reader(int id, String[] filePaths) {
            this.filePaths = filePaths;
            this.id = id;
        }

        private void readFile(String filePath, DistributedFileSystem hdfs) {
            Path path = new Path("hdfs://10.241.64.14:9000/" + filePath);

            try {
                FSDataInputStream inputStream = hdfs.open(path);
                BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
                String line = null;
                while ((line = br.readLine()) != null)
                    System.out.println("[Reader " + id + "] " + line);

                inputStream.close();
                br.close();
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }

        @Override
        public void run() {
            DistributedFileSystem hdfs = connect("hdfs://10.241.64.14:9000");

            for (int i = 0; i < filePaths.length; i++) {
                String filePath = filePaths[i];
                readFile(filePath, hdfs);

                try {
                    Thread.sleep(10);
                } catch (InterruptedException ex) {
                    ex.printStackTrace();
                }
            }
        }
    }

    public class Writer implements Runnable {
        private final String[] filePaths;
        private final String[] fileContents;
        private final int id;

        public Writer(int id, String[] filePaths, String[] fileContents) {
            assert(filePaths.length == fileContents.length);

            this.filePaths = filePaths;
            this.fileContents = fileContents;
            this.id = id;
        }

        private void createAndWriteFile(String filePath, String fileContent, DistributedFileSystem hdfs) {
            Path path = new Path("hdfs://10.241.64.14:9000/" + filePath);

            try {
                System.out.println("[Writer " + id + "] Creating file \"" + path + "\" with contents \""
                        + fileContent + "\"...");

                FSDataOutputStream outputStream = hdfs.create(path);
                System.out.println("[Writer " + id + "] Called create() successfully.");

                BufferedWriter br = new BufferedWriter(new OutputStreamWriter(outputStream, "UTF-8"));
                System.out.println("[Writer " + id + "] Created BufferedWriter object.");

                br.write(fileContent);
                System.out.println("[Writer " + id + "] Wrote \"" + fileContent + "\" using BufferedWriter.");

                br.close();
                System.out.println("[Writer " + id + "] Closed BufferedWriter.");
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }

        @Override
        public void run() {
            DistributedFileSystem hdfs = connect("hdfs://10.241.64.14:9000");

            for (int i = 0; i < filePaths.length; i++) {
                String filePath = filePaths[i];
                String fileContent = fileContents[i];

                createAndWriteFile(filePath, fileContent, hdfs);

                try {
                    Thread.sleep(10);
                } catch (InterruptedException ex) {
                    ex.printStackTrace();
                }
            }
        }
    }

    /**
     * Connect to the HopsFS cluster.
     */
    public static DistributedFileSystem connect(String endpoint) {
        System.out.println("Starting HdfsTest now.");
        Configuration configuration = Utils.getConfiguration();
        System.out.println("Created configuration.");
        DistributedFileSystem hdfs = new DistributedFileSystem();
        System.out.println("Created DistributedFileSystem object.");

        try {
            hdfs.initialize(new URI(endpoint), configuration);
            System.out.println("Called initialize() successfully.");
        } catch (URISyntaxException | IOException ex) {
            ex.printStackTrace();
        }

        return hdfs;
    }

    public static void main(String[] args) {

    }
}
