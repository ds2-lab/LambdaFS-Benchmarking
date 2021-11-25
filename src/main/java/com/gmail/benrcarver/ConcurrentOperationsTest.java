package com.gmail.benrcarver;

import com.google.gson.JsonObject;
import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.fs.FileStatus;
import io.hops.metrics.OperationPerformed;
import org.yaml.snakeyaml.Yaml;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

public class ConcurrentOperationsTest {
    private List<Reader> readers;

    private List<Writer> writers;

    private ConcurrentLinkedQueue<String> filesCreated;

    private ConcurrentLinkedQueue<OperationPerformed> operationsPerformed;

    public ConcurrentOperationsTest(List<Reader> readers, List<Writer>  writers) {
        this.readers = readers;
        this.writers = writers;
        this.filesCreated = new ConcurrentLinkedQueue<>();
        this.operationsPerformed = new ConcurrentLinkedQueue<OperationPerformed>();
    }

    /**
     * Add the list of operations to the queue.
     */
    public void addOperationsPerformed(List<OperationPerformed> ops) {
        this.operationsPerformed.addAll(ops);
    }

    public void cleanUpWrittenFiles() throws IOException {
        System.out.println("Cleaning up " + filesCreated.size() + " file(s) created during test.");

        DistributedFileSystem hdfs = connect("hdfs://10.241.64.14:9000");

        while (filesCreated.size() > 0) {
            String targetPath = filesCreated.poll();

            System.out.println("Deleting file: " + targetPath);

            Path filePath = new Path("hdfs://10.241.64.14:9000/" + targetPath);

            try {
                boolean success = hdfs.delete(filePath, true);
                System.out.println("\t Delete was successful: " + success);
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }

        hdfs.printDebugInformation();
        hdfs.printOperationsPerformed();

        hdfs.close();
    }

    /**
     * Add a file to the `filesCreated` queue.
     */
    public void addFileWritten(String filePath) {
        this.filesCreated.add(filePath);
    }

    public void doTest() {
        Thread[] readerThreads = new Thread[readers.size()];
        Thread[] writerThreads = new Thread[writers.size()];

        for (int i = 0; i < readers.size(); i++) {
            Thread readerThread = new Thread(readers.get(i));
            readerThreads[i] = readerThread;
        }

        for (int i = 0; i < writers.size(); i++) {
            Thread writerThread = new Thread(writers.get(i));
            writerThreads[i] = writerThread;
        }


        Instant startTime = Instant.now();
        int maxLength = Math.max(readerThreads.length, writerThreads.length);
        for (int i = 0; i < maxLength; i++) {
            if (i < readerThreads.length)
                readerThreads[i].start();

            if (i < writers.size())
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

            if (i < writers.size()) {
                try {
                    writerThreads[i].join();
                } catch (InterruptedException ex) {
                    ex.printStackTrace();
                }
            }
        }

        Instant endTime = Instant.now();
        Duration testDuration = Duration.between(startTime, endTime);
        System.out.println("\n\n\nTEST COMPLETED. TIME ELAPSED: " + testDuration.toString()
                + ". CLEANING UP FILES NOW.");
        try {
            cleanUpWrittenFiles();
        } catch (IOException ex) {
            System.out.println("Encountered IOException while cleaning up the written files.");
            ex.printStackTrace();
        }

        List<OperationPerformed> opsPerformedList = new ArrayList<OperationPerformed>(operationsPerformed);
        Collections.sort(opsPerformedList, OperationPerformed.BY_START_TIME);

        System.out.println("====================== Operations Performed ======================");
        System.out.println("Number performed: " + operationsPerformed.size());
        for (OperationPerformed operationPerformed : opsPerformedList)
            System.out.println(operationPerformed.toString());
        System.out.println("==================================================================");
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

    public static void oneReaderOneWriter() {
        Reader[] readers = new Reader[1];
        Writer[] writers = new Writer[1];
    }

    public static void main(String[] args) throws FileNotFoundException {
        String testYaml = args[0];

        System.out.println("Test YAML file: " + testYaml);

        Yaml yaml = new Yaml();
        InputStream inputStream = new FileInputStream(new File(testYaml));
        //ConcurrentOperationsTest.class.getResourceAsStream(testYaml);

        Map<String, Object> obj = yaml.load(inputStream);

        System.out.println(obj);

        List<Object> readerObjects;
        if (obj.containsKey("readers"))
            readerObjects = (List<Object>)obj.get("readers");
        else
            readerObjects = new ArrayList<>();

        List<Object> writerObjects;
        if (obj.containsKey("writers"))
            writerObjects = (List<Object>)obj.get("writers");
        else
            writerObjects = new ArrayList<>();

        List<Reader> readers = new ArrayList<Reader>();
        List<Writer> writers = new ArrayList<Writer>();

        ConcurrentOperationsTest concurrentOperationsTest =
                new ConcurrentOperationsTest(readers, writers);

        for (Object readerObject : readerObjects) {
            Map<String, Object> readerMap = (Map<String, Object>)readerObject;
            System.out.println("Reader: " + readerMap.toString());
            int id = (Integer)readerMap.get("id");
            List<String> paths = (List<String>)readerMap.get("paths");

            Reader reader = new Reader(id, paths.toArray(new String[0]), concurrentOperationsTest);
            readers.add(reader);
        }

        for (Object writerObject : writerObjects) {
            Map<String, Object> writerMap = (Map<String, Object>)writerObject;
            System.out.println("Reader: " + writerMap.toString());
            int id = (Integer)writerMap.get("id");
            List<String> paths = (List<String>)writerMap.get("paths");
            List<String> contents = (List<String>)writerMap.get("contents");

            Writer writer = new Writer(id, paths.toArray(new String[0]), contents.toArray(new String[0]),
                    concurrentOperationsTest);
            writers.add(writer);
        }

        concurrentOperationsTest.doTest();
    }
}

class Writer implements Runnable {
    private final String[] filePaths;
    private final String[] fileContents;
    private final int id;
    private final ConcurrentOperationsTest testObject;

    public Writer(int id, String[] filePaths, String[] fileContents, ConcurrentOperationsTest testObject) {
        assert(filePaths.length == fileContents.length);

        this.filePaths = filePaths;
        this.fileContents = fileContents;
        this.id = id;
        this.testObject = testObject;
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

            this.testObject.addFileWritten(filePath);
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    @Override
    public void run() {
        DistributedFileSystem hdfs = ConcurrentOperationsTest.connect("hdfs://10.241.64.14:9000");

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

        hdfs.printDebugInformation();
        hdfs.printOperationsPerformed();
        List<OperationPerformed> ops = hdfs.getOperationsPerformed();
        this.testObject.addOperationsPerformed(ops);

        try {
            hdfs.close();
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }
}

class Reader implements Runnable {
    private final String[] filePaths;
    private final int id;
    private final ConcurrentOperationsTest testObject;

    public Reader(int id, String[] filePaths, ConcurrentOperationsTest testObject) {
        this.filePaths = filePaths;
        this.id = id;
        this.testObject = testObject;
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
        DistributedFileSystem hdfs = ConcurrentOperationsTest.connect("hdfs://10.241.64.14:9000");

        for (int i = 0; i < filePaths.length; i++) {
            String filePath = filePaths[i];
            readFile(filePath, hdfs);

            try {
                Thread.sleep(10);
            } catch (InterruptedException ex) {
                ex.printStackTrace();
            }
        }

        hdfs.printDebugInformation();
        hdfs.printOperationsPerformed();
        List<OperationPerformed> ops = hdfs.getOperationsPerformed();
        this.testObject.addOperationsPerformed(ops);

        try {
            hdfs.close();
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }
}
