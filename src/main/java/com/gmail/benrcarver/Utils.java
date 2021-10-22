package com.gmail.benrcarver;

import org.apache.hadoop.conf.Configuration;
import java.io.File;
import java.net.MalformedURLException;

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
}
