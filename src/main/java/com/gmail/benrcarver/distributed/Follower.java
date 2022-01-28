package com.gmail.benrcarver.distributed;

import com.esotericsoftware.kryonet.Client;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;

/**
 * Runs HopsFS benchmarks as directed by a {@link Commander}.
 */
public class Follower {
    public static final Log LOG = LogFactory.getLog(Follower.class);

    private static final int CONN_TIMEOUT_MILLISECONDS = 5000;

    private final Client client;

    private final String masterIp;
    private final int masterPort;

    public Follower(String masterIp, int masterPort) {
        client = new Client();
        this.masterIp = masterIp;
        this.masterPort = masterPort;
    }

    public void connect() {
        Thread connectThread = new Thread(() -> {
            client.start();

            try {
                client.connect(CONN_TIMEOUT_MILLISECONDS, masterIp, masterPort);
            } catch (IOException e) {
                LOG.error("Failed to connect to Leader.", e);
            }
        });
        connectThread.start();

        try {
            connectThread.join();
        } catch (InterruptedException ex) {
            LOG.warn("InterruptedException encountered while trying to connect to HopsFS Client via TCP:", ex);
        }

        if (client.isConnected())
            LOG.info("Successfully connected to master at " + masterIp + ":" + masterPort + ".");
        else {
            LOG.error("Failed to connect to master at " + masterIp + ":" + masterPort + ".");
            System.exit(1);
        }
    }
}
