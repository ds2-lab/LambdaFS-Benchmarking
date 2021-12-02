package com.gmail.benrcarver.distributed;

/**
 * Controls a fleet of distributed machines. Executes HopsFS benchmarks based on user input/commands.
 */
public class Commander {
    /**
     * The TCP server used by the commander.
     */
    private final CommanderTcpServer commanderTcpServer;

    public Commander() {
        commanderTcpServer = new CommanderTcpServer();
    }
}
