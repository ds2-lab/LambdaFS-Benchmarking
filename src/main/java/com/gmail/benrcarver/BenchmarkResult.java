package com.gmail.benrcarver;

import java.util.ArrayList;

/**
 * Encapsulates a result returned by a com.gmail.benrcarver.com.gmail.benrcarver.HopsFSClient during the benchmark.
 */
public class BenchmarkResult {
    /**
     * The timing results obtained via the benchmark.
     */
    private ArrayList<Double> timeResults;

    /**
     * Identifies the NameNode used to generate the timing results.
     */
    private String associatedNameNodeUri;

    public BenchmarkResult(String associatedNameNodeUri, ArrayList<Double> timeResults) {
        this.associatedNameNodeUri = associatedNameNodeUri;
        this.timeResults = timeResults;
    }

    public ArrayList<Double> getTimeResults() {
        return timeResults;
    }

    public String getAssociatedNameNodeUri() {
        return associatedNameNodeUri;
    }
}
