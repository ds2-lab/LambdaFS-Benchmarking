package com.gmail.benrcarver.distributed.workload;

/**
 *
 * @author Tianium
 */
public enum BenchmarkDistribution {

    UNIFORM   ("UNIFORM"),
    PARETO    ("PARETO"),
    POISSON   ("POISSON"),
    ZIPF      ("ZIPF");

    private final String distribution;
    private BenchmarkDistribution(String distribution){
        this.distribution = distribution;
    }

    public boolean equals(BenchmarkDistribution otherName){
        return (otherName == null)? false:distribution.equals(otherName.toString());
    }

    public String toString(){
        return distribution;
    }
}