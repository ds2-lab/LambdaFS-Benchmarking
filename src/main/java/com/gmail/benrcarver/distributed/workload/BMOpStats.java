package com.gmail.benrcarver.distributed.workload;

import java.io.Serializable;

/**
 * @author Tianium
 */
public class BMOpStats implements Serializable {
    public long OpStart;
    public long OpDuration;

    public BMOpStats() { }

    public BMOpStats(long start, long duration) {
        this.OpStart = start;
        this.OpDuration = duration;
    }
}