package com.gmail.benrcarver.distributed.workload;

import java.io.Serializable;

/**
 * @author Tianium
 */
public class BMOpStats implements Serializable {
    public final long OpStart;
    public final long OpDuration;

    public BMOpStats(long start, long duration) {
        this.OpStart = start;
        this.OpDuration = duration;
    }
}