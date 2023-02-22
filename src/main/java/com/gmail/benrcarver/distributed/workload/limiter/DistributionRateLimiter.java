/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gmail.benrcarver.distributed.workload.limiter;

import com.gmail.benrcarver.distributed.Commander;
import com.gmail.benrcarver.distributed.coin.BMConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class DistributionRateLimiter implements WorkerRateLimiter {
  public static final Logger LOG = LoggerFactory.getLogger(DistributionRateLimiter.class);

  public static int OPS_BASE = 1000; // 1 s
  public static int OPS_INTERVAL = 10; // 10 ms

  protected int lenSlave = 1;
  protected DistributionGenerator generator;
  protected long startTime = 0;
  protected long duration = 0;
  protected long lastInterval = 0;
  protected final Semaphore semaphore = new Semaphore(0 , false);
  protected boolean closed = false;

  protected int unfulfilled = 0;
  protected int unfulfilledUnit = 0;
  protected int unfulfilledRemainder = 0;
  protected AtomicInteger completed;
  protected long lastCompleted;

  // Unit in seconds for an generated OPS settings will last.
  protected int opsUnit = 1;
  // Workload will start after skiped unit.
  protected int opsUnitSkiped = 0;

  private Writer throughputWriter;

  /**
   * DistributionRateLimiter constructor
   */
  public DistributionRateLimiter(BMConfiguration bmConf, DistributionGenerator distGenerator, int numWorkers) {
    try {
      this.lenSlave = numWorkers; // bmConf.getWork().size();
    } catch (Exception e) {
      // Ignore
    }
    this.generator = distGenerator;
    this.duration = bmConf.getInterleavedBmDuration();
    this.lastInterval = startTime - OPS_INTERVAL;
    this.opsUnit = bmConf.getInterleavedBMIaTUnit();
    this.opsUnitSkiped = bmConf.getInterleavedBMIaTSkipUnit();

    try {
      this.throughputWriter = new BufferedWriter(new OutputStreamWriter(
              new FileOutputStream("rand-workload-throughput.txt"), StandardCharsets.UTF_8));
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    }
  }

  public int getOPS() {
    return (int) (generator.get());
  }

  @Override
  public void setStart(long startTime) {
    this.startTime = startTime;
  }

  @Override
  public void setDuration(long duration) {
    this.duration = duration;
  }

  /**
   * Set the stat to be logged
   * @param key String Only "completed" is supported
   * @param val AtomicLong The number of completed operations
   */
  @Override
  public void setStat(String key, AtomicInteger val) {
    // Only completed is supported
    this.completed = val;
    this.lastCompleted = 0;
  }

  @Override
  public boolean checkRate() {
    if (closed) {
      return false;
    }
    try {
      semaphore.acquire();
      return true;
    } catch (InterruptedException ex) {
      return false;
    }
  }

  @Override
  public Object call() throws Exception {
    // Skip units
    while (opsUnitSkiped > 0) {
      getOPS();
      opsUnitSkiped--;
    }

    if (startTime == 0) {
      startTime = System.currentTimeMillis();
    }
    int lasts = 0;
    int lastOPS = 0;
    while (true) {
      long now = System.currentTimeMillis();
      if ((now - startTime) > duration) {
        closed = true;

        if (LOG.isDebugEnabled())
          LOG.debug("Experiment has completed. Releasing all " + semaphore.getQueueLength() + " waiting threads now.");
        // Release all waiting threads
        while(semaphore.hasQueuedThreads()) {
          semaphore.release();
        }
        return null;
      }

      long until = lastInterval + (long)OPS_INTERVAL - now;
      if (until <= 0) {
        if (unfulfilled <= 0) {
          // Generate new OPS after opsUnit.
          if (lasts <= 0) {
            lasts = opsUnit;
            lastOPS = getOPS() / lenSlave;
          }
          unfulfilled = lastOPS;
          lasts--;
          int numInterval = OPS_BASE / OPS_INTERVAL;
          unfulfilledUnit = unfulfilled / numInterval;
          unfulfilledRemainder = unfulfilled % numInterval;

          // Log every 1 second
          long c = completed.get();
          LOG.info("Completed: " +  (c - lastCompleted) + " Released: " + unfulfilled);

          if (this.throughputWriter != null) {
            this.throughputWriter.write("Completed: " +  (c - lastCompleted) + " Released: " + unfulfilled + "\n");
          }

          lastCompleted = c;
        }

        // Grant quota
        semaphore.release(unfulfilledUnit);
        unfulfilled -= unfulfilledUnit;
        if (unfulfilledRemainder > 0) {
          semaphore.release();
          unfulfilledRemainder--;
          unfulfilled--;
        }

        // Update interval to sleep
        lastInterval = System.currentTimeMillis();
        until = (long)OPS_INTERVAL;
      }

      TimeUnit.MILLISECONDS.sleep(until);
    }
  }
}
