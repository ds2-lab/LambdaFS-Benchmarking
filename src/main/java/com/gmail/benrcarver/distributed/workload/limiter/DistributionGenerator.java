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

import java.util.Random;

import com.gmail.benrcarver.distributed.coin.BMConfiguration;

/**
 *
 * @author Tianium
 */
public class DistributionGenerator {
  protected long seed;
  protected Random rand;

  public DistributionGenerator(BMConfiguration bmConf) {
    this.seed = bmConf.getBenchMarkRandomSeed();
    System.out.println("Confirmed seed used for bursty workload generator: " + seed);
    this.rand = new Random(seed);
  }

  public double get() {
    return 0.0;
  }

  public int getRandom() {
    return rand.nextInt();
  }

  public int getRandom(int min, int max) {
    return rand.nextInt(max - min) + min;
  }

  public double getProbability() {
    return rand.nextDouble();
  }

  public double getGaussian() {
    return rand.nextGaussian();
  }
}
