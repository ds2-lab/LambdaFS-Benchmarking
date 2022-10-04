/**
 * Copyright (c) 2010 Yahoo! Inc., 2016 YCSB contributors. All rights reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package com.gmail.benrcarver.distributed.workload.random;

import java.util.concurrent.ThreadLocalRandom;

/**
 * Utility functions.
 */
public final class Utils {
  private Utils() {
    // not used
  }

  /**
   * Hash an integer value.
   */
  public static long hash(long val) {
    return fnvhash64(val);
  }

  public static final long FNV_OFFSET_BASIS_64 = 0xCBF29CE484222325L;
  public static final long FNV_PRIME_64 = 1099511628211L;

  /**
   * 64 bit FNV hash. Produces more "random" hashes than (say) String.hashCode().
   *
   * @param val The value to hash.
   * @return The hash value
   */
  public static long fnvhash64(long val) {
    //from http://en.wikipedia.org/wiki/Fowler_Noll_Vo_hash
    long hashval = FNV_OFFSET_BASIS_64;

    for (int i = 0; i < 8; i++) {
      long octet = val & 0x00ff;
      val = val >> 8;

      hashval = hashval ^ octet;
      hashval = hashval * FNV_PRIME_64;
      //hashval = hashval ^ octet;
    }
    return Math.abs(hashval);
  }

  /**
   * Reads a big-endian 8-byte long from an offset in the given array.
   * @param bytes The array to read from.
   * @return A long integer.
   * @throws IndexOutOfBoundsException if the byte array is too small.
   * @throws NullPointerException if the byte array is null.
   */
  public static long bytesToLong(final byte[] bytes) {
    return (bytes[0] & 0xFFL) << 56
        | (bytes[1] & 0xFFL) << 48
        | (bytes[2] & 0xFFL) << 40
        | (bytes[3] & 0xFFL) << 32
        | (bytes[4] & 0xFFL) << 24
        | (bytes[5] & 0xFFL) << 16
        | (bytes[6] & 0xFFL) << 8
        | (bytes[7] & 0xFFL) << 0;
  }

  /**
   * Writes a big-endian 8-byte long at an offset in the given array.
   * @param val The value to encode.
   * @throws IndexOutOfBoundsException if the byte array is too small.
   */
  public static byte[] longToBytes(final long val) {
    final byte[] bytes = new byte[8];
    bytes[0] = (byte) (val >>> 56);
    bytes[1] = (byte) (val >>> 48);
    bytes[2] = (byte) (val >>> 40);
    bytes[3] = (byte) (val >>> 32);
    bytes[4] = (byte) (val >>> 24);
    bytes[5] = (byte) (val >>> 16);
    bytes[6] = (byte) (val >>> 8);
    bytes[7] = (byte) (val >>> 0);
    return bytes;
  }

  /**
   * Parses the byte array into a double.
   * The byte array must be at least 8 bytes long and have been encoded using
   * {@link #doubleToBytes}. If the array is longer than 8 bytes, only the
   * first 8 bytes are parsed.
   * @param bytes The byte array to parse, at least 8 bytes.
   * @return A double value read from the byte array.
   * @throws IllegalArgumentException if the byte array is not 8 bytes wide.
   */
  public static double bytesToDouble(final byte[] bytes) {
    if (bytes.length < 8) {
      throw new IllegalArgumentException("Byte array must be 8 bytes wide.");
    }
    return Double.longBitsToDouble(bytesToLong(bytes));
  }

  /**
   * Encodes the double value as an 8 byte array.
   * @param val The double value to encode.
   * @return A byte array of length 8.
   */
  public static byte[] doubleToBytes(final double val) {
    return longToBytes(Double.doubleToRawLongBits(val));
  }

  /**
   * Simple Fisher-Yates array shuffle to randomize discrete sets.
   * @param array The array to randomly shuffle.
   * @return The shuffled array.
   */
  public static <T> T [] shuffleArray(final T[] array) {
    for (int i = array.length -1; i > 0; i--) {
      final int idx = ThreadLocalRandom.current().nextInt(i + 1);
      final T temp = array[idx];
      array[idx] = array[i];
      array[i] = temp;
    }
    return array;
  }
}