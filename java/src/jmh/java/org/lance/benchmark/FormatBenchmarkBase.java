/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lance.benchmark;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Base class for Lance vs Parquet format benchmarks. Provides common configuration and utilities.
 */
@Fork(1)
@State(Scope.Benchmark)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public abstract class FormatBenchmarkBase {

  /** Number of rows in the dataset. */
  @Param({"100000", "1000000"})
  protected int numRows;

  /** Compression codec to use. */
  @Param({"UNCOMPRESSED", "SNAPPY", "ZSTD"})
  protected String compression;

  /** Batch size for reading/writing. */
  protected static final int BATCH_SIZE = 10000;

  /** Number of random rows to access in random access benchmarks. */
  protected static final int RANDOM_ACCESS_SAMPLE_SIZE = 1000;

  /** Temporary directory for benchmark files. */
  protected File tempDir;

  /** Arrow buffer allocator. */
  protected BufferAllocator allocator;

  /** Data generator instance. */
  protected BenchmarkDataGenerator dataGenerator;

  /** Random indices for random access benchmarks. */
  protected List<Long> randomIndices;

  @Setup(Level.Trial)
  public void setupBase() throws IOException {
    tempDir = Files.createTempDirectory("lance-benchmark").toFile();
    allocator = new RootAllocator(Long.MAX_VALUE);
    dataGenerator = new BenchmarkDataGenerator(42);
    randomIndices = dataGenerator.generateRandomIndices(numRows, RANDOM_ACCESS_SAMPLE_SIZE);
  }

  @TearDown(Level.Trial)
  public void tearDownBase() {
    if (allocator != null) {
      allocator.close();
    }
    if (tempDir != null) {
      deleteDirectory(tempDir);
    }
  }

  /** Recursively deletes a directory. */
  protected static void deleteDirectory(File dir) {
    if (dir == null || !dir.exists()) {
      return;
    }
    File[] files = dir.listFiles();
    if (files != null) {
      for (File file : files) {
        if (file.isDirectory()) {
          deleteDirectory(file);
        } else {
          file.delete();
        }
      }
    }
    dir.delete();
  }

  /** Returns a checksum to prevent JIT optimization. */
  protected static long computeChecksum(long value) {
    return value ^ (value >>> 32);
  }
}
