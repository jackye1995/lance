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

import org.lance.Dataset;
import org.lance.WriteParams;
import org.lance.ipc.LanceScanner;
import org.lance.ipc.ScanOptions;

import org.apache.arrow.c.ArrowArrayStream;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.Blackhole;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

/** JMH benchmarks for Lance format read/write operations. */
public class LanceBenchmark extends FormatBenchmarkBase {

  private File datasetPath;
  private Dataset dataset;

  @Setup(Level.Trial)
  public void setup() throws IOException {
    super.setupBase();
    datasetPath = new File(tempDir, "lance_benchmark.lance");
    writeDataset();
    dataset = Dataset.open(datasetPath.getAbsolutePath(), allocator);
  }

  @TearDown(Level.Trial)
  public void teardown() {
    if (dataset != null) {
      dataset.close();
    }
    super.tearDownBase();
  }

  private void writeDataset() {
    try (VectorSchemaRoot root = BenchmarkDataGenerator.createVectorSchemaRoot(allocator)) {
      int rowsWritten = 0;
      boolean first = true;

      while (rowsWritten < numRows) {
        int batchRows = Math.min(BATCH_SIZE, numRows - rowsWritten);
        dataGenerator.fillArrowBatch(root, rowsWritten, batchRows);

        try (ArrowArrayStream stream = ArrowArrayStream.allocateNew(allocator)) {
          Data.exportArrayStream(allocator, new SingleBatchReader(allocator, root), stream);

          WriteParams.Builder paramsBuilder =
              new WriteParams.Builder().withMaxRowsPerFile(100000).withMaxRowsPerGroup(10000);

          if (first) {
            paramsBuilder.withMode(WriteParams.WriteMode.CREATE);
            first = false;
          } else {
            paramsBuilder.withMode(WriteParams.WriteMode.APPEND);
          }

          try (Dataset ds =
              Dataset.create(
                  allocator, stream, datasetPath.getAbsolutePath(), paramsBuilder.build())) {
            // Dataset created/appended
          }
        }

        rowsWritten += batchRows;
      }
    }
  }

  @Benchmark
  public long readFullScan(Blackhole blackhole) throws Exception {
    long checksum = 0;
    ScanOptions scanOptions = new ScanOptions.Builder().batchSize(BATCH_SIZE).build();

    try (LanceScanner scanner = dataset.newScan(scanOptions);
        ArrowReader reader = scanner.scanBatches()) {
      while (reader.loadNextBatch()) {
        VectorSchemaRoot batch = reader.getVectorSchemaRoot();
        BigIntVector idVector = (BigIntVector) batch.getVector("id");
        for (int i = 0; i < batch.getRowCount(); i++) {
          checksum ^= idVector.get(i);
        }
        blackhole.consume(batch.getRowCount());
      }
    }

    return computeChecksum(checksum);
  }

  @Benchmark
  public long readRandomAccess(Blackhole blackhole) throws IOException {
    long checksum = 0;

    try (ArrowReader reader = dataset.take(randomIndices, Arrays.asList("id", "score"))) {
      while (reader.loadNextBatch()) {
        VectorSchemaRoot batch = reader.getVectorSchemaRoot();
        BigIntVector idVector = (BigIntVector) batch.getVector("id");
        for (int i = 0; i < batch.getRowCount(); i++) {
          checksum ^= idVector.get(i);
        }
        blackhole.consume(batch.getRowCount());
      }
    }

    return computeChecksum(checksum);
  }

  @Benchmark
  public void writeDataBenchmark(Blackhole blackhole) {
    File benchWritePath = new File(tempDir, "lance_write_bench_" + System.nanoTime() + ".lance");

    try (VectorSchemaRoot root = BenchmarkDataGenerator.createVectorSchemaRoot(allocator)) {
      int rowsWritten = 0;
      boolean first = true;

      while (rowsWritten < numRows) {
        int batchRows = Math.min(BATCH_SIZE, numRows - rowsWritten);
        dataGenerator.fillArrowBatch(root, rowsWritten, batchRows);

        try (ArrowArrayStream stream = ArrowArrayStream.allocateNew(allocator)) {
          Data.exportArrayStream(allocator, new SingleBatchReader(allocator, root), stream);

          WriteParams.Builder paramsBuilder =
              new WriteParams.Builder().withMaxRowsPerFile(100000).withMaxRowsPerGroup(10000);

          if (first) {
            paramsBuilder.withMode(WriteParams.WriteMode.CREATE);
            first = false;
          } else {
            paramsBuilder.withMode(WriteParams.WriteMode.APPEND);
          }

          try (Dataset ds =
              Dataset.create(
                  allocator, stream, benchWritePath.getAbsolutePath(), paramsBuilder.build())) {
            blackhole.consume(ds.countRows());
          }
        }

        rowsWritten += batchRows;
      }
    } finally {
      deleteDirectory(benchWritePath);
    }
  }

  /** Helper class to wrap a VectorSchemaRoot as an ArrowReader for a single batch. */
  private static class SingleBatchReader extends ArrowReader {
    private final VectorSchemaRoot root;
    private boolean consumed = false;

    SingleBatchReader(BufferAllocator allocator, VectorSchemaRoot root) {
      super(allocator, null);
      this.root = root;
    }

    @Override
    public boolean loadNextBatch() {
      if (consumed) {
        return false;
      }
      consumed = true;
      return true;
    }

    @Override
    public VectorSchemaRoot getVectorSchemaRoot() {
      return root;
    }

    @Override
    public long bytesRead() {
      return 0;
    }

    @Override
    protected void closeReadSource() {
      // Don't close the root, it's managed externally
    }

    @Override
    protected org.apache.arrow.vector.types.pojo.Schema readSchema() {
      return root.getSchema();
    }
  }
}
