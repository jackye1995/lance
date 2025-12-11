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

import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.parquet.Parquet;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.Blackhole;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** JMH benchmarks for Parquet format read/write operations using Iceberg's Parquet API. */
public class ParquetBenchmark extends FormatBenchmarkBase {

  private File parquetFile;
  private Schema icebergSchema;

  @Setup(Level.Trial)
  public void setup() throws IOException {
    super.setupBase();
    parquetFile = new File(tempDir, "parquet_benchmark.parquet");
    icebergSchema = BenchmarkDataGenerator.getIcebergSchema();
    writeParquetFile();
  }

  @TearDown(Level.Trial)
  public void teardown() {
    super.tearDownBase();
  }

  private void writeParquetFile() throws IOException {
    List<Record> records = dataGenerator.generateIcebergRecords(numRows);

    try (FileAppender<Record> appender =
        Parquet.write(Files.localOutput(parquetFile))
            .schema(icebergSchema)
            .createWriterFunc(GenericParquetWriter::buildWriter)
            .set("parquet.compression", getParquetCompression())
            .overwrite()
            .build()) {
      appender.addAll(records);
    }
  }

  private String getParquetCompression() {
    switch (compression) {
      case "SNAPPY":
        return "snappy";
      case "ZSTD":
        return "zstd";
      case "UNCOMPRESSED":
      default:
        return "uncompressed";
    }
  }

  @Benchmark
  public long readFullScan(Blackhole blackhole) throws IOException {
    long checksum = 0;

    try (CloseableIterable<Record> reader =
        Parquet.read(Files.localInput(parquetFile))
            .project(icebergSchema)
            .createReaderFunc(
                fileSchema -> GenericParquetReaders.buildReader(icebergSchema, fileSchema))
            .build()) {

      for (Record record : reader) {
        Long id = (Long) record.getField("id");
        checksum ^= id;
        blackhole.consume(record);
      }
    }

    return computeChecksum(checksum);
  }

  @Benchmark
  public long readRandomAccess(Blackhole blackhole) throws IOException {
    // Parquet doesn't support efficient random access by row index.
    // We simulate it by filtering with an IN predicate on id.
    // Note: This is NOT a fair comparison - Parquet has to scan the whole file.
    // This benchmark demonstrates Lance's advantage in random access workloads.

    Set<Long> targetIds = new HashSet<>(randomIndices);
    long checksum = 0;

    try (CloseableIterable<Record> reader =
        Parquet.read(Files.localInput(parquetFile))
            .project(icebergSchema)
            .createReaderFunc(
                fileSchema -> GenericParquetReaders.buildReader(icebergSchema, fileSchema))
            .build()) {

      for (Record record : reader) {
        Long id = (Long) record.getField("id");
        if (targetIds.contains(id)) {
          checksum ^= id;
          blackhole.consume(record);
        }
      }
    }

    return computeChecksum(checksum);
  }

  @Benchmark
  public void writeDataBenchmark(Blackhole blackhole) throws IOException {
    File benchWriteFile =
        new File(tempDir, "parquet_write_bench_" + System.nanoTime() + ".parquet");

    try {
      List<Record> records = dataGenerator.generateIcebergRecords(numRows);

      try (FileAppender<Record> appender =
          Parquet.write(Files.localOutput(benchWriteFile))
              .schema(icebergSchema)
              .createWriterFunc(GenericParquetWriter::buildWriter)
              .set("parquet.compression", getParquetCompression())
              .overwrite()
              .build()) {
        appender.addAll(records);
        blackhole.consume(appender.length());
      }
    } finally {
      benchWriteFile.delete();
    }
  }
}
