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
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Types;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

/**
 * Generates ML-focused benchmark data with:
 *
 * <ul>
 *   <li>id: Long (primary key)
 *   <li>timestamp: Timestamp with timezone
 *   <li>label: String (category)
 *   <li>score: Double
 *   <li>embedding: FixedSizeList&lt;Float32&gt;[128] (vector column)
 * </ul>
 */
public class BenchmarkDataGenerator {

  public static final int EMBEDDING_DIM = 128;
  private static final String[] LABELS = {"cat", "dog", "bird", "fish", "car", "truck", "plane"};

  private final Random random;

  public BenchmarkDataGenerator() {
    this.random = new Random(42);
  }

  public BenchmarkDataGenerator(long seed) {
    this.random = new Random(seed);
  }

  /** Returns the Arrow schema for benchmark data. */
  public static Schema getArrowSchema() {
    Field embeddingElement =
        new Field(
            "item",
            FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)),
            null);
    Field embeddingField =
        new Field(
            "embedding",
            FieldType.nullable(new ArrowType.FixedSizeList(EMBEDDING_DIM)),
            Collections.singletonList(embeddingElement));

    return new Schema(
        Arrays.asList(
            Field.notNullable("id", new ArrowType.Int(64, true)),
            Field.nullable("timestamp", new ArrowType.Timestamp(TimeUnit.MICROSECOND, "UTC")),
            Field.nullable("label", ArrowType.Utf8.INSTANCE),
            Field.nullable("score", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
            embeddingField));
  }

  /** Returns the Iceberg schema for benchmark data. */
  public static org.apache.iceberg.Schema getIcebergSchema() {
    return new org.apache.iceberg.Schema(
        required(1, "id", Types.LongType.get()),
        optional(2, "timestamp", Types.TimestampType.withZone()),
        optional(3, "label", Types.StringType.get()),
        optional(4, "score", Types.DoubleType.get()),
        optional(5, "embedding", Types.ListType.ofOptional(6, Types.FloatType.get())));
  }

  /**
   * Fills a VectorSchemaRoot with generated data.
   *
   * @param root the VectorSchemaRoot to fill (must have the expected schema)
   * @param startId the starting ID for this batch
   * @param numRows the number of rows to generate
   */
  public void fillArrowBatch(VectorSchemaRoot root, long startId, int numRows) {
    root.allocateNew();

    BigIntVector idVector = (BigIntVector) root.getVector("id");
    TimeStampMicroTZVector timestampVector = (TimeStampMicroTZVector) root.getVector("timestamp");
    VarCharVector labelVector = (VarCharVector) root.getVector("label");
    Float8Vector scoreVector = (Float8Vector) root.getVector("score");
    FixedSizeListVector embeddingVector = (FixedSizeListVector) root.getVector("embedding");
    Float4Vector embeddingValues = (Float4Vector) embeddingVector.getDataVector();

    long baseTimestamp = System.currentTimeMillis() * 1000;

    for (int i = 0; i < numRows; i++) {
      long id = startId + i;
      idVector.setSafe(i, id);
      timestampVector.setSafe(i, baseTimestamp + id);
      labelVector.setSafe(i, LABELS[(int) (id % LABELS.length)].getBytes(StandardCharsets.UTF_8));
      scoreVector.setSafe(i, random.nextDouble());

      int embeddingStart = i * EMBEDDING_DIM;
      for (int j = 0; j < EMBEDDING_DIM; j++) {
        embeddingValues.setSafe(embeddingStart + j, random.nextFloat());
      }
      embeddingVector.setNotNull(i);
    }

    root.setRowCount(numRows);
  }

  /**
   * Creates a new VectorSchemaRoot with the benchmark schema.
   *
   * @param allocator the buffer allocator
   * @return a new VectorSchemaRoot
   */
  public static VectorSchemaRoot createVectorSchemaRoot(BufferAllocator allocator) {
    return VectorSchemaRoot.create(getArrowSchema(), allocator);
  }

  /**
   * Generates a list of Iceberg GenericRecords for benchmark data.
   *
   * @param numRows the number of rows to generate
   * @return a list of Iceberg Records
   */
  public List<Record> generateIcebergRecords(int numRows) {
    return generateIcebergRecords(0, numRows);
  }

  /**
   * Generates a list of Iceberg GenericRecords for benchmark data.
   *
   * @param startId the starting ID
   * @param numRows the number of rows to generate
   * @return a list of Iceberg Records
   */
  public List<Record> generateIcebergRecords(long startId, int numRows) {
    List<Record> records = new ArrayList<>(numRows);
    org.apache.iceberg.Schema schema = getIcebergSchema();
    long baseTimestamp = System.currentTimeMillis() * 1000;

    for (int i = 0; i < numRows; i++) {
      long id = startId + i;
      GenericRecord record = GenericRecord.create(schema);
      record.setField("id", id);
      record.setField("timestamp", baseTimestamp + id);
      record.setField("label", LABELS[(int) (id % LABELS.length)]);
      record.setField("score", random.nextDouble());

      List<Float> embedding = new ArrayList<>(EMBEDDING_DIM);
      for (int j = 0; j < EMBEDDING_DIM; j++) {
        embedding.add(random.nextFloat());
      }
      record.setField("embedding", embedding);

      records.add(record);
    }

    return records;
  }

  /**
   * Generates random row indices for random access benchmarks.
   *
   * @param totalRows the total number of rows in the dataset
   * @param sampleSize the number of random indices to generate
   * @return a list of random row indices
   */
  public List<Long> generateRandomIndices(long totalRows, int sampleSize) {
    List<Long> indices = new ArrayList<>(sampleSize);
    for (int i = 0; i < sampleSize; i++) {
      indices.add((long) (random.nextDouble() * totalRows));
    }
    return indices;
  }
}
