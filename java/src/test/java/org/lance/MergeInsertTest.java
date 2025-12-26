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
package org.lance;

import org.lance.merge.MergeInsertParams;
import org.lance.merge.MergeInsertResult;

import org.apache.arrow.c.ArrowArrayStream;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

public class MergeInsertTest {
  @TempDir private Path tempDir;
  private RootAllocator allocator;
  private TestUtils.SimpleTestDataset testDataset;
  private Dataset dataset;

  @BeforeEach
  public void setup() {
    String datasetPath = tempDir.resolve(UUID.randomUUID().toString()).toString();
    allocator = new RootAllocator(Long.MAX_VALUE);
    testDataset = new TestUtils.SimpleTestDataset(allocator, datasetPath);
    testDataset.createEmptyDataset().close();
    dataset = testDataset.write(1, 5);
  }

  @AfterEach
  public void tearDown() {
    dataset.close();
    allocator.close();
  }

  @Test
  public void testWhenNotMatchedInsertAll() throws Exception {
    // Test insert all unmatched source rows

    try (VectorSchemaRoot source = buildSource(testDataset.getSchema(), allocator)) {
      try (ArrowArrayStream sourceStream = convertToStream(source, allocator)) {
        MergeInsertResult result =
            dataset.mergeInsert(
                new MergeInsertParams(Collections.singletonList("id")), sourceStream);

        Assertions.assertEquals(
            "{0=Person 0, 1=Person 1, 2=Person 2, 3=Person 3, 4=Person 4, 7=Source 7, 8=Source 8, 9=Source 9}",
            readAll(result.dataset()).toString());
      }
    }
  }

  @Test
  public void testWhenNotMatchedDoNothing() throws Exception {
    // Test ignore unmatched source rows

    try (VectorSchemaRoot source = buildSource(testDataset.getSchema(), allocator)) {
      try (ArrowArrayStream sourceStream = convertToStream(source, allocator)) {
        MergeInsertResult result =
            dataset.mergeInsert(
                new MergeInsertParams(Collections.singletonList("id"))
                    .withMatchedUpdateAll()
                    .withNotMatched(MergeInsertParams.WhenNotMatched.DoNothing),
                sourceStream);

        Assertions.assertEquals(
            "{0=Source 0, 1=Source 1, 2=Source 2, 3=Person 3, 4=Person 4}",
            readAll(result.dataset()).toString());
      }
    }
  }

  @Test
  public void testWhenMatchedUpdateIf() throws Exception {
    // Test update matched rows if expression is true

    try (VectorSchemaRoot source = buildSource(testDataset.getSchema(), allocator)) {
      try (ArrowArrayStream sourceStream = convertToStream(source, allocator)) {
        MergeInsertResult result =
            dataset.mergeInsert(
                new MergeInsertParams(Collections.singletonList("id"))
                    .withMatchedUpdateIf("target.name = 'Person 0' or target.name = 'Person 1'")
                    .withNotMatched(MergeInsertParams.WhenNotMatched.DoNothing),
                sourceStream);

        Assertions.assertEquals(
            "{0=Source 0, 1=Source 1, 2=Person 2, 3=Person 3, 4=Person 4}",
            readAll(result.dataset()).toString());
      }
    }
  }

  @Test
  public void testWhenNotMatchedBySourceDelete() throws Exception {
    // Test delete target rows which are not matched with source.

    try (VectorSchemaRoot source = buildSource(testDataset.getSchema(), allocator)) {
      try (ArrowArrayStream sourceStream = convertToStream(source, allocator)) {
        MergeInsertResult result =
            dataset.mergeInsert(
                new MergeInsertParams(Collections.singletonList("id"))
                    .withNotMatchedBySourceDelete()
                    .withNotMatched(MergeInsertParams.WhenNotMatched.DoNothing),
                sourceStream);

        Assertions.assertEquals(
            "{0=Person 0, 1=Person 1, 2=Person 2}", readAll(result.dataset()).toString());
      }
    }
  }

  @Test
  public void testWhenNotMatchedBySourceDeleteIf() throws Exception {
    // Test delete target rows which are not matched with source if expression is true

    try (VectorSchemaRoot source = buildSource(testDataset.getSchema(), allocator)) {
      try (ArrowArrayStream sourceStream = convertToStream(source, allocator)) {
        MergeInsertResult result =
            dataset.mergeInsert(
                new MergeInsertParams(Collections.singletonList("id"))
                    .withNotMatchedBySourceDeleteIf("name = 'Person 3'")
                    .withNotMatched(MergeInsertParams.WhenNotMatched.DoNothing),
                sourceStream);

        Assertions.assertEquals(
            "{0=Person 0, 1=Person 1, 2=Person 2, 4=Person 4}",
            readAll(result.dataset()).toString());
      }
    }
  }

  @Test
  public void testWhenMatchedFailWithMatches() throws Exception {
    // Test fail when there are matched rows

    try (VectorSchemaRoot source = buildSource(testDataset.getSchema(), allocator)) {
      try (ArrowArrayStream sourceStream = convertToStream(source, allocator)) {
        String originalDataset = readAll(dataset).toString();

        Assertions.assertThrows(
            Exception.class,
            () ->
                dataset.mergeInsert(
                    new MergeInsertParams(Collections.singletonList("id")).withMatchedFail(),
                    sourceStream));

        // Verify dataset remains unchanged
        Assertions.assertEquals(
            originalDataset,
            readAll(dataset).toString(),
            "Dataset should remain unchanged after failed mergeInsert");
      }
    }
  }

  @Test
  public void testWhenMatchedFailWithoutMatches() throws Exception {
    // Test success when there are no matched rows

    try (VectorSchemaRoot root = VectorSchemaRoot.create(testDataset.getSchema(), allocator)) {
      root.allocateNew();

      IntVector idVector = (IntVector) root.getVector("id");
      VarCharVector nameVector = (VarCharVector) root.getVector("name");

      List<Integer> sourceIds = Arrays.asList(100, 101, 102);
      for (int i = 0; i < sourceIds.size(); i++) {
        idVector.setSafe(i, sourceIds.get(i));
        String name = "New Data " + sourceIds.get(i);
        nameVector.setSafe(i, name.getBytes(StandardCharsets.UTF_8));
      }

      root.setRowCount(sourceIds.size());

      try (ArrowArrayStream sourceStream = convertToStream(root, allocator)) {
        MergeInsertResult result =
            dataset.mergeInsert(
                new MergeInsertParams(Collections.singletonList("id")).withMatchedFail(),
                sourceStream);

        // Verify new data is inserted
        Map<Integer, String> resultMap = readAll(result.dataset());
        for (int id : sourceIds) {
          Assertions.assertTrue(resultMap.containsKey(id));
          Assertions.assertEquals("New Data " + id, resultMap.get(id));
        }
      }
    }
  }

  private VectorSchemaRoot buildSource(Schema schema, RootAllocator allocator) {
    List<Integer> sourceIds = Arrays.asList(0, 1, 2, 7, 8, 9);

    VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
    root.allocateNew();

    IntVector idVector = (IntVector) root.getVector("id");
    VarCharVector nameVector = (VarCharVector) root.getVector("name");

    for (int i = 0; i < sourceIds.size(); i++) {
      idVector.setSafe(i, sourceIds.get(i));
      String name = "Source " + sourceIds.get(i);
      nameVector.setSafe(i, name.getBytes(StandardCharsets.UTF_8));
    }

    root.setRowCount(sourceIds.size());

    return root;
  }

  private ArrowArrayStream convertToStream(VectorSchemaRoot root, RootAllocator allocator)
      throws Exception {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    try (ArrowStreamWriter writer = new ArrowStreamWriter(root, null, out)) {
      writer.start();
      writer.writeBatch();
      writer.end();
    }

    ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
    ArrowStreamReader reader = new ArrowStreamReader(in, allocator);

    ArrowArrayStream stream = ArrowArrayStream.allocateNew(allocator);
    Data.exportArrayStream(allocator, reader, stream);

    return stream;
  }

  private TreeMap<Integer, String> readAll(Dataset dataset) throws Exception {
    try (ArrowReader reader = dataset.newScan().scanBatches()) {
      TreeMap<Integer, String> map = new TreeMap<>();

      while (reader.loadNextBatch()) {
        VectorSchemaRoot batch = reader.getVectorSchemaRoot();
        for (int i = 0; i < batch.getRowCount(); i++) {
          IntVector idVector = (IntVector) batch.getVector("id");
          VarCharVector nameVector = (VarCharVector) batch.getVector("name");
          map.put(idVector.get(i), new String(nameVector.get(i)));
        }
      }

      return map;
    }
  }

  @Test
  public void testDedupeAscending() throws Exception {
    // Test dedupe_by with ascending ordering keeps the smallest value

    // Create a dataset with id, value, ts columns
    Schema schemaWithTs =
        new Schema(
            Arrays.asList(
                Field.nullable("id", new ArrowType.Int(32, true)),
                Field.nullable("value", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
                Field.nullable("ts", new ArrowType.Int(64, true))));

    String datasetPath = tempDir.resolve("dedupe_test_asc").toString();

    // Create initial dataset
    try (VectorSchemaRoot root = VectorSchemaRoot.create(schemaWithTs, allocator)) {
      root.allocateNew();
      IntVector idVector = (IntVector) root.getVector("id");
      org.apache.arrow.vector.Float8Vector valueVector =
          (org.apache.arrow.vector.Float8Vector) root.getVector("value");
      BigIntVector tsVector = (BigIntVector) root.getVector("ts");

      int[] ids = {1, 2, 3};
      double[] values = {1.0, 2.0, 3.0};
      long[] timestamps = {100, 200, 300};

      for (int i = 0; i < ids.length; i++) {
        idVector.setSafe(i, ids[i]);
        valueVector.setSafe(i, values[i]);
        tsVector.setSafe(i, timestamps[i]);
      }
      root.setRowCount(ids.length);

      try (ArrowArrayStream stream = convertToStream(root, allocator)) {
        Dataset.create(allocator, stream, datasetPath, new WriteParams.Builder().build()).close();
      }
    }

    // Source data with duplicates:
    // id=1: ts=150 and ts=50 -> should keep ts=50 (ascending)
    // id=2: ts=180 and ts=250 -> should keep ts=180 (ascending)
    try (Dataset ds = Dataset.open(datasetPath, allocator)) {
      try (VectorSchemaRoot source = VectorSchemaRoot.create(schemaWithTs, allocator)) {
        source.allocateNew();
        IntVector idVector = (IntVector) source.getVector("id");
        org.apache.arrow.vector.Float8Vector valueVector =
            (org.apache.arrow.vector.Float8Vector) source.getVector("value");
        BigIntVector tsVector = (BigIntVector) source.getVector("ts");

        int[] ids = {1, 1, 2, 2};
        double[] values = {10.0, 11.0, 20.0, 21.0};
        long[] timestamps = {150, 50, 180, 250};

        for (int i = 0; i < ids.length; i++) {
          idVector.setSafe(i, ids[i]);
          valueVector.setSafe(i, values[i]);
          tsVector.setSafe(i, timestamps[i]);
        }
        source.setRowCount(ids.length);

        try (ArrowArrayStream sourceStream = convertToStream(source, allocator)) {
          MergeInsertResult result =
              ds.mergeInsert(
                  new MergeInsertParams(Collections.singletonList("id"))
                      .withMatchedUpdateAll()
                      .withDedupeBy("ts")
                      .withDedupeSortOptions(MergeInsertParams.SortOptions.ascending()),
                  sourceStream);

          Assertions.assertEquals(2, result.stats().numUpdatedRows());

          // Verify results - should have ts=50 for id=1 and ts=180 for id=2
          TreeMap<Integer, Long> tsMap = readAllWithTs(result.dataset());
          Assertions.assertEquals(50L, tsMap.get(1));
          Assertions.assertEquals(180L, tsMap.get(2));
          Assertions.assertEquals(300L, tsMap.get(3)); // unchanged
        }
      }
    }
  }

  @Test
  public void testDedupeDescending() throws Exception {
    // Test dedupe_by with descending ordering keeps the largest value

    Schema schemaWithTs =
        new Schema(
            Arrays.asList(
                Field.nullable("id", new ArrowType.Int(32, true)),
                Field.nullable("value", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
                Field.nullable("ts", new ArrowType.Int(64, true))));

    String datasetPath = tempDir.resolve("dedupe_test_desc").toString();

    // Create initial dataset
    try (VectorSchemaRoot root = VectorSchemaRoot.create(schemaWithTs, allocator)) {
      root.allocateNew();
      IntVector idVector = (IntVector) root.getVector("id");
      org.apache.arrow.vector.Float8Vector valueVector =
          (org.apache.arrow.vector.Float8Vector) root.getVector("value");
      BigIntVector tsVector = (BigIntVector) root.getVector("ts");

      int[] ids = {1, 2, 3};
      double[] values = {1.0, 2.0, 3.0};
      long[] timestamps = {100, 200, 300};

      for (int i = 0; i < ids.length; i++) {
        idVector.setSafe(i, ids[i]);
        valueVector.setSafe(i, values[i]);
        tsVector.setSafe(i, timestamps[i]);
      }
      root.setRowCount(ids.length);

      try (ArrowArrayStream stream = convertToStream(root, allocator)) {
        Dataset.create(allocator, stream, datasetPath, new WriteParams.Builder().build()).close();
      }
    }

    // Source data with duplicates:
    // id=1: ts=150 and ts=50 -> should keep ts=150 (descending)
    // id=2: ts=180 and ts=250 -> should keep ts=250 (descending)
    try (Dataset ds = Dataset.open(datasetPath, allocator)) {
      try (VectorSchemaRoot source = VectorSchemaRoot.create(schemaWithTs, allocator)) {
        source.allocateNew();
        IntVector idVector = (IntVector) source.getVector("id");
        org.apache.arrow.vector.Float8Vector valueVector =
            (org.apache.arrow.vector.Float8Vector) source.getVector("value");
        BigIntVector tsVector = (BigIntVector) source.getVector("ts");

        int[] ids = {1, 1, 2, 2};
        double[] values = {10.0, 11.0, 20.0, 21.0};
        long[] timestamps = {150, 50, 180, 250};

        for (int i = 0; i < ids.length; i++) {
          idVector.setSafe(i, ids[i]);
          valueVector.setSafe(i, values[i]);
          tsVector.setSafe(i, timestamps[i]);
        }
        source.setRowCount(ids.length);

        try (ArrowArrayStream sourceStream = convertToStream(source, allocator)) {
          MergeInsertResult result =
              ds.mergeInsert(
                  new MergeInsertParams(Collections.singletonList("id"))
                      .withMatchedUpdateAll()
                      .withDedupeBy("ts")
                      .withDedupeSortOptions(MergeInsertParams.SortOptions.descending()),
                  sourceStream);

          Assertions.assertEquals(2, result.stats().numUpdatedRows());

          // Verify results - should have ts=150 for id=1 and ts=250 for id=2
          TreeMap<Integer, Long> tsMap = readAllWithTs(result.dataset());
          Assertions.assertEquals(150L, tsMap.get(1));
          Assertions.assertEquals(250L, tsMap.get(2));
          Assertions.assertEquals(300L, tsMap.get(3)); // unchanged
        }
      }
    }
  }

  private TreeMap<Integer, Long> readAllWithTs(Dataset dataset) throws Exception {
    try (ArrowReader reader = dataset.newScan().scanBatches()) {
      TreeMap<Integer, Long> map = new TreeMap<>();

      while (reader.loadNextBatch()) {
        VectorSchemaRoot batch = reader.getVectorSchemaRoot();
        for (int i = 0; i < batch.getRowCount(); i++) {
          IntVector idVector = (IntVector) batch.getVector("id");
          BigIntVector tsVector = (BigIntVector) batch.getVector("ts");
          map.put(idVector.get(i), tsVector.get(i));
        }
      }

      return map;
    }
  }
}
