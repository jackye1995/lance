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
package org.lance.namespace;

import org.lance.namespace.model.*;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/** Tests for DirectoryNamespace implementation. */
public class DirectoryNamespaceTest {
  @TempDir Path tempDir;

  private BufferAllocator allocator;
  private DirectoryNamespace namespace;

  @BeforeEach
  void setUp() {
    allocator = new RootAllocator(Long.MAX_VALUE);
    namespace = new DirectoryNamespace();

    Map<String, String> config = new HashMap<>();
    config.put("root", tempDir.toString());
    namespace.initialize(config, allocator);
  }

  @AfterEach
  void tearDown() {
    if (namespace != null) {
      namespace.close();
    }
    if (allocator != null) {
      allocator.close();
    }
  }

  private byte[] createTestTableData() throws Exception {
    Schema schema =
        new Schema(
            Arrays.asList(
                new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null),
                new Field("name", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("age", FieldType.nullable(new ArrowType.Int(32, true)), null)));

    try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
      IntVector idVector = (IntVector) root.getVector("id");
      VarCharVector nameVector = (VarCharVector) root.getVector("name");
      IntVector ageVector = (IntVector) root.getVector("age");

      // Allocate space for 3 rows
      idVector.allocateNew(3);
      nameVector.allocateNew(3);
      ageVector.allocateNew(3);

      idVector.set(0, 1);
      nameVector.set(0, "Alice".getBytes());
      ageVector.set(0, 30);

      idVector.set(1, 2);
      nameVector.set(1, "Bob".getBytes());
      ageVector.set(1, 25);

      idVector.set(2, 3);
      nameVector.set(2, "Charlie".getBytes());
      ageVector.set(2, 35);

      // Set value counts
      idVector.setValueCount(3);
      nameVector.setValueCount(3);
      ageVector.setValueCount(3);
      root.setRowCount(3);

      ByteArrayOutputStream out = new ByteArrayOutputStream();
      try (ArrowStreamWriter writer = new ArrowStreamWriter(root, null, out)) {
        writer.writeBatch();
      }
      return out.toByteArray();
    }
  }

  @Test
  void testNamespaceId() {
    String namespaceId = namespace.namespaceId();
    assertNotNull(namespaceId);
    assertTrue(namespaceId.contains("DirectoryNamespace"));
  }

  @Test
  void testCreateAndListNamespaces() {
    // Create a namespace
    CreateNamespaceRequest createReq = new CreateNamespaceRequest().id(Arrays.asList("workspace"));
    CreateNamespaceResponse createResp = namespace.createNamespace(createReq);
    assertNotNull(createResp);

    // List namespaces
    ListNamespacesRequest listReq = new ListNamespacesRequest();
    ListNamespacesResponse listResp = namespace.listNamespaces(listReq);
    assertNotNull(listResp);
    assertNotNull(listResp.getNamespaces());
    assertTrue(listResp.getNamespaces().contains("workspace"));
  }

  @Test
  void testDescribeNamespace() {
    // Create a namespace
    CreateNamespaceRequest createReq = new CreateNamespaceRequest().id(Arrays.asList("workspace"));
    namespace.createNamespace(createReq);

    // Describe namespace
    DescribeNamespaceRequest descReq =
        new DescribeNamespaceRequest().id(Arrays.asList("workspace"));
    DescribeNamespaceResponse descResp = namespace.describeNamespace(descReq);
    assertNotNull(descResp);
    assertNotNull(descResp.getProperties());
  }

  @Test
  void testNamespaceExists() {
    // Create a namespace
    CreateNamespaceRequest createReq = new CreateNamespaceRequest().id(Arrays.asList("workspace"));
    namespace.createNamespace(createReq);

    // Check existence
    NamespaceExistsRequest existsReq = new NamespaceExistsRequest().id(Arrays.asList("workspace"));
    assertDoesNotThrow(() -> namespace.namespaceExists(existsReq));

    // Check non-existent namespace
    NamespaceExistsRequest notExistsReq =
        new NamespaceExistsRequest().id(Arrays.asList("nonexistent"));
    assertThrows(RuntimeException.class, () -> namespace.namespaceExists(notExistsReq));
  }

  @Test
  void testDropNamespace() {
    // Create a namespace
    CreateNamespaceRequest createReq = new CreateNamespaceRequest().id(Arrays.asList("workspace"));
    namespace.createNamespace(createReq);

    // Drop namespace
    DropNamespaceRequest dropReq = new DropNamespaceRequest().id(Arrays.asList("workspace"));
    DropNamespaceResponse dropResp = namespace.dropNamespace(dropReq);
    assertNotNull(dropResp);

    // Verify it's gone
    NamespaceExistsRequest existsReq = new NamespaceExistsRequest().id(Arrays.asList("workspace"));
    assertThrows(RuntimeException.class, () -> namespace.namespaceExists(existsReq));
  }

  @Test
  void testCreateTable() throws Exception {
    // Create parent namespace
    CreateNamespaceRequest createNsReq =
        new CreateNamespaceRequest().id(Arrays.asList("workspace"));
    namespace.createNamespace(createNsReq);

    // Create table with data
    byte[] tableData = createTestTableData();
    CreateTableRequest createReq =
        new CreateTableRequest().id(Arrays.asList("workspace", "test_table"));
    CreateTableResponse createResp = namespace.createTable(createReq, tableData);

    assertNotNull(createResp);
    assertNotNull(createResp.getLocation());
    assertTrue(createResp.getLocation().contains("test_table"));
    assertEquals(Long.valueOf(1), createResp.getVersion());
  }

  @Test
  void testListTables() throws Exception {
    // Create parent namespace
    CreateNamespaceRequest createNsReq =
        new CreateNamespaceRequest().id(Arrays.asList("workspace"));
    namespace.createNamespace(createNsReq);

    // Create a table
    byte[] tableData = createTestTableData();
    CreateTableRequest createReq =
        new CreateTableRequest().id(Arrays.asList("workspace", "test_table"));
    namespace.createTable(createReq, tableData);

    // List tables
    ListTablesRequest listReq = new ListTablesRequest().id(Arrays.asList("workspace"));
    ListTablesResponse listResp = namespace.listTables(listReq);

    assertNotNull(listResp);
    assertNotNull(listResp.getTables());
    assertTrue(listResp.getTables().contains("test_table"));
  }

  @Test
  void testDescribeTable() throws Exception {
    // Create parent namespace
    CreateNamespaceRequest createNsReq =
        new CreateNamespaceRequest().id(Arrays.asList("workspace"));
    namespace.createNamespace(createNsReq);

    // Create a table
    byte[] tableData = createTestTableData();
    CreateTableRequest createReq =
        new CreateTableRequest().id(Arrays.asList("workspace", "test_table"));
    namespace.createTable(createReq, tableData);

    // Describe table
    DescribeTableRequest descReq =
        new DescribeTableRequest().id(Arrays.asList("workspace", "test_table"));
    DescribeTableResponse descResp = namespace.describeTable(descReq);

    assertNotNull(descResp);
    assertNotNull(descResp.getLocation());
    assertTrue(descResp.getLocation().contains("test_table"));
  }

  @Test
  void testTableExists() throws Exception {
    // Create parent namespace
    CreateNamespaceRequest createNsReq =
        new CreateNamespaceRequest().id(Arrays.asList("workspace"));
    namespace.createNamespace(createNsReq);

    // Create a table
    byte[] tableData = createTestTableData();
    CreateTableRequest createReq =
        new CreateTableRequest().id(Arrays.asList("workspace", "test_table"));
    namespace.createTable(createReq, tableData);

    // Check existence
    TableExistsRequest existsReq =
        new TableExistsRequest().id(Arrays.asList("workspace", "test_table"));
    assertDoesNotThrow(() -> namespace.tableExists(existsReq));

    // Check non-existent table
    TableExistsRequest notExistsReq =
        new TableExistsRequest().id(Arrays.asList("workspace", "nonexistent"));
    assertThrows(RuntimeException.class, () -> namespace.tableExists(notExistsReq));
  }

  @Test
  void testDropTable() throws Exception {
    // Create parent namespace
    CreateNamespaceRequest createNsReq =
        new CreateNamespaceRequest().id(Arrays.asList("workspace"));
    namespace.createNamespace(createNsReq);

    // Create a table
    byte[] tableData = createTestTableData();
    CreateTableRequest createReq =
        new CreateTableRequest().id(Arrays.asList("workspace", "test_table"));
    namespace.createTable(createReq, tableData);

    // Drop table
    DropTableRequest dropReq = new DropTableRequest().id(Arrays.asList("workspace", "test_table"));
    DropTableResponse dropResp = namespace.dropTable(dropReq);
    assertNotNull(dropResp);

    // Verify it's gone
    TableExistsRequest existsReq =
        new TableExistsRequest().id(Arrays.asList("workspace", "test_table"));
    assertThrows(RuntimeException.class, () -> namespace.tableExists(existsReq));
  }

  @Test
  void testCreateEmptyTable() {
    // Create parent namespace
    CreateNamespaceRequest createNsReq =
        new CreateNamespaceRequest().id(Arrays.asList("workspace"));
    namespace.createNamespace(createNsReq);

    // Create empty table (metadata-only operation)
    CreateEmptyTableRequest createReq =
        new CreateEmptyTableRequest().id(Arrays.asList("workspace", "empty_table"));

    CreateEmptyTableResponse createResp = namespace.createEmptyTable(createReq);

    assertNotNull(createResp);
    assertNotNull(createResp.getLocation());
  }

  @Test
  void testConcurrentCreateAndDropWithSingleInstance() throws Exception {
    int numTables = 10;
    ExecutorService executor = Executors.newFixedThreadPool(numTables);
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch doneLatch = new CountDownLatch(numTables);
    AtomicInteger successCount = new AtomicInteger(0);
    AtomicInteger failCount = new AtomicInteger(0);

    for (int i = 0; i < numTables; i++) {
      final int tableIndex = i;
      executor.submit(
          () -> {
            try {
              startLatch.await();

              String tableName = "concurrent_table_" + tableIndex;
              byte[] tableData = createTestTableData();

              CreateTableRequest createReq = new CreateTableRequest().id(Arrays.asList(tableName));
              namespace.createTable(createReq, tableData);

              DropTableRequest dropReq = new DropTableRequest().id(Arrays.asList(tableName));
              namespace.dropTable(dropReq);

              successCount.incrementAndGet();
            } catch (Exception e) {
              failCount.incrementAndGet();
            } finally {
              doneLatch.countDown();
            }
          });
    }

    startLatch.countDown();
    assertTrue(doneLatch.await(60, TimeUnit.SECONDS), "Timed out waiting for tasks to complete");

    executor.shutdown();
    assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));

    assertEquals(numTables, successCount.get(), "All tasks should succeed");
    assertEquals(0, failCount.get(), "No tasks should fail");

    ListTablesRequest listReq = new ListTablesRequest().id(Arrays.asList());
    ListTablesResponse listResp = namespace.listTables(listReq);
    assertEquals(0, listResp.getTables().size(), "All tables should be dropped");
  }

  @Test
  void testConcurrentCreateAndDropWithMultipleInstances() throws Exception {
    int numTables = 10;
    ExecutorService executor = Executors.newFixedThreadPool(numTables);
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch doneLatch = new CountDownLatch(numTables);
    AtomicInteger successCount = new AtomicInteger(0);
    AtomicInteger failCount = new AtomicInteger(0);
    List<DirectoryNamespace> namespaces = new ArrayList<>();

    for (int i = 0; i < numTables; i++) {
      executor.submit(
          () -> {
            DirectoryNamespace localNs = null;
            try {
              startLatch.await();

              localNs = new DirectoryNamespace();
              Map<String, String> config = new HashMap<>();
              config.put("root", tempDir.toString());
              config.put("inline_optimization_enabled", "false");
              localNs.initialize(config, allocator);

              synchronized (namespaces) {
                namespaces.add(localNs);
              }

              String tableName = "multi_ns_table_" + Thread.currentThread().getId();
              byte[] tableData = createTestTableData();

              CreateTableRequest createReq = new CreateTableRequest().id(Arrays.asList(tableName));
              localNs.createTable(createReq, tableData);

              DropTableRequest dropReq = new DropTableRequest().id(Arrays.asList(tableName));
              localNs.dropTable(dropReq);

              successCount.incrementAndGet();
            } catch (Exception e) {
              failCount.incrementAndGet();
            } finally {
              doneLatch.countDown();
            }
          });
    }

    startLatch.countDown();
    assertTrue(doneLatch.await(60, TimeUnit.SECONDS), "Timed out waiting for tasks to complete");

    executor.shutdown();
    assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));

    // Close all namespace instances
    for (DirectoryNamespace ns : namespaces) {
      try {
        ns.close();
      } catch (Exception e) {
        // Ignore
      }
    }

    assertEquals(numTables, successCount.get(), "All tasks should succeed");
    assertEquals(0, failCount.get(), "No tasks should fail");

    // Verify with a fresh namespace
    DirectoryNamespace verifyNs = new DirectoryNamespace();
    Map<String, String> config = new HashMap<>();
    config.put("root", tempDir.toString());
    verifyNs.initialize(config, allocator);

    ListTablesRequest listReq = new ListTablesRequest().id(Arrays.asList());
    ListTablesResponse listResp = verifyNs.listTables(listReq);
    assertEquals(0, listResp.getTables().size(), "All tables should be dropped");

    verifyNs.close();
  }

  @Test
  void testConcurrentCreateThenDropFromDifferentInstance() throws Exception {
    int numTables = 10;

    // First, create all tables using separate namespace instances
    ExecutorService createExecutor = Executors.newFixedThreadPool(numTables);
    CountDownLatch createStartLatch = new CountDownLatch(1);
    CountDownLatch createDoneLatch = new CountDownLatch(numTables);
    AtomicInteger createSuccessCount = new AtomicInteger(0);
    List<DirectoryNamespace> createNamespaces = new ArrayList<>();

    for (int i = 0; i < numTables; i++) {
      final int tableIndex = i;
      createExecutor.submit(
          () -> {
            DirectoryNamespace localNs = null;
            try {
              createStartLatch.await();

              localNs = new DirectoryNamespace();
              Map<String, String> config = new HashMap<>();
              config.put("root", tempDir.toString());
              config.put("inline_optimization_enabled", "false");
              localNs.initialize(config, allocator);

              synchronized (createNamespaces) {
                createNamespaces.add(localNs);
              }

              String tableName = "cross_instance_table_" + tableIndex;
              byte[] tableData = createTestTableData();

              CreateTableRequest createReq = new CreateTableRequest().id(Arrays.asList(tableName));
              localNs.createTable(createReq, tableData);

              createSuccessCount.incrementAndGet();
            } catch (Exception e) {
              // Ignore - test will fail on assertion
            } finally {
              createDoneLatch.countDown();
            }
          });
    }

    createStartLatch.countDown();
    assertTrue(createDoneLatch.await(60, TimeUnit.SECONDS), "Timed out waiting for creates");
    createExecutor.shutdown();

    assertEquals(numTables, createSuccessCount.get(), "All creates should succeed");

    // Close create namespaces
    for (DirectoryNamespace ns : createNamespaces) {
      try {
        ns.close();
      } catch (Exception e) {
        // Ignore
      }
    }

    // Now drop all tables using NEW namespace instances
    ExecutorService dropExecutor = Executors.newFixedThreadPool(numTables);
    CountDownLatch dropStartLatch = new CountDownLatch(1);
    CountDownLatch dropDoneLatch = new CountDownLatch(numTables);
    AtomicInteger dropSuccessCount = new AtomicInteger(0);
    AtomicInteger dropFailCount = new AtomicInteger(0);
    List<DirectoryNamespace> dropNamespaces = new ArrayList<>();

    for (int i = 0; i < numTables; i++) {
      final int tableIndex = i;
      dropExecutor.submit(
          () -> {
            DirectoryNamespace localNs = null;
            try {
              dropStartLatch.await();

              localNs = new DirectoryNamespace();
              Map<String, String> config = new HashMap<>();
              config.put("root", tempDir.toString());
              config.put("inline_optimization_enabled", "false");
              localNs.initialize(config, allocator);

              synchronized (dropNamespaces) {
                dropNamespaces.add(localNs);
              }

              String tableName = "cross_instance_table_" + tableIndex;

              DropTableRequest dropReq = new DropTableRequest().id(Arrays.asList(tableName));
              localNs.dropTable(dropReq);

              dropSuccessCount.incrementAndGet();
            } catch (Exception e) {
              dropFailCount.incrementAndGet();
            } finally {
              dropDoneLatch.countDown();
            }
          });
    }

    dropStartLatch.countDown();
    assertTrue(dropDoneLatch.await(60, TimeUnit.SECONDS), "Timed out waiting for drops");
    dropExecutor.shutdown();

    // Close drop namespaces
    for (DirectoryNamespace ns : dropNamespaces) {
      try {
        ns.close();
      } catch (Exception e) {
        // Ignore
      }
    }

    assertEquals(numTables, dropSuccessCount.get(), "All drops should succeed");
    assertEquals(0, dropFailCount.get(), "No drops should fail");
  }
}
