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
package com.lancedb.lance;

import com.lancedb.lance.io.StorageOptionsProvider;
import com.lancedb.lance.operation.Append;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.net.URI;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration tests for Fragment write and Transaction commit with StorageOptionsProvider.
 *
 * <p>These tests verify that: - Fragment.create() can use StorageOptionsProvider for credential
 * refresh - Transaction.commit() can use StorageOptionsProvider when creating new datasets -
 * Transaction.commit() can use StorageOptionsProvider when committing to existing datasets
 *
 * <p>These tests require LocalStack to be running. Run with: docker compose up -d
 *
 * <p>Set LANCE_INTEGRATION_TEST=1 environment variable to enable these tests.
 */
@EnabledIfEnvironmentVariable(named = "LANCE_INTEGRATION_TEST", matches = "1")
public class StorageOptionsProviderWriteTest {

  private static final String ENDPOINT_URL = "http://localhost:4566";
  private static final String REGION = "us-east-1";
  private static final String ACCESS_KEY = "ACCESS_KEY";
  private static final String SECRET_KEY = "SECRET_KEY";
  private static final String BUCKET_NAME = "lance-write-provider-test-java";

  private static S3Client s3Client;

  @BeforeAll
  static void setup() {
    s3Client =
        S3Client.builder()
            .endpointOverride(URI.create(ENDPOINT_URL))
            .region(Region.of(REGION))
            .credentialsProvider(
                StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(ACCESS_KEY, SECRET_KEY)))
            .forcePathStyle(true)
            .build();

    // Delete bucket if it exists from previous run
    try {
      deleteBucket();
    } catch (Exception e) {
      // Ignore if bucket doesn't exist
    }

    // Create test bucket
    s3Client.createBucket(CreateBucketRequest.builder().bucket(BUCKET_NAME).build());
  }

  @AfterAll
  static void tearDown() {
    if (s3Client != null) {
      try {
        deleteBucket();
      } catch (Exception e) {
        // Ignore cleanup errors
      }
      s3Client.close();
    }
  }

  private static void deleteBucket() {
    // Delete all objects first
    List<S3Object> objects =
        s3Client
            .listObjectsV2(ListObjectsV2Request.builder().bucket(BUCKET_NAME).build())
            .contents();
    for (S3Object obj : objects) {
      s3Client.deleteObject(
          DeleteObjectRequest.builder().bucket(BUCKET_NAME).key(obj.key()).build());
    }
    s3Client.deleteBucket(DeleteBucketRequest.builder().bucket(BUCKET_NAME).build());
  }

  /**
   * Mock StorageOptionsProvider that tracks how many times it's been called and provides
   * credentials with expiration times.
   */
  static class MockStorageOptionsProvider implements StorageOptionsProvider {
    private final Map<String, String> baseStorageOptions;
    private final int credentialExpiresInSeconds;
    private final AtomicInteger callCount = new AtomicInteger(0);

    public MockStorageOptionsProvider(
        Map<String, String> storageOptions, int credentialExpiresInSeconds) {
      this.baseStorageOptions = new HashMap<>(storageOptions);
      this.credentialExpiresInSeconds = credentialExpiresInSeconds;
    }

    @Override
    public Map<String, String> fetchStorageOptions() {
      int count = callCount.incrementAndGet();

      Map<String, String> storageOptions = new HashMap<>(baseStorageOptions);
      long expiresAtMillis = System.currentTimeMillis() + (credentialExpiresInSeconds * 1000L);
      storageOptions.put("expires_at_millis", String.valueOf(expiresAtMillis));

      // Add a marker to track which credential set this is
      storageOptions.put("credential_version", String.valueOf(count));

      return storageOptions;
    }

    public int getCallCount() {
      return callCount.get();
    }
  }

  @Test
  void testFragmentCreateWithStorageOptionsProvider() throws Exception {
    try (BufferAllocator allocator = new RootAllocator()) {
      String tableName = UUID.randomUUID().toString();
      String tableUri = "s3://" + BUCKET_NAME + "/" + tableName + ".lance";

      // Create base storage options
      Map<String, String> storageOptions = new HashMap<>();
      storageOptions.put("allow_http", "true");
      storageOptions.put("aws_access_key_id", ACCESS_KEY);
      storageOptions.put("aws_secret_access_key", SECRET_KEY);
      storageOptions.put("aws_endpoint", ENDPOINT_URL);
      storageOptions.put("aws_region", REGION);

      // Create mock provider with 60-second expiration
      MockStorageOptionsProvider provider = new MockStorageOptionsProvider(storageOptions, 60);

      // Create schema
      Schema schema =
          new Schema(
              Arrays.asList(
                  new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null),
                  new Field("value", FieldType.nullable(new ArrowType.Int(32, true)), null)));

      // First, create an empty dataset
      WriteParams createParams =
          new WriteParams.Builder().withStorageOptions(storageOptions).build();
      try (Dataset dataset = Dataset.create(allocator, tableUri, schema, createParams)) {
        assertEquals(1, dataset.version());

        // Now write fragments using StorageOptionsProvider
        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
          IntVector idVector = (IntVector) root.getVector("id");
          IntVector valueVector = (IntVector) root.getVector("value");

          // Write first fragment
          idVector.allocateNew(3);
          valueVector.allocateNew(3);

          idVector.set(0, 1);
          valueVector.set(0, 100);
          idVector.set(1, 2);
          valueVector.set(1, 200);
          idVector.set(2, 3);
          valueVector.set(2, 300);

          idVector.setValueCount(3);
          valueVector.setValueCount(3);
          root.setRowCount(3);

          WriteParams writeParams = new WriteParams.Builder().build();

          int callCountBefore = provider.getCallCount();

          // Create fragment with StorageOptionsProvider
          List<FragmentMetadata> fragments1 =
              Fragment.create(tableUri, allocator, root, writeParams, provider);

          assertNotNull(fragments1);
          assertEquals(1, fragments1.size());

          // Verify provider was called
          int callCountAfter1 = provider.getCallCount();
          assertTrue(
              callCountAfter1 > callCountBefore,
              "Provider should be called during fragment creation");

          // Write second fragment with different data
          idVector.set(0, 4);
          valueVector.set(0, 400);
          idVector.set(1, 5);
          valueVector.set(1, 500);
          idVector.set(2, 6);
          valueVector.set(2, 600);
          root.setRowCount(3);

          // Create another fragment with the same provider
          List<FragmentMetadata> fragments2 =
              Fragment.create(tableUri, allocator, root, writeParams, provider);

          assertNotNull(fragments2);
          assertEquals(1, fragments2.size());

          int callCountAfter2 = provider.getCallCount();
          assertTrue(
              callCountAfter2 > callCountAfter1,
              "Provider should be called again for second fragment");

          // Commit both fragments to the dataset
          FragmentOperation.Append appendOp = new FragmentOperation.Append(fragments1);
          try (Dataset updatedDataset =
              Dataset.commit(allocator, tableUri, appendOp, Optional.of(1L), storageOptions)) {
            assertEquals(2, updatedDataset.version());
            assertEquals(3, updatedDataset.countRows());

            // Append second fragment
            FragmentOperation.Append appendOp2 = new FragmentOperation.Append(fragments2);
            try (Dataset finalDataset =
                Dataset.commit(allocator, tableUri, appendOp2, Optional.of(2L), storageOptions)) {
              assertEquals(3, finalDataset.version());
              assertEquals(6, finalDataset.countRows());
            }
          }
        }
      }
    }
  }

  @Test
  void testTransactionCommitWithStorageOptionsProviderNewDataset() throws Exception {
    try (BufferAllocator allocator = new RootAllocator()) {
      String tableName = UUID.randomUUID().toString();
      String tableUri = "s3://" + BUCKET_NAME + "/" + tableName + ".lance";

      // Create base storage options
      Map<String, String> storageOptions = new HashMap<>();
      storageOptions.put("allow_http", "true");
      storageOptions.put("aws_access_key_id", ACCESS_KEY);
      storageOptions.put("aws_secret_access_key", SECRET_KEY);
      storageOptions.put("aws_endpoint", ENDPOINT_URL);
      storageOptions.put("aws_region", REGION);

      // Create mock provider
      MockStorageOptionsProvider provider = new MockStorageOptionsProvider(storageOptions, 60);

      // Create schema
      Schema schema =
          new Schema(
              Arrays.asList(
                  new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null),
                  new Field("name", FieldType.nullable(new ArrowType.Utf8()), null)));

      // Create empty dataset first with provider
      WriteParams createParams =
          new WriteParams.Builder().withStorageOptions(storageOptions).build();
      try (Dataset dataset = Dataset.create(allocator, tableUri, schema, createParams)) {
        assertEquals(1, dataset.version());

        // Re-open dataset with provider so transactions can use it
        ReadOptions readOptions =
            new ReadOptions.Builder()
                .setStorageOptions(storageOptions)
                .setStorageOptionsProvider(provider)
                .build();
        try (Dataset datasetWithProvider = Dataset.open(allocator, tableUri, readOptions)) {

          // Create fragments to commit
          try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
            IntVector idVector = (IntVector) root.getVector("id");
            org.apache.arrow.vector.VarCharVector nameVector =
                (org.apache.arrow.vector.VarCharVector) root.getVector("name");

            idVector.allocateNew(2);
            nameVector.allocateNew(2);

            idVector.set(0, 1);
            nameVector.setSafe(0, "Alice".getBytes());
            idVector.set(1, 2);
            nameVector.setSafe(1, "Bob".getBytes());

            idVector.setValueCount(2);
            nameVector.setValueCount(2);
            root.setRowCount(2);

            WriteParams writeParams = new WriteParams.Builder().build();

            // Create fragments with provider
            List<FragmentMetadata> fragments =
                Fragment.create(tableUri, allocator, root, writeParams, provider);

            // Create transaction - provider will be inherited from dataset
            Append appendOp = Append.builder().fragments(fragments).build();

            int callCountBefore = provider.getCallCount();

            Transaction transaction =
                new Transaction.Builder(datasetWithProvider)
                    .readVersion(1L)
                    .operation(appendOp)
                    .build();

            // Commit transaction
            try (Dataset committedDataset = transaction.commit()) {
              assertNotNull(committedDataset);
              assertEquals(2, committedDataset.version());
              assertEquals(2, committedDataset.countRows());

              // Verify provider was called during commit
              int callCountAfter = provider.getCallCount();
              assertTrue(
                  callCountAfter > callCountBefore,
                  "Provider should be called during transaction commit");
            }
          }
        }
      }
    }
  }

  @Test
  void testTransactionCommitWithStorageOptionsProviderExistingDataset() throws Exception {
    try (BufferAllocator allocator = new RootAllocator()) {
      String tableName = UUID.randomUUID().toString();
      String tableUri = "s3://" + BUCKET_NAME + "/" + tableName + ".lance";

      // Create base storage options
      Map<String, String> storageOptions = new HashMap<>();
      storageOptions.put("allow_http", "true");
      storageOptions.put("aws_access_key_id", ACCESS_KEY);
      storageOptions.put("aws_secret_access_key", SECRET_KEY);
      storageOptions.put("aws_endpoint", ENDPOINT_URL);
      storageOptions.put("aws_region", REGION);

      // Create schema
      Schema schema =
          new Schema(
              Arrays.asList(
                  new Field("x", FieldType.nullable(new ArrowType.Int(32, true)), null),
                  new Field("y", FieldType.nullable(new ArrowType.Int(32, true)), null)));

      // Create initial dataset with some data
      try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
        IntVector xVector = (IntVector) root.getVector("x");
        IntVector yVector = (IntVector) root.getVector("y");

        xVector.allocateNew(2);
        yVector.allocateNew(2);

        xVector.set(0, 1);
        yVector.set(0, 10);
        xVector.set(1, 2);
        yVector.set(1, 20);

        xVector.setValueCount(2);
        yVector.setValueCount(2);
        root.setRowCount(2);

        WriteParams createParams =
            new WriteParams.Builder().withStorageOptions(storageOptions).build();

        try (Dataset dataset = Dataset.create(allocator, tableUri, schema, createParams)) {
          List<FragmentMetadata> initialFragments =
              Fragment.create(tableUri, allocator, root, createParams);
          FragmentOperation.Append initialAppend = new FragmentOperation.Append(initialFragments);

          try (Dataset datasetV2 =
              Dataset.commit(allocator, tableUri, initialAppend, Optional.of(1L), storageOptions)) {
            assertEquals(2, datasetV2.version());
            assertEquals(2, datasetV2.countRows());

            // Now test committing additional data with StorageOptionsProvider
            MockStorageOptionsProvider provider =
                new MockStorageOptionsProvider(storageOptions, 60);

            // Create more data to append
            xVector.set(0, 3);
            yVector.set(0, 30);
            xVector.set(1, 4);
            yVector.set(1, 40);
            root.setRowCount(2);

            WriteParams writeParams = new WriteParams.Builder().build();

            // Create fragments with provider
            List<FragmentMetadata> newFragments =
                Fragment.create(tableUri, allocator, root, writeParams, provider);

            int callCountBefore = provider.getCallCount();

            // Open the existing dataset with provider
            ReadOptions readOptions =
                new ReadOptions.Builder()
                    .setStorageOptions(storageOptions)
                    .setStorageOptionsProvider(provider)
                    .build();
            try (Dataset existingDataset = Dataset.open(allocator, tableUri, readOptions)) {
              // Create transaction - provider will be inherited from dataset
              Append appendOp = Append.builder().fragments(newFragments).build();

              Transaction transaction =
                  new Transaction.Builder(existingDataset)
                      .readVersion(existingDataset.version())
                      .operation(appendOp)
                      .build();

              // Commit transaction to existing dataset
              try (Dataset committedDataset = transaction.commit()) {
                assertNotNull(committedDataset);
                assertEquals(3, committedDataset.version());
                assertEquals(4, committedDataset.countRows());

                // Verify provider was called during commit
                int callCountAfter = provider.getCallCount();
                assertTrue(
                    callCountAfter > callCountBefore,
                    "Provider should be called during transaction commit to existing dataset");
              }
            }
          }
        }
      }
    }
  }

  @Test
  void testMultipleFragmentsAndCommitWithProvider() throws Exception {
    try (BufferAllocator allocator = new RootAllocator()) {
      String tableName = UUID.randomUUID().toString();
      String tableUri = "s3://" + BUCKET_NAME + "/" + tableName + ".lance";

      // Create base storage options
      Map<String, String> storageOptions = new HashMap<>();
      storageOptions.put("allow_http", "true");
      storageOptions.put("aws_access_key_id", ACCESS_KEY);
      storageOptions.put("aws_secret_access_key", SECRET_KEY);
      storageOptions.put("aws_endpoint", ENDPOINT_URL);
      storageOptions.put("aws_region", REGION);

      // Create mock provider with short expiration to test refresh
      MockStorageOptionsProvider provider = new MockStorageOptionsProvider(storageOptions, 5);

      // Create schema
      Schema schema =
          new Schema(
              Arrays.asList(
                  new Field("data", FieldType.nullable(new ArrowType.Int(32, true)), null)));

      // Create empty dataset
      WriteParams createParams =
          new WriteParams.Builder().withStorageOptions(storageOptions).build();
      try (Dataset dataset = Dataset.create(allocator, tableUri, schema, createParams)) {
        assertEquals(1, dataset.version());

        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
          IntVector dataVector = (IntVector) root.getVector("data");
          WriteParams writeParams = new WriteParams.Builder().build();

          // Create multiple fragments with the provider
          List<List<FragmentMetadata>> allFragments = new java.util.ArrayList<>();

          for (int i = 0; i < 3; i++) {
            dataVector.allocateNew(2);
            dataVector.set(0, i * 10);
            dataVector.set(1, i * 10 + 1);
            dataVector.setValueCount(2);
            root.setRowCount(2);

            List<FragmentMetadata> fragments =
                Fragment.create(tableUri, allocator, root, writeParams, provider);
            allFragments.add(fragments);

            // Add a small delay to simulate real-world usage
            Thread.sleep(100);
          }

          // Verify provider was called multiple times
          int callCountAfterFragments = provider.getCallCount();
          assertTrue(callCountAfterFragments >= 3, "Provider should be called for each fragment");

          // Commit all fragments in one transaction with provider
          int callCountBeforeCommit = provider.getCallCount();

          FragmentOperation.Append appendOp1 = new FragmentOperation.Append(allFragments.get(0));
          try (Dataset v2 =
              Dataset.commit(allocator, tableUri, appendOp1, Optional.of(1L), storageOptions)) {

            // Open v2 with provider so transaction can inherit it
            ReadOptions readOptions =
                new ReadOptions.Builder()
                    .setStorageOptions(storageOptions)
                    .setStorageOptionsProvider(provider)
                    .build();
            try (Dataset v2WithProvider = Dataset.open(allocator, tableUri, readOptions)) {

              Append appendOp2 = Append.builder().fragments(allFragments.get(1)).build();
              Transaction transaction =
                  new Transaction.Builder(v2WithProvider)
                      .readVersion(v2WithProvider.version())
                      .operation(appendOp2)
                      .build();

              try (Dataset v3 = transaction.commit()) {
                assertEquals(3, v3.version());

                // Re-open v3 with provider for next transaction
                try (Dataset v3WithProvider = Dataset.open(allocator, tableUri, readOptions)) {

                  // Commit third fragment
                  Append appendOp3 = Append.builder().fragments(allFragments.get(2)).build();
                  Transaction transaction2 =
                      new Transaction.Builder(v3WithProvider)
                          .readVersion(v3WithProvider.version())
                          .operation(appendOp3)
                          .build();

                  try (Dataset finalDataset = transaction2.commit()) {
                    assertNotNull(finalDataset);
                    assertEquals(4, finalDataset.version());
                    assertEquals(6, finalDataset.countRows()); // 3 fragments * 2 rows each

                    // Verify provider was used during commits
                    int finalCallCount = provider.getCallCount();
                    assertTrue(
                        finalCallCount > callCountBeforeCommit,
                        "Provider should be called during transaction commits");
                  }
                }
              }
            }
          }
        }
      }
    }
  }
}
