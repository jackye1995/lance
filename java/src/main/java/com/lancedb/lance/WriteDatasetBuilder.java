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

import com.lancedb.lance.namespace.LanceNamespace;
import com.lancedb.lance.namespace.model.CreateEmptyTableRequest;
import com.lancedb.lance.namespace.model.CreateEmptyTableResponse;
import com.lancedb.lance.namespace.model.DescribeTableRequest;
import com.lancedb.lance.namespace.model.DescribeTableResponse;

import org.apache.arrow.c.ArrowArrayStream;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Builder for writing datasets.
 *
 * <p>This builder provides a fluent API for creating or writing to datasets either directly to a
 * URI or through a LanceNamespace. When using a namespace, the table location and storage options
 * are automatically managed with credential vending support.
 *
 * <p>Example usage with URI:
 *
 * <pre>{@code
 * Dataset dataset = Dataset.write(reader)
 *     .uri("s3://bucket/table.lance")
 *     .mode(WriteMode.CREATE)
 *     .execute();
 * }</pre>
 *
 * <p>Example usage with namespace:
 *
 * <pre>{@code
 * Dataset dataset = Dataset.write(reader)
 *     .namespace(myNamespace, Arrays.asList("my_table"))
 *     .mode(WriteMode.CREATE)
 *     .execute();
 * }</pre>
 */
public class WriteDatasetBuilder {
  private final ArrowReader reader;
  private BufferAllocator allocator;
  private String uri;
  private LanceNamespace namespace;
  private List<String> tableId;
  private WriteParams.WriteMode mode = WriteParams.WriteMode.CREATE;
  private Schema schema;
  private Map<String, String> storageOptions = new HashMap<>();
  private boolean ignoreNamespaceStorageOptions = false;
  private Optional<Integer> maxRowsPerFile = Optional.empty();
  private Optional<Integer> maxRowsPerGroup = Optional.empty();
  private Optional<Long> maxBytesPerFile = Optional.empty();
  private Optional<Boolean> enableStableRowIds = Optional.empty();
  private Optional<WriteParams.LanceFileVersion> dataStorageVersion = Optional.empty();

  /** Creates a new builder instance. Package-private, use Dataset.write() instead. */
  WriteDatasetBuilder(ArrowReader reader) {
    Preconditions.checkNotNull(reader, "reader must not be null");
    this.reader = reader;
  }

  /**
   * Sets the buffer allocator.
   *
   * @param allocator Arrow buffer allocator
   * @return this builder instance
   */
  public WriteDatasetBuilder allocator(BufferAllocator allocator) {
    Preconditions.checkNotNull(allocator);
    this.allocator = allocator;
    return this;
  }

  /**
   * Sets the dataset URI.
   *
   * <p>Either uri() or namespace() must be specified, but not both.
   *
   * @param uri The dataset URI (e.g., "s3://bucket/table.lance" or "file:///path/to/table.lance")
   * @return this builder instance
   */
  public WriteDatasetBuilder uri(String uri) {
    this.uri = uri;
    return this;
  }

  /**
   * Sets the namespace and table identifier.
   *
   * <p>Either uri() or namespace() must be specified, but not both.
   *
   * @param namespace The namespace implementation to use for table operations
   * @param tableId The table identifier (e.g., Arrays.asList("my_table"))
   * @return this builder instance
   */
  public WriteDatasetBuilder namespace(LanceNamespace namespace, List<String> tableId) {
    this.namespace = namespace;
    this.tableId = tableId;
    return this;
  }

  /**
   * Sets the write mode.
   *
   * @param mode The write mode (CREATE, APPEND, or OVERWRITE)
   * @return this builder instance
   */
  public WriteDatasetBuilder mode(WriteParams.WriteMode mode) {
    Preconditions.checkNotNull(mode);
    this.mode = mode;
    return this;
  }

  /**
   * Sets the schema for the dataset.
   *
   * <p>If not provided, the schema will be inferred from the reader.
   *
   * @param schema The dataset schema
   * @return this builder instance
   */
  public WriteDatasetBuilder schema(Schema schema) {
    this.schema = schema;
    return this;
  }

  /**
   * Sets storage options for the dataset.
   *
   * @param storageOptions Storage configuration options
   * @return this builder instance
   */
  public WriteDatasetBuilder storageOptions(Map<String, String> storageOptions) {
    this.storageOptions = new HashMap<>(storageOptions);
    return this;
  }

  /**
   * Sets whether to ignore storage options from the namespace's describe_table() or
   * create_empty_table().
   *
   * @param ignoreNamespaceStorageOptions If true, storage options returned from namespace will be
   *     ignored
   * @return this builder instance
   */
  public WriteDatasetBuilder ignoreNamespaceStorageOptions(boolean ignoreNamespaceStorageOptions) {
    this.ignoreNamespaceStorageOptions = ignoreNamespaceStorageOptions;
    return this;
  }

  /**
   * Sets the maximum number of rows per file.
   *
   * @param maxRowsPerFile Maximum rows per file
   * @return this builder instance
   */
  public WriteDatasetBuilder maxRowsPerFile(int maxRowsPerFile) {
    this.maxRowsPerFile = Optional.of(maxRowsPerFile);
    return this;
  }

  /**
   * Sets the maximum number of rows per group.
   *
   * @param maxRowsPerGroup Maximum rows per group
   * @return this builder instance
   */
  public WriteDatasetBuilder maxRowsPerGroup(int maxRowsPerGroup) {
    this.maxRowsPerGroup = Optional.of(maxRowsPerGroup);
    return this;
  }

  /**
   * Sets the maximum number of bytes per file.
   *
   * @param maxBytesPerFile Maximum bytes per file
   * @return this builder instance
   */
  public WriteDatasetBuilder maxBytesPerFile(long maxBytesPerFile) {
    this.maxBytesPerFile = Optional.of(maxBytesPerFile);
    return this;
  }

  /**
   * Sets whether to enable stable row IDs.
   *
   * @param enableStableRowIds Whether to enable stable row IDs
   * @return this builder instance
   */
  public WriteDatasetBuilder enableStableRowIds(boolean enableStableRowIds) {
    this.enableStableRowIds = Optional.of(enableStableRowIds);
    return this;
  }

  /**
   * Sets the data storage version.
   *
   * @param dataStorageVersion The Lance file version to use
   * @return this builder instance
   */
  public WriteDatasetBuilder dataStorageVersion(WriteParams.LanceFileVersion dataStorageVersion) {
    this.dataStorageVersion = Optional.of(dataStorageVersion);
    return this;
  }

  /**
   * Executes the write operation and returns the created dataset.
   *
   * <p>If a namespace is configured, this automatically handles table creation or retrieval through
   * the namespace API with credential vending support.
   *
   * @return Dataset
   * @throws IllegalArgumentException if required parameters are missing or invalid
   */
  public Dataset execute() {
    // Validate that exactly one of uri or namespace is provided
    boolean hasUri = uri != null;
    boolean hasNamespace = namespace != null && tableId != null;

    if (hasUri && hasNamespace) {
      throw new IllegalArgumentException(
          "Cannot specify both uri and namespace. Use one or the other.");
    }
    if (!hasUri && !hasNamespace) {
      if (namespace != null) {
        throw new IllegalArgumentException(
            "namespace is set but tableId is missing. Both namespace and tableId must be provided"
                + " together.");
      } else if (tableId != null) {
        throw new IllegalArgumentException(
            "tableId is set but namespace is missing. Both namespace and tableId must be provided"
                + " together.");
      } else {
        throw new IllegalArgumentException("Either uri or namespace must be provided.");
      }
    }

    // Create allocator if not provided
    if (allocator == null) {
      allocator = new RootAllocator(Long.MAX_VALUE);
    }

    // Handle namespace-based writing
    if (hasNamespace) {
      return executeWithNamespace();
    }

    // Handle URI-based writing
    return executeWithUri();
  }

  private Dataset executeWithNamespace() {
    String tableUri;
    Map<String, String> namespaceStorageOptions = null;

    // Mode-specific namespace operations
    if (mode == WriteParams.WriteMode.CREATE) {
      // Call namespace.createEmptyTable() to create new table
      CreateEmptyTableRequest request = new CreateEmptyTableRequest();
      request.setId(tableId);

      CreateEmptyTableResponse response = namespace.createEmptyTable(request);

      tableUri = response.getLocation();
      if (tableUri == null || tableUri.isEmpty()) {
        throw new IllegalArgumentException("Namespace did not return a table location");
      }

      namespaceStorageOptions = ignoreNamespaceStorageOptions ? null : response.getStorageOptions();
    } else {
      // For APPEND/OVERWRITE modes, call namespace.describeTable()
      DescribeTableRequest request = new DescribeTableRequest();
      request.setId(tableId);

      DescribeTableResponse response = namespace.describeTable(request);

      tableUri = response.getLocation();
      if (tableUri == null || tableUri.isEmpty()) {
        throw new IllegalArgumentException("Namespace did not return a table location");
      }

      namespaceStorageOptions = ignoreNamespaceStorageOptions ? null : response.getStorageOptions();
    }

    // Merge storage options (namespace options + user options, with namespace taking precedence)
    Map<String, String> mergedStorageOptions = new HashMap<>(storageOptions);
    if (namespaceStorageOptions != null && !namespaceStorageOptions.isEmpty()) {
      mergedStorageOptions.putAll(namespaceStorageOptions);
    }

    // Build WriteParams with merged storage options
    WriteParams.Builder paramsBuilder =
        new WriteParams.Builder().withMode(mode).withStorageOptions(mergedStorageOptions);

    maxRowsPerFile.ifPresent(paramsBuilder::withMaxRowsPerFile);
    maxRowsPerGroup.ifPresent(paramsBuilder::withMaxRowsPerGroup);
    maxBytesPerFile.ifPresent(paramsBuilder::withMaxBytesPerFile);
    enableStableRowIds.ifPresent(paramsBuilder::withEnableStableRowIds);
    dataStorageVersion.ifPresent(paramsBuilder::withDataStorageVersion);

    WriteParams params = paramsBuilder.build();

    // Use Dataset.create() which handles CREATE/APPEND/OVERWRITE modes
    return createDatasetWithStream(tableUri, params);
  }

  private Dataset executeWithUri() {
    WriteParams.Builder paramsBuilder =
        new WriteParams.Builder().withMode(mode).withStorageOptions(storageOptions);

    maxRowsPerFile.ifPresent(paramsBuilder::withMaxRowsPerFile);
    maxRowsPerGroup.ifPresent(paramsBuilder::withMaxRowsPerGroup);
    maxBytesPerFile.ifPresent(paramsBuilder::withMaxBytesPerFile);
    enableStableRowIds.ifPresent(paramsBuilder::withEnableStableRowIds);
    dataStorageVersion.ifPresent(paramsBuilder::withDataStorageVersion);

    WriteParams params = paramsBuilder.build();

    return createDatasetWithStream(uri, params);
  }

  private Dataset createDatasetWithStream(String path, WriteParams params) {
    try (ArrowArrayStream stream = ArrowArrayStream.allocateNew(allocator)) {
      Data.exportArrayStream(allocator, reader, stream);
      return Dataset.create(allocator, stream, path, params);
    }
  }
}
