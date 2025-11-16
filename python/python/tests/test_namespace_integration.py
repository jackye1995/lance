# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The Lance Authors

"""
Integration tests for Lance Namespace with S3 and credential refresh.

This test simulates a namespace server that returns incrementing credentials
and verifies that the credential refresh mechanism works correctly.

See DEVELOPMENT.md under heading "Integration Tests" for more information.
"""

import copy
import time
import uuid
from threading import Lock
from typing import Dict

import lance
import pyarrow as pa
import pytest
from lance_namespace import (
    CreateEmptyTableRequest,
    CreateEmptyTableResponse,
    DescribeTableRequest,
    DescribeTableResponse,
    LanceNamespace,
)

# These are all keys that are accepted by storage_options
CONFIG = {
    "allow_http": "true",
    "aws_access_key_id": "ACCESS_KEY",
    "aws_secret_access_key": "SECRET_KEY",
    "aws_endpoint": "http://localhost:4566",
    "aws_region": "us-east-1",
}


def get_boto3_client(*args, **kwargs):
    import boto3

    return boto3.client(
        *args,
        region_name=CONFIG["aws_region"],
        aws_access_key_id=CONFIG["aws_access_key_id"],
        aws_secret_access_key=CONFIG["aws_secret_access_key"],
        **kwargs,
    )


@pytest.fixture(scope="module")
def s3_bucket():
    s3 = get_boto3_client("s3", endpoint_url=CONFIG["aws_endpoint"])
    bucket_name = "lance-namespace-integtest"
    # if bucket exists, delete it
    try:
        delete_bucket(s3, bucket_name)
    except s3.exceptions.NoSuchBucket:
        pass
    s3.create_bucket(Bucket=bucket_name)
    yield bucket_name

    delete_bucket(s3, bucket_name)


def delete_bucket(s3, bucket_name):
    # Delete all objects first
    try:
        for obj in s3.list_objects(Bucket=bucket_name).get("Contents", []):
            s3.delete_object(Bucket=bucket_name, Key=obj["Key"])
        s3.delete_bucket(Bucket=bucket_name)
    except Exception:
        pass


class TrackingNamespace(LanceNamespace):
    """
    Mock namespace implementation that wraps DirectoryNamespace with call tracking.

    This implementation:
    - Wraps DirectoryNamespace to get real namespace behavior
    - Tracks the number of times describe_table/create_empty_table are called
    - Modifies storage options to add incrementing credentials for testing
    - Returns credentials with short expiration times for testing refresh
    """

    def __init__(
        self,
        bucket_name: str,
        storage_options: Dict[str, str],
        credential_expires_in_seconds: int = 60,
    ):
        """
        Initialize the mock namespace.

        Parameters
        ----------
        bucket_name : str
            The S3 bucket name or local filesystem path where tables are stored.
            If starts with '/' or 'file://', treated as local path; otherwise S3.
        storage_options : Dict[str, str]
            Base storage options (aws_endpoint, aws_region, etc.)
        credential_expires_in_seconds : int
            How long credentials should be valid (for testing refresh)
        """
        from lance.namespace import DirectoryNamespace

        self.bucket_name = bucket_name
        self.base_storage_options = storage_options
        self.credential_expires_in_seconds = credential_expires_in_seconds
        self.describe_call_count = 0
        self.create_call_count = 0
        self.lock = Lock()

        # Create underlying DirectoryNamespace
        # Pass storage options with storage. prefix for DirectoryNamespace
        # Disable manifest mode for testing to avoid initialization issues
        dir_props = {f"storage.{k}": v for k, v in storage_options.items()}

        # Support both S3 buckets and local paths
        # If bucket_name looks like a path, use it directly; else treat as S3
        if bucket_name.startswith("/") or bucket_name.startswith("file://"):
            # Local filesystem path
            dir_props["root"] = f"{bucket_name}/namespace_root"
        else:
            # S3 bucket
            dir_props["root"] = f"s3://{bucket_name}/namespace_root"

        # Manifest mode is enabled by default
        # (required for multi-level table IDs / child namespaces)
        self.inner = DirectoryNamespace(**dir_props)

    def get_describe_call_count(self) -> int:
        """Get the number of times describe_table has been called."""
        with self.lock:
            return self.describe_call_count

    def get_create_call_count(self) -> int:
        """Get the number of times create_empty_table has been called."""
        with self.lock:
            return self.create_call_count

    def namespace_id(self) -> str:
        """Return a unique identifier for this namespace instance."""
        return f"TrackingNamespace {{ inner: {self.inner.namespace_id()} }}"

    def _modify_storage_options(
        self, storage_options: Dict[str, str], count: int
    ) -> Dict[str, str]:
        """Add incrementing credentials and expiration to storage options."""
        modified = copy.deepcopy(storage_options) if storage_options else {}

        # Add incrementing credentials for testing
        modified["aws_access_key_id"] = f"AKID_{count}"
        modified["aws_secret_access_key"] = f"SECRET_{count}"
        modified["aws_session_token"] = f"TOKEN_{count}"

        # Add expiration timestamp
        expires_at_millis = int(
            (time.time() + self.credential_expires_in_seconds) * 1000
        )
        modified["expires_at_millis"] = str(expires_at_millis)

        return modified

    def create_empty_table(
        self, request: CreateEmptyTableRequest
    ) -> CreateEmptyTableResponse:
        """
        Create an empty table via DirectoryNamespace and add test credentials.

        This delegates to DirectoryNamespace for real table creation, then
        modifies the response to add incrementing credentials for testing.
        """
        with self.lock:
            self.create_call_count += 1
            count = self.create_call_count

        # Delegate to inner DirectoryNamespace
        response = self.inner.create_empty_table(request)

        # Modify storage options to add test credentials
        response.storage_options = self._modify_storage_options(
            response.storage_options, count
        )

        return response

    def describe_table(self, request: DescribeTableRequest) -> DescribeTableResponse:
        """
        Describe a table via DirectoryNamespace and add test credentials.

        This delegates to DirectoryNamespace for real table lookup, then
        modifies the response to add incrementing credentials for testing.
        """
        with self.lock:
            self.describe_call_count += 1
            count = self.describe_call_count

        # Delegate to inner DirectoryNamespace
        response = self.inner.describe_table(request)

        # Modify storage options to add test credentials
        response.storage_options = self._modify_storage_options(
            response.storage_options, count
        )

        return response


@pytest.mark.integration
def test_namespace_open_dataset(s3_bucket: str):
    """
    Test opening a dataset through a namespace with credential tracking.

    This test verifies that:
    1. We can create a dataset through a namespace
    2. We can open the dataset through the namespace
    3. The namespace's describe_table method is called to fetch credentials
    """
    storage_options = copy.deepcopy(CONFIG)

    # Create mock namespace
    # Use long credential expiration to test caching
    namespace = TrackingNamespace(
        bucket_name=s3_bucket,
        storage_options=storage_options,
        credential_expires_in_seconds=3600,  # 1 hour
    )

    # Create dataset through namespace
    table1 = pa.Table.from_pylist([{"a": 1, "b": 2}, {"a": 10, "b": 20}])
    table_name = uuid.uuid4().hex
    # Use child namespace instead of root
    table_id = ["test_ns", table_name]

    assert namespace.get_create_call_count() == 0
    assert namespace.get_describe_call_count() == 0

    # Write dataset through namespace (CREATE mode)
    ds = lance.write_dataset(
        table1, namespace=namespace, table_id=table_id, mode="create"
    )
    assert len(ds.versions()) == 1
    assert ds.count_rows() == 2

    # Verify create_empty_table was called during CREATE
    assert namespace.get_create_call_count() == 1

    # Open dataset through namespace again (ignoring storage options from namespace)
    # This should call describe_table once
    # We still need to provide storage_options since we're ignoring namespace ones
    ds_from_namespace = lance.dataset(
        namespace=namespace,
        table_id=table_id,
        storage_options=storage_options,
        ignore_namespace_table_storage_options=True,
    )

    # Verify describe_table was called exactly once during open
    assert namespace.get_describe_call_count() == 1

    # Verify we can read the data
    assert ds_from_namespace.count_rows() == 2
    result = ds_from_namespace.to_table()
    assert result == table1

    # Test credential caching: verify subsequent reads use cached credentials
    call_count_before_reads = namespace.get_describe_call_count()
    for _ in range(3):
        assert ds_from_namespace.count_rows() == 2

    # Verify describe_table was NOT called again (credentials are cached)
    assert namespace.get_describe_call_count() == call_count_before_reads


@pytest.mark.integration
def test_namespace_with_refresh(s3_bucket: str):
    storage_options = copy.deepcopy(CONFIG)

    # Create mock namespace with very short expiration (3 seconds)
    # to simulate credentials that need frequent refresh
    namespace = TrackingNamespace(
        bucket_name=s3_bucket,
        storage_options=storage_options,
        credential_expires_in_seconds=3,  # Short expiration for testing
    )

    # Create dataset through namespace
    table1 = pa.Table.from_pylist([{"a": 1, "b": 2}, {"a": 10, "b": 20}])
    table_name = uuid.uuid4().hex
    table_id = ["test_ns", table_name]

    assert namespace.get_create_call_count() == 0
    assert namespace.get_describe_call_count() == 0

    ds = lance.write_dataset(
        table1, namespace=namespace, table_id=table_id, mode="create"
    )
    assert ds.count_rows() == 2

    # Verify create_empty_table was called
    assert namespace.get_create_call_count() == 1

    # Open dataset with short refresh offset
    # Storage options from namespace are used by default
    ds_from_namespace = lance.dataset(
        namespace=namespace,
        table_id=table_id,
        s3_credentials_refresh_offset_seconds=1,
    )

    initial_call_count = namespace.get_describe_call_count()
    assert initial_call_count == 1

    # Verify we can read the data
    assert ds_from_namespace.count_rows() == 2
    result = ds_from_namespace.to_table()
    assert result == table1

    # Record call count after initial reads
    call_count_after_initial_reads = namespace.get_describe_call_count()

    # Wait for credentials to expire
    time.sleep(5)

    # Perform another read operation after expiration
    # This should trigger a credential refresh since credentials have expired
    assert ds_from_namespace.count_rows() == 2
    result2 = ds_from_namespace.to_table()
    assert result2 == table1

    final_call_count = namespace.get_describe_call_count()
    assert final_call_count == call_count_after_initial_reads + 1


@pytest.mark.integration
def test_namespace_append_through_namespace(s3_bucket: str):
    """
    Test appending to a dataset through namespace.

    This verifies that write operations work correctly with namespace-managed
    credentials.
    """
    storage_options = copy.deepcopy(CONFIG)

    # Create namespace
    namespace = TrackingNamespace(
        bucket_name=s3_bucket,
        storage_options=storage_options,
        credential_expires_in_seconds=3600,
    )

    # Create initial dataset through namespace
    table1 = pa.Table.from_pylist([{"a": 1, "b": 2}])
    table_name = uuid.uuid4().hex
    table_id = ["test_ns", table_name]

    assert namespace.get_create_call_count() == 0
    assert namespace.get_describe_call_count() == 0

    ds = lance.write_dataset(
        table1, namespace=namespace, table_id=table_id, mode="create"
    )
    assert ds.count_rows() == 1
    assert len(ds.versions()) == 1

    # Verify create_empty_table was called
    assert namespace.get_create_call_count() == 1
    initial_describe_count = namespace.get_describe_call_count()

    # Append more data through namespace
    table2 = pa.Table.from_pylist([{"a": 10, "b": 20}])
    ds = lance.write_dataset(
        table2, namespace=namespace, table_id=table_id, mode="append"
    )
    assert ds.count_rows() == 2
    assert len(ds.versions()) == 2

    # Verify describe_table was called for append (not create_empty_table)
    assert namespace.get_create_call_count() == 1  # Still just 1
    assert namespace.get_describe_call_count() == initial_describe_count + 1

    # Re-open through namespace to see updated data
    # We still need to provide storage_options since we're ignoring namespace ones
    ds_from_namespace = lance.dataset(
        namespace=namespace,
        table_id=table_id,
        storage_options=storage_options,
        ignore_namespace_table_storage_options=True,
    )

    assert ds_from_namespace.count_rows() == 2
    assert len(ds_from_namespace.versions()) == 2

    # Describe_table should have been called again
    assert namespace.get_describe_call_count() == initial_describe_count + 2


@pytest.mark.integration
def test_namespace_write_create_mode(s3_bucket: str):
    """
    Test writing a dataset through namespace in CREATE mode.

    This verifies that:
    1. CREATE mode calls namespace.create_empty_table()
    2. Storage options provider is set up correctly
    3. Data is written to the location returned by namespace
    """
    storage_options = copy.deepcopy(CONFIG)

    # Create namespace
    namespace = TrackingNamespace(
        bucket_name=s3_bucket,
        storage_options=storage_options,
        credential_expires_in_seconds=3600,
    )

    # Write dataset through namespace in CREATE mode
    table1 = pa.Table.from_pylist([{"a": 1, "b": 2}, {"a": 10, "b": 20}])
    table_name = uuid.uuid4().hex

    assert namespace.get_create_call_count() == 0
    assert namespace.get_describe_call_count() == 0

    ds = lance.write_dataset(
        table1,
        namespace=namespace,
        table_id=["test_ns", table_name],
        mode="create",
    )

    # Verify create_empty_table was called once
    assert namespace.get_create_call_count() == 1
    # Note: describe_table may be called during write operations by the storage
    # options provider to refresh credentials. This is expected behavior.

    # Verify data was written correctly
    assert ds.count_rows() == 2
    assert len(ds.versions()) == 1
    result = ds.to_table()
    assert result == table1


@pytest.mark.integration
def test_namespace_write_append_mode(s3_bucket: str):
    """
    Test writing a dataset through namespace in APPEND mode.

    This verifies that:
    1. CREATE mode calls namespace.create_empty_table()
    2. APPEND mode calls namespace.describe_table()
    3. Storage options provider is set up correctly
    4. Data is appended to existing table
    """
    storage_options = copy.deepcopy(CONFIG)

    # Create namespace
    namespace = TrackingNamespace(
        bucket_name=s3_bucket,
        storage_options=storage_options,
        credential_expires_in_seconds=3600,
    )

    # Create initial dataset through namespace
    table1 = pa.Table.from_pylist([{"a": 1, "b": 2}])
    table_name = uuid.uuid4().hex
    table_id = ["test_ns", table_name]

    assert namespace.get_create_call_count() == 0
    assert namespace.get_describe_call_count() == 0

    ds = lance.write_dataset(
        table1, namespace=namespace, table_id=table_id, mode="create"
    )
    assert ds.count_rows() == 1

    # Verify create_empty_table was called
    assert namespace.get_create_call_count() == 1
    assert namespace.get_describe_call_count() == 0

    # Append data through namespace
    table2 = pa.Table.from_pylist([{"a": 10, "b": 20}])

    ds = lance.write_dataset(
        table2,
        namespace=namespace,
        table_id=table_id,
        mode="append",
    )

    # Verify describe_table was called exactly once
    # (credentials are cached since expiration is long)
    assert namespace.get_create_call_count() == 1  # Still just 1
    describe_count_after_append = namespace.get_describe_call_count()
    assert describe_count_after_append == 1

    # Verify data was appended correctly
    assert ds.count_rows() == 2
    assert len(ds.versions()) == 2

    # Test credential caching: perform additional reads
    call_count_before_reads = namespace.get_describe_call_count()
    for _ in range(3):
        assert ds.count_rows() == 2
    # Verify no additional describe_table calls (credentials are cached)
    assert namespace.get_describe_call_count() == call_count_before_reads


@pytest.mark.integration
def test_namespace_write_overwrite_mode(s3_bucket: str):
    """
    Test writing a dataset through namespace in OVERWRITE mode.

    This verifies that:
    1. CREATE mode calls namespace.create_empty_table()
    2. OVERWRITE mode calls namespace.describe_table()
    3. Storage options provider is set up correctly
    4. Data is overwritten
    """
    storage_options = copy.deepcopy(CONFIG)

    # Create namespace
    namespace = TrackingNamespace(
        bucket_name=s3_bucket,
        storage_options=storage_options,
        credential_expires_in_seconds=3600,
    )

    # Create initial dataset through namespace
    table1 = pa.Table.from_pylist([{"a": 1, "b": 2}])
    table_name = uuid.uuid4().hex
    table_id = ["test_ns", table_name]

    assert namespace.get_create_call_count() == 0
    assert namespace.get_describe_call_count() == 0

    ds = lance.write_dataset(
        table1, namespace=namespace, table_id=table_id, mode="create"
    )
    assert ds.count_rows() == 1

    # Verify create_empty_table was called
    assert namespace.get_create_call_count() == 1
    assert namespace.get_describe_call_count() == 0

    # Overwrite data through namespace
    table2 = pa.Table.from_pylist([{"a": 10, "b": 20}, {"a": 100, "b": 200}])

    ds = lance.write_dataset(
        table2,
        namespace=namespace,
        table_id=table_id,
        mode="overwrite",
    )

    # Verify describe_table was called exactly once
    # (credentials are cached since expiration is long)
    assert namespace.get_create_call_count() == 1  # Still just 1
    describe_count_after_overwrite = namespace.get_describe_call_count()
    assert describe_count_after_overwrite == 1

    # Verify data was overwritten correctly
    assert ds.count_rows() == 2
    assert len(ds.versions()) == 2
    result = ds.to_table()
    assert result == table2

    # Test credential caching: perform additional reads
    call_count_before_reads = namespace.get_describe_call_count()
    for _ in range(3):
        assert ds.count_rows() == 2
    # Verify no additional describe_table calls (credentials are cached)
    assert namespace.get_describe_call_count() == call_count_before_reads


@pytest.mark.integration
def test_namespace_distributed_write(s3_bucket: str):
    """
    Test distributed write pattern through namespace.

    This simulates a distributed write workflow:
    1. Call namespace.create_empty_table() to get table location and credentials
    2. Write multiple fragments in parallel (simulated sequentially here)
    3. Commit all fragments together to create the initial table

    This verifies that:
    - create_empty_table() is called once
    - Storage options provider is used for write_fragments
    - All fragments are committed successfully
    """
    storage_options = copy.deepcopy(CONFIG)

    # Create namespace
    namespace = TrackingNamespace(
        bucket_name=s3_bucket,
        storage_options=storage_options,
        credential_expires_in_seconds=3600,
    )

    table_name = uuid.uuid4().hex
    table_id = ["test_ns", table_name]

    # Step 1: Create empty table through namespace
    from lance_namespace import CreateEmptyTableRequest

    request = CreateEmptyTableRequest(id=table_id, location=None, properties=None)
    response = namespace.create_empty_table(request)

    assert namespace.get_create_call_count() == 1
    assert namespace.get_describe_call_count() == 0

    table_uri = response.location
    assert table_uri is not None

    # Get storage options and create provider
    from lance.namespace import LanceNamespaceStorageOptionsProvider

    namespace_storage_options = response.storage_options
    assert namespace_storage_options is not None

    storage_options_provider = LanceNamespaceStorageOptionsProvider(
        namespace=namespace, table_id=table_id
    )

    # Merge storage options (namespace takes precedence)
    merged_options = dict(storage_options)
    merged_options.update(namespace_storage_options)

    # Step 2: Write multiple fragments (simulating distributed writes)
    from lance.fragment import write_fragments

    # Fragment 1
    fragment1_data = pa.Table.from_pylist([{"a": 1, "b": 2}, {"a": 3, "b": 4}])
    fragment1 = write_fragments(
        fragment1_data,
        table_uri,
        storage_options=merged_options,
        storage_options_provider=storage_options_provider,
    )

    # Fragment 2
    fragment2_data = pa.Table.from_pylist([{"a": 10, "b": 20}, {"a": 30, "b": 40}])
    fragment2 = write_fragments(
        fragment2_data,
        table_uri,
        storage_options=merged_options,
        storage_options_provider=storage_options_provider,
    )

    # Fragment 3
    fragment3_data = pa.Table.from_pylist([{"a": 100, "b": 200}])
    fragment3 = write_fragments(
        fragment3_data,
        table_uri,
        storage_options=merged_options,
        storage_options_provider=storage_options_provider,
    )

    # Step 3: Commit all fragments together
    all_fragments = fragment1 + fragment2 + fragment3

    operation = lance.LanceOperation.Overwrite(fragment1_data.schema, all_fragments)

    ds = lance.LanceDataset.commit(
        table_uri,
        operation,
        storage_options=merged_options,
        storage_options_provider=storage_options_provider,
    )

    # Verify the table was created with all fragments
    assert ds.count_rows() == 5  # 2 + 2 + 1
    assert len(ds.versions()) == 1

    # Verify data is correct
    result = ds.to_table().sort_by("a")
    expected = pa.Table.from_pylist(
        [
            {"a": 1, "b": 2},
            {"a": 3, "b": 4},
            {"a": 10, "b": 20},
            {"a": 30, "b": 40},
            {"a": 100, "b": 200},
        ]
    )
    assert result == expected

    # Verify we can read through namespace
    ds_from_namespace = lance.dataset(
        namespace=namespace,
        table_id=table_id,
    )
    assert ds_from_namespace.count_rows() == 5


@pytest.mark.integration
def test_file_writer_with_storage_options_provider(s3_bucket: str):
    """
    Test LanceFileWriter with storage_options_provider from namespace.

    This test verifies that:
    1. LanceFileWriter accepts storage_options_provider parameter
    2. Initial storage options from namespace are used
    3. Storage options provider properly refreshes credentials when they expire
    """
    from lance import LanceNamespaceStorageOptionsProvider
    from lance.file import LanceFileReader, LanceFileWriter

    storage_options = copy.deepcopy(CONFIG)

    # Create namespace with short credential expiration (3 seconds)
    # This allows us to deterministically test credential refresh
    namespace = TrackingNamespace(
        bucket_name=s3_bucket,
        storage_options=storage_options,
        credential_expires_in_seconds=3,  # Short expiration for testing refresh
    )

    # Create a test dataset through namespace
    table1 = pa.Table.from_pylist([{"a": 1, "b": 2}, {"a": 10, "b": 20}])
    table_name = uuid.uuid4().hex
    table_id = ["test_ns", table_name]

    assert namespace.get_create_call_count() == 0
    assert namespace.get_describe_call_count() == 0

    ds = lance.write_dataset(
        table1, namespace=namespace, table_id=table_id, mode="create"
    )
    assert ds.count_rows() == 2

    # Verify create_empty_table was called
    assert namespace.get_create_call_count() == 1

    # Get storage options and provider from namespace
    describe_response = namespace.describe_table(
        DescribeTableRequest(id=table_id, version=None)
    )
    namespace_storage_options = describe_response.storage_options

    # Create storage options provider
    provider = LanceNamespaceStorageOptionsProvider(
        namespace=namespace, table_id=table_id
    )

    # Reset call count after the describe call above
    initial_describe_count = namespace.get_describe_call_count()

    # Test 1: Write a lance file quickly (should complete within 3 seconds)
    # This should NOT trigger a credential refresh
    file_uri = f"s3://{s3_bucket}/{table_name}_file_test.lance"
    schema = pa.schema([pa.field("x", pa.int64()), pa.field("y", pa.int64())])

    writer = LanceFileWriter(
        file_uri,
        schema=schema,
        storage_options=namespace_storage_options,
        storage_options_provider=provider,
    )

    # Write some batches (small write that completes quickly)
    batch = pa.RecordBatch.from_pydict({"x": [1, 2, 3], "y": [4, 5, 6]}, schema=schema)
    writer.write_batch(batch)

    batch2 = pa.RecordBatch.from_pydict(
        {"x": [7, 8, 9], "y": [10, 11, 12]}, schema=schema
    )
    writer.write_batch(batch2)

    # Close the writer (this also finishes the file)
    writer.close()

    # Verify no credential refresh happened (write completed quickly)
    describe_count_after_write = namespace.get_describe_call_count()
    assert describe_count_after_write == initial_describe_count

    # Verify the file was written successfully
    reader = LanceFileReader(file_uri, storage_options=namespace_storage_options)
    result = reader.read_all(batch_size=1024)
    result_table = result.to_table()
    assert result_table.num_rows == 6
    assert result_table.schema == schema

    expected_table = pa.table(
        {"x": [1, 2, 3, 7, 8, 9], "y": [4, 5, 6, 10, 11, 12]}, schema=schema
    )
    assert result_table == expected_table

    # Test 2: Wait for credentials to expire, then write again
    # This SHOULD trigger a credential refresh
    time.sleep(5)

    file_uri2 = f"s3://{s3_bucket}/{table_name}_file_test2.lance"
    writer2 = LanceFileWriter(
        file_uri2,
        schema=schema,
        storage_options=namespace_storage_options,
        storage_options_provider=provider,
    )

    batch3 = pa.RecordBatch.from_pydict(
        {"x": [100, 200], "y": [300, 400]}, schema=schema
    )
    writer2.write_batch(batch3)
    writer2.close()

    # Verify credential refresh happened (credentials expired)
    final_describe_count = namespace.get_describe_call_count()
    assert final_describe_count == describe_count_after_write + 1

    # Verify the second file was written successfully
    reader2 = LanceFileReader(file_uri2, storage_options=namespace_storage_options)
    result2 = reader2.read_all(batch_size=1024)
    result_table2 = result2.to_table()
    assert result_table2.num_rows == 2
    expected_table2 = pa.table({"x": [100, 200], "y": [300, 400]}, schema=schema)
    assert result_table2 == expected_table2
