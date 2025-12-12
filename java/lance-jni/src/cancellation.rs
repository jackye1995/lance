// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Cancellation support for JNI operations.
//!
//! This module provides a wrapper around Arrow streams that checks a Java AtomicBoolean
//! for cancellation before each batch is read. This allows Java code to cancel long-running
//! native operations by setting the cancellation flag.

use arrow::array::{RecordBatch, RecordBatchReader};
use arrow::ffi_stream::ArrowArrayStreamReader;
use arrow_schema::{ArrowError, SchemaRef};
use jni::objects::GlobalRef;
use jni::JavaVM;
use std::sync::Arc;

/// A wrapper around ArrowArrayStreamReader that checks for cancellation before each batch.
///
/// This reader wraps an Arrow FFI stream and checks a Java AtomicBoolean before reading
/// each batch. If the AtomicBoolean is set to true, the reader returns an error indicating
/// that the operation was cancelled.
pub struct CancellableStreamReader {
    inner: ArrowArrayStreamReader,
    jvm: Arc<JavaVM>,
    cancellation_flag: GlobalRef,
}

impl CancellableStreamReader {
    /// Create a new CancellableStreamReader.
    ///
    /// # Arguments
    /// * `inner` - The underlying ArrowArrayStreamReader
    /// * `jvm` - Reference to the Java VM for making JNI calls
    /// * `cancellation_flag` - GlobalRef to a Java AtomicBoolean that signals cancellation
    pub fn new(
        inner: ArrowArrayStreamReader,
        jvm: Arc<JavaVM>,
        cancellation_flag: GlobalRef,
    ) -> Self {
        Self {
            inner,
            jvm,
            cancellation_flag,
        }
    }

    /// Check if the operation has been cancelled.
    fn is_cancelled(&self) -> bool {
        // Attach to the JVM to check the cancellation flag
        let Ok(mut env) = self.jvm.attach_current_thread() else {
            // If we can't attach, assume not cancelled and continue
            return false;
        };

        // Call AtomicBoolean.get() to check the flag
        let result = env.call_method(&self.cancellation_flag, "get", "()Z", &[]);
        match result {
            Ok(value) => value.z().unwrap_or(false),
            Err(_) => false,
        }
    }
}

impl Iterator for CancellableStreamReader {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        // Check for cancellation before reading the next batch
        if self.is_cancelled() {
            return Some(Err(ArrowError::ExternalError(Box::new(
                std::io::Error::new(
                    std::io::ErrorKind::Interrupted,
                    "Operation cancelled by user",
                ),
            ))));
        }

        self.inner.next()
    }
}

impl RecordBatchReader for CancellableStreamReader {
    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }
}

// Make CancellableStreamReader Send + Sync safe
// The GlobalRef is Send + Sync, and JavaVM is Send + Sync
unsafe impl Send for CancellableStreamReader {}
