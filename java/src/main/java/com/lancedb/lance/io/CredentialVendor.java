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
package com.lancedb.lance.io;

import java.util.Map;

/**
 * Interface for providing cloud storage credentials to Lance datasets.
 *
 * <p>Credential vendors enable automatic credential refresh for long-running operations on cloud
 * storage (S3, Azure, GCS). Implement this interface to integrate with custom credential management
 * systems such as AWS STS, GCP STS, or proprietary credential services.
 *
 * <p>The vendor is called automatically before credentials expire, ensuring uninterrupted access
 * during long-running queries, training jobs, or data processing.
 *
 * <h2>Example Implementation</h2>
 *
 * <pre>{@code
 * public class MyCredentialVendor implements CredentialVendor {
 *   public Map<String, String> getCredentials() {
 *     // Fetch from your credential service
 *     Map<String, String> credentials = new HashMap<>();
 *     credentials.put("aws_access_key_id", "ASIA...");
 *     credentials.put("aws_secret_access_key", "secret");
 *     credentials.put("aws_session_token", "token");
 *
 *     long expiresAtMillis = System.currentTimeMillis() + 3600000L;
 *     credentials.put("expires_at_millis", String.valueOf(expiresAtMillis));
 *
 *     return credentials;
 *   }
 * }
 *
 * // Use with dataset
 * CredentialVendor vendor = new MyCredentialVendor();
 * Dataset dataset = Dataset.open(
 *     "s3://bucket/table.lance",
 *     new ReadOptions.Builder()
 *         .setCredentialVendor(vendor)
 *         .build()
 * );
 * }</pre>
 *
 * <h2>Error Handling</h2>
 *
 * <p>If getCredentials() throws an exception, operations requiring credentials will fail.
 * Implementations should handle recoverable errors internally (e.g., retry token refresh) and only
 * throw exceptions for unrecoverable errors.
 */
public interface CredentialVendor {

  /**
   * Get fresh storage credentials.
   *
   * <p>This method is called automatically before each request and before existing credentials
   * expire. It must return credentials in the format described below.
   *
   * @return Map of string key-value pairs containing cloud storage credentials and expiration time.
   *     Required key:
   *     <ul>
   *       <li>"expires_at_millis" (String): Unix timestamp in milliseconds (as string) when
   *           credentials expire. Lance will automatically call getCredentials() again before this
   *           time.
   *     </ul>
   *     Plus provider-specific credential keys:
   *     <ul>
   *       <li>AWS S3: "aws_access_key_id", "aws_secret_access_key", "aws_session_token" (optional)
   *       <li>Azure Blob Storage: "account_name", "account_key" or "sas_token"
   *       <li>Google Cloud Storage: "service_account_key" or "token"
   *     </ul>
   *
   * @throws RuntimeException if unable to fetch credentials
   */
  Map<String, String> getCredentials();
}
