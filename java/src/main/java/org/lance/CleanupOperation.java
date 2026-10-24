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

import org.lance.cleanup.CleanupExplanation;
import org.lance.cleanup.CleanupPolicy;
import org.lance.cleanup.RemovalStats;

import org.apache.arrow.util.Preconditions;

import java.time.Duration;
import java.util.Optional;

/**
 * A cleanup operation with separate read-only explain and destructive execute actions.
 *
 * <p>This is not a deletion plan. Calling {@link #execute()} re-evaluates the current dataset and
 * reference state before deleting files.
 */
public class CleanupOperation {
  private final Dataset dataset;
  private final CleanupPolicy policy;
  private Optional<Long> deleteConcurrency = Optional.empty();
  private Optional<Long> deleteRateLimit = Optional.empty();
  private Optional<Long> maxCandidateFiles = Optional.empty();
  private Optional<Long> keepOnePerMillis = Optional.empty();
  private Optional<Long> olderThanMillis = Optional.empty();

  CleanupOperation(Dataset dataset, CleanupPolicy policy) {
    this.dataset = Preconditions.checkNotNull(dataset, "dataset cannot be null");
    this.policy = Preconditions.checkNotNull(policy, "policy cannot be null");
  }

  /**
   * Keep the newest eligible commit per UTC epoch-aligned period, breaking timestamp ties by
   * version. Tags, current/newer versions and branch dependencies remain protected, so a period may
   * contain additional protected versions. Defaults to retaining full history for two days; use
   * {@link #withOlderThan(Duration)} to configure the age, which must be at least two days. The
   * cutoff is relative to the hinted commit timestamp, or this dataset's commit if no hint exists.
   * The hinted version and every later version remain intact. This delay bounds exposure to delayed
   * hint writes; it does not make hint updates monotonic. Without this option, cleanup removes all
   * eligible versions as before.
   *
   * <p>Example: {@code dataset.cleanup(policy).withKeepOnePer(Duration.ofDays(1)).execute()}.
   *
   * @param period positive bucket width, with millisecond precision
   * @return this operation for chaining
   */
  public CleanupOperation withKeepOnePer(Duration period) {
    Preconditions.checkNotNull(period, "period cannot be null");
    long millis = period.toMillis();
    Preconditions.checkArgument(
        millis > 0 && period.equals(Duration.ofMillis(millis)),
        "keep_one_per must be positive with millisecond precision, got %s",
        period);
    this.keepOnePerMillis = Optional.of(millis);
    return this;
  }

  /**
   * Configure the age of versions eligible for cleanup, intersecting policy filters. With thinning
   * this defaults to, and must be at least, two days measured from the hinted commit (or this
   * dataset's commit without a hint). Otherwise it is measured from current time. Validated on
   * explain and execute.
   *
   * @param age non-negative duration with millisecond precision
   * @return this operation for chaining
   */
  public CleanupOperation withOlderThan(Duration age) {
    Preconditions.checkNotNull(age, "age cannot be null");
    long millis = age.toMillis();
    Preconditions.checkArgument(
        millis >= 0 && age.equals(Duration.ofMillis(millis)),
        "older_than must be non-negative with millisecond precision, got %s",
        age);
    this.olderThanMillis = Optional.of(millis);
    return this;
  }

  /**
   * Set the maximum in-flight file deletes shared with all cascaded branches.
   *
   * <p>Defaults to the object store's I/O parallelism. Must be positive (at most 2^61 - 1 on 64-bit
   * platforms), validated on execute.
   *
   * <p>Example: {@code dataset.cleanup(policy).withDeleteConcurrency(32).execute()}.
   *
   * @param deleteConcurrency maximum concurrent file deletes
   * @return this operation for chaining
   */
  public CleanupOperation withDeleteConcurrency(long deleteConcurrency) {
    this.deleteConcurrency = Optional.of(deleteConcurrency);
    return this;
  }

  /**
   * Set delete requests per second, shared with all cascaded branches.
   *
   * <p>Overrides the policy's rate. Must be positive, validated on execute. Without either setting,
   * deletes are not rate limited.
   *
   * <p>Example: {@code dataset.cleanup(policy).withDeleteRateLimit(100).execute()}.
   *
   * @param deleteRateLimit maximum delete requests per second
   * @return this operation for chaining
   */
  public CleanupOperation withDeleteRateLimit(long deleteRateLimit) {
    this.deleteRateLimit = Optional.of(deleteRateLimit);
    return this;
  }

  /**
   * Set the maximum number of candidate files included in the {@link #explain()} result.
   *
   * <p>Defaults to 1000 if not set. The aggregate {@link RemovalStats} returned by {@link
   * #explain()} still account for all files that would be removed regardless of this limit; only
   * the per-file {@code candidateFiles} list is truncated.
   *
   * @param maxCandidateFiles maximum number of candidate files to include; must be positive
   * @return this operation for chaining
   */
  public CleanupOperation withMaxCandidateFiles(long maxCandidateFiles) {
    Preconditions.checkArgument(
        maxCandidateFiles > 0, "maxCandidateFiles must be positive, got %s", maxCandidateFiles);
    this.maxCandidateFiles = Optional.of(maxCandidateFiles);
    return this;
  }

  /**
   * Explain what cleanup would remove without deleting files.
   *
   * @return cleanup explanation
   */
  public CleanupExplanation explain() {
    return dataset.explainCleanup(policy, maxCandidateFiles, keepOnePerMillis, olderThanMillis);
  }

  /**
   * Execute cleanup, re-evaluating the current dataset and reference state before deleting files.
   *
   * @return removal stats
   */
  public RemovalStats execute() {
    return dataset.executeCleanup(
        policy, deleteConcurrency, deleteRateLimit, keepOnePerMillis, olderThanMillis);
  }
}
