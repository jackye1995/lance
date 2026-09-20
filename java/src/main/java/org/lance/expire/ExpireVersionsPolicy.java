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
package org.lance.expire;

import java.util.Optional;

/**
 * Policy for expiring dataset versions.
 *
 * <p>Expiring removes manifests and nothing else. Data files that only an expired version
 * referenced are left behind; the next cleanup reclaims them.
 *
 * <p>All fields are optional. Defaults deliberately live on the Rust side rather than here, so the
 * two cannot disagree. With neither a timestamp nor a version bound, nothing is expired.
 */
public class ExpireVersionsPolicy {
  private final Optional<Long> beforeTimestampMillis;
  private final Optional<Long> beforeVersion;
  private final Optional<Long> keepOnePerSeconds;
  private final Optional<Boolean> errorIfTaggedOldVersions;
  private final Optional<Long> deleteRateLimit;

  private ExpireVersionsPolicy(
      Optional<Long> beforeTimestampMillis,
      Optional<Long> beforeVersion,
      Optional<Long> keepOnePerSeconds,
      Optional<Boolean> errorIfTaggedOldVersions,
      Optional<Long> deleteRateLimit) {
    this.beforeTimestampMillis = beforeTimestampMillis;
    this.beforeVersion = beforeVersion;
    this.keepOnePerSeconds = keepOnePerSeconds;
    this.errorIfTaggedOldVersions = errorIfTaggedOldVersions;
    this.deleteRateLimit = deleteRateLimit;
  }

  public static Builder builder() {
    return new Builder();
  }

  public Optional<Long> getBeforeTimestampMillis() {
    return beforeTimestampMillis;
  }

  public Optional<Long> getBeforeVersion() {
    return beforeVersion;
  }

  public Optional<Long> getKeepOnePerSeconds() {
    return keepOnePerSeconds;
  }

  public Optional<Boolean> getErrorIfTaggedOldVersions() {
    return errorIfTaggedOldVersions;
  }

  public Optional<Long> getDeleteRateLimit() {
    return deleteRateLimit;
  }

  public static class Builder {
    private Optional<Long> beforeTimestampMillis = Optional.empty();
    private Optional<Long> beforeVersion = Optional.empty();
    private Optional<Long> keepOnePerSeconds = Optional.empty();
    private Optional<Boolean> errorIfTaggedOldVersions = Optional.empty();
    private Optional<Long> deleteRateLimit = Optional.empty();

    /**
     * Expire versions whose manifest was written before this time.
     *
     * <p>This is the manifest object's write time, not the commit timestamp recorded inside it. Use
     * {@link #withBeforeVersion} when an exact boundary matters.
     */
    public Builder withBeforeTimestampMillis(long beforeTimestampMillis) {
      this.beforeTimestampMillis = Optional.of(beforeTimestampMillis);
      return this;
    }

    /** Expire versions numbered below this. Exact, and needs no timestamps. */
    public Builder withBeforeVersion(long beforeVersion) {
      this.beforeVersion = Optional.of(beforeVersion);
      return this;
    }

    /**
     * Instead of expiring every version past the cutoff, keep the newest one in each bucket of this
     * width. 3600 keeps one version per hour, 86400 one per day.
     */
    public Builder withKeepOnePerSeconds(long keepOnePerSeconds) {
      this.keepOnePerSeconds = Optional.of(keepOnePerSeconds);
      return this;
    }

    /** Fail instead of silently keeping a tagged version the policy would expire. */
    public Builder withErrorIfTaggedOldVersions(boolean errorIfTaggedOldVersions) {
      this.errorIfTaggedOldVersions = Optional.of(errorIfTaggedOldVersions);
      return this;
    }

    /** Maximum delete requests per second. One request is one manifest. */
    public Builder withDeleteRateLimit(long deleteRateLimit) {
      this.deleteRateLimit = Optional.of(deleteRateLimit);
      return this;
    }

    public ExpireVersionsPolicy build() {
      return new ExpireVersionsPolicy(
          beforeTimestampMillis,
          beforeVersion,
          keepOnePerSeconds,
          errorIfTaggedOldVersions,
          deleteRateLimit);
    }
  }
}
