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

import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 * Main entry point to run all Lance vs Parquet benchmarks.
 *
 * <p>Usage:
 *
 * <pre>
 * # Build with release mode for accurate benchmarks
 * cd java
 * ./mvnw clean package -DskipTests -Drust.release.build=true
 *
 * # Run all benchmarks
 * java -cp "target/classes:target/test-classes:$(mvnw dependency:build-classpath -q -DincludeScope=test -Dmdep.outputFile=/dev/stdout)" \
 *   org.lance.benchmark.BenchmarkRunner
 *
 * # Or run with specific parameters
 * java -cp ... org.lance.benchmark.BenchmarkRunner --numRows 100000 --compression ZSTD
 * </pre>
 */
public class BenchmarkRunner {

  public static void main(String[] args) throws RunnerException {
    String numRows = "100000";
    String compression = null;
    String include = null;

    for (int i = 0; i < args.length; i++) {
      if ("--numRows".equals(args[i]) && i + 1 < args.length) {
        numRows = args[++i];
      } else if ("--compression".equals(args[i]) && i + 1 < args.length) {
        compression = args[++i];
      } else if ("--include".equals(args[i]) && i + 1 < args.length) {
        include = args[++i];
      }
    }

    ChainedOptionsBuilder builder =
        new OptionsBuilder().resultFormat(ResultFormatType.JSON).result("benchmark-results.json");

    if (include != null) {
      builder = builder.include(include);
    } else {
      builder = builder.include(LanceBenchmark.class.getSimpleName());
      builder = builder.include(ParquetBenchmark.class.getSimpleName());
    }

    builder = builder.param("numRows", numRows);

    if (compression != null) {
      builder = builder.param("compression", compression);
    }

    Options opt = builder.build();

    String separator = "============================================================";
    System.out.println(separator);
    System.out.println("Lance vs Parquet Benchmark");
    System.out.println(separator);
    System.out.println("Parameters:");
    System.out.println("  numRows: " + numRows);
    System.out.println("  compression: " + (compression != null ? compression : "all"));
    System.out.println(separator);

    new Runner(opt).run();
  }
}
