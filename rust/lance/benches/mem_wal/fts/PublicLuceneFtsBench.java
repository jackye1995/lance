// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;

public class PublicLuceneFtsBench {
  private static final String TEXT_FIELD = "text";
  private static final String ID_FIELD = "row_id";

  private static final class InputQuery {
    final String id;
    final String text;

    InputQuery(String id, String text) {
      this.id = id;
      this.text = text;
    }
  }

  private static final class RunResult {
    final double p50Us;
    final double p95Us;
    final double p99Us;
    final double qps1;
    final double qpsN;

    RunResult(double p50Us, double p95Us, double p99Us, double qps1, double qpsN) {
      this.p50Us = p50Us;
      this.p95Us = p95Us;
      this.p99Us = p99Us;
      this.qps1 = qps1;
      this.qpsN = qpsN;
    }
  }

  private static String arg(String[] args, String name, String defaultValue) {
    for (int i = 0; i < args.length; i++) {
      if (args[i].equals(name)) {
        if (i + 1 == args.length) {
          throw new IllegalArgumentException("missing value for " + name);
        }
        return args[i + 1];
      }
    }
    return defaultValue;
  }

  private static String requiredArg(String[] args, String name) {
    String value = arg(args, name, null);
    if (value == null) {
      throw new IllegalArgumentException("required argument: " + name);
    }
    return value;
  }

  private static int intArg(String[] args, String name, int defaultValue) {
    int value = Integer.parseInt(arg(args, name, Integer.toString(defaultValue)));
    if (value <= 0) {
      throw new IllegalArgumentException(name + " must be positive");
    }
    return value;
  }

  private static List<InputQuery> readQueries(Path path) throws IOException {
    List<InputQuery> queries = new ArrayList<>();
    Set<String> ids = new HashSet<>();
    try (BufferedReader reader = Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
      String line;
      while ((line = reader.readLine()) != null) {
        int tab = line.indexOf('\t');
        if (tab <= 0) {
          throw new IllegalArgumentException("invalid query line: " + line);
        }
        String id = line.substring(0, tab);
        if (!ids.add(id)) {
          throw new IllegalArgumentException("duplicate query id: " + id);
        }
        queries.add(new InputQuery(id, line.substring(tab + 1)));
      }
    }
    if (queries.isEmpty()) {
      throw new IllegalArgumentException("query file is empty: " + path);
    }
    return queries;
  }

  private static Query buildQuery(Analyzer analyzer, String text) throws IOException {
    List<String> tokens = new ArrayList<>();
    try (TokenStream stream = analyzer.tokenStream(TEXT_FIELD, text)) {
      CharTermAttribute term = stream.addAttribute(CharTermAttribute.class);
      stream.reset();
      while (stream.incrementToken()) {
        tokens.add(term.toString());
      }
      stream.end();
    }
    if (tokens.isEmpty()) {
      return new MatchNoDocsQuery();
    }
    if (tokens.size() == 1) {
      return new TermQuery(new Term(TEXT_FIELD, tokens.get(0)));
    }
    BooleanQuery.Builder builder = new BooleanQuery.Builder();
    for (String token : tokens) {
      builder.add(new TermQuery(new Term(TEXT_FIELD, token)), BooleanClause.Occur.SHOULD);
    }
    return builder.build();
  }

  private static long buildIndex(
      Path corpus, Directory directory, Analyzer analyzer, FieldType textFieldType)
      throws IOException {
    IndexWriterConfig config = new IndexWriterConfig(analyzer);
    config.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
    config.setSimilarity(new BM25Similarity(1.2f, 0.75f));
    long documentCount = 0;
    try (IndexWriter writer = new IndexWriter(directory, config);
        BufferedReader reader = Files.newBufferedReader(corpus, StandardCharsets.UTF_8)) {
      String line;
      while ((line = reader.readLine()) != null) {
        Document document = new Document();
        document.add(new StoredField(ID_FIELD, documentCount));
        document.add(new Field(TEXT_FIELD, line, textFieldType));
        writer.addDocument(document);
        documentCount++;
      }
      writer.commit();
    }
    if (documentCount == 0) {
      throw new IllegalArgumentException("corpus file is empty: " + corpus);
    }
    return documentCount;
  }

  private static void runSweep(
      IndexSearcher searcher, Analyzer analyzer, List<InputQuery> queries, int k)
      throws IOException {
    for (InputQuery input : queries) {
      searcher.search(buildQuery(analyzer, input.text), k);
    }
  }

  private static double percentile(double[] values, double percentile) {
    double[] sorted = values.clone();
    Arrays.sort(sorted);
    int index = Math.max(0, (int) Math.ceil(percentile * sorted.length) - 1);
    return sorted[index];
  }

  private static double runParallelSweep(
      IndexSearcher searcher,
      Analyzer analyzer,
      List<InputQuery> queries,
      int k,
      int threads,
      ExecutorService executor)
      throws Exception {
    long started = System.nanoTime();
    List<Future<?>> futures = new ArrayList<>();
    for (int thread = 0; thread < threads; thread++) {
      final int offset = thread;
      futures.add(
          executor.submit(
              () -> {
                for (int i = offset; i < queries.size(); i += threads) {
                  InputQuery input = queries.get(i);
                  searcher.search(buildQuery(analyzer, input.text), k);
                }
                return null;
              }));
    }
    for (Future<?> future : futures) {
      future.get();
    }
    return queries.size() / ((System.nanoTime() - started) / 1.0e9);
  }

  private static long indexBytes(Directory directory) throws IOException {
    long bytes = 0;
    for (String file : directory.listAll()) {
      bytes += directory.fileLength(file);
    }
    return bytes;
  }

  private static long prewarmIndex(Directory directory) throws IOException {
    byte[] buffer = new byte[8 * 1024 * 1024];
    long bytes = 0;
    for (String file : directory.listAll()) {
      try (IndexInput input = directory.openInput(file, IOContext.READONCE)) {
        long remaining = input.length();
        while (remaining > 0) {
          int chunk = (int) Math.min(buffer.length, remaining);
          input.readBytes(buffer, 0, chunk);
          remaining -= chunk;
          bytes += chunk;
        }
      }
    }
    return bytes;
  }

  private static void writeTopK(
      Path output,
      IndexSearcher searcher,
      Analyzer analyzer,
      List<InputQuery> queries,
      int qualityK)
      throws IOException {
    Path parent = output.getParent();
    if (parent != null) {
      Files.createDirectories(parent);
    }
    StoredFields storedFields = searcher.storedFields();
    try (BufferedWriter writer =
        Files.newBufferedWriter(
            output,
            StandardCharsets.UTF_8,
            StandardOpenOption.CREATE,
            StandardOpenOption.TRUNCATE_EXISTING)) {
      for (InputQuery input : queries) {
        TopDocs topDocs = searcher.search(buildQuery(analyzer, input.text), qualityK);
        writer.write(input.id);
        writer.write('\t');
        boolean first = true;
        for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
          long rowId =
              storedFields.document(scoreDoc.doc).getField(ID_FIELD).numericValue().longValue();
          if (!first) {
            writer.write(' ');
          }
          writer.write(Long.toString(rowId));
          first = false;
        }
        writer.newLine();
      }
    }
  }

  private static String json(
      long documents,
      int queryCount,
      int k,
      int qualityK,
      int threads,
      int warmupRounds,
      int measuredRuns,
      double buildSeconds,
      long indexBytes,
      String directoryImpl,
      long indexPrewarmBytes,
      double indexPrewarmSeconds,
      List<RunResult> runs) {
    StringBuilder out = new StringBuilder();
    out.append('{');
    out.append("\"impl\":\"lucene\"");
    out.append(",\"documents\":").append(documents);
    out.append(",\"query_count\":").append(queryCount);
    out.append(",\"k\":").append(k);
    out.append(",\"quality_k\":").append(qualityK);
    out.append(",\"threads\":").append(threads);
    out.append(",\"warmup_rounds\":").append(warmupRounds);
    out.append(",\"measured_runs\":").append(measuredRuns);
    out.append(String.format(Locale.ROOT, ",\"build_seconds\":%.6f", buildSeconds));
    out.append(
        String.format(
            Locale.ROOT,
            ",\"build_docs_per_second\":%.3f",
            documents / buildSeconds));
    out.append(",\"index_bytes\":").append(indexBytes);
    out.append(",\"query_cache\":{");
    out.append("\"mode\":\"lucene_directory_full_index_read\"");
    out.append(",\"directory_impl\":\"").append(directoryImpl).append("\"");
    out.append(",\"index_prewarm_bytes\":").append(indexPrewarmBytes);
    out.append(
        String.format(
            Locale.ROOT,
            ",\"index_prewarm_seconds\":%.6f",
            indexPrewarmSeconds));
    out.append(",\"prewarm_completed\":true}");
    out.append(",\"runs\":[");
    for (int i = 0; i < runs.size(); i++) {
      if (i > 0) {
        out.append(',');
      }
      RunResult run = runs.get(i);
      out.append(
          String.format(
              Locale.ROOT,
              "{\"repetition\":%d,\"p50_us\":%.3f,\"p95_us\":%.3f,\"p99_us\":%.3f,\"qps_1\":%.3f,\"qps_n\":%.3f}",
              i + 1,
              run.p50Us,
              run.p95Us,
              run.p99Us,
              run.qps1,
              run.qpsN));
    }
    out.append("]}");
    return out.toString();
  }

  public static void main(String[] args) throws Exception {
    Path corpus = Paths.get(requiredArg(args, "--corpus"));
    Path queriesPath = Paths.get(requiredArg(args, "--queries"));
    Path indexPath = Paths.get(requiredArg(args, "--index-dir"));
    Path output = Paths.get(requiredArg(args, "--output"));
    Path topKFile = Paths.get(requiredArg(args, "--topk-file"));
    int k = intArg(args, "--k", 10);
    int qualityK = intArg(args, "--quality-k", 1000);
    int threads = intArg(args, "--threads", Runtime.getRuntime().availableProcessors());
    int warmupRounds = intArg(args, "--warmup-rounds", 1);
    int measuredRuns = intArg(args, "--measured-runs", 3);

    Files.createDirectories(indexPath);
    List<InputQuery> queries = readQueries(queriesPath);
    FieldType textFieldType = new FieldType();
    textFieldType.setTokenized(true);
    textFieldType.setOmitNorms(false);
    textFieldType.setIndexOptions(IndexOptions.DOCS_AND_FREQS);
    textFieldType.freeze();
    try (Analyzer analyzer = new WhitespaceAnalyzer();
        Directory directory = FSDirectory.open(indexPath)) {
      long buildStarted = System.nanoTime();
      long documents = buildIndex(corpus, directory, analyzer, textFieldType);
      double buildSeconds = (System.nanoTime() - buildStarted) / 1.0e9;
      long bytes = indexBytes(directory);
      long prewarmStarted = System.nanoTime();
      long prewarmBytes = prewarmIndex(directory);
      double prewarmSeconds = (System.nanoTime() - prewarmStarted) / 1.0e9;

      try (DirectoryReader reader = DirectoryReader.open(directory)) {
        IndexSearcher searcher = new IndexSearcher(reader);
        searcher.setSimilarity(new BM25Similarity(1.2f, 0.75f));

        for (int round = 0; round < warmupRounds; round++) {
          runSweep(searcher, analyzer, queries, k);
        }

        List<RunResult> runs = new ArrayList<>();
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        try {
          for (int repetition = 0; repetition < measuredRuns; repetition++) {
            double[] latencies = new double[queries.size()];
            long sweepStarted = System.nanoTime();
            for (int i = 0; i < queries.size(); i++) {
              long queryStarted = System.nanoTime();
              InputQuery input = queries.get(i);
              searcher.search(buildQuery(analyzer, input.text), k);
              latencies[i] = (System.nanoTime() - queryStarted) / 1.0e3;
            }
            double qps1 = queries.size() / ((System.nanoTime() - sweepStarted) / 1.0e9);
            double qpsN =
                runParallelSweep(searcher, analyzer, queries, k, threads, executor);
            runs.add(
                new RunResult(
                    percentile(latencies, 0.50),
                    percentile(latencies, 0.95),
                    percentile(latencies, 0.99),
                    qps1,
                    qpsN));
          }
        } finally {
          executor.shutdown();
        }

        writeTopK(topKFile, searcher, analyzer, queries, qualityK);
        String result =
            json(
                documents,
                queries.size(),
                k,
                qualityK,
                threads,
                warmupRounds,
                measuredRuns,
                buildSeconds,
                bytes,
                directory.getClass().getName(),
                prewarmBytes,
                prewarmSeconds,
                runs);
        Path parent = output.getParent();
        if (parent != null) {
          Files.createDirectories(parent);
        }
        Files.writeString(
            output,
            result + System.lineSeparator(),
            StandardCharsets.UTF_8,
            StandardOpenOption.CREATE,
            StandardOpenOption.TRUNCATE_EXISTING);
        System.out.println(result);
      }
    }
  }
}
