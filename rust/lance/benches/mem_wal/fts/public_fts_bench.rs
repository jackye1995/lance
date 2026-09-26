// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

#![allow(clippy::print_stdout)]

use std::error::Error;
use std::fs::{self, File};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path as FsPath, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use arrow_array::{RecordBatch, StringArray, UInt64Array};
use arrow_schema::{DataType, Field as ArrowField, Schema as ArrowSchema};
use datafusion::error::DataFusionError;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures::{StreamExt, TryStreamExt, stream};
use lance_core::ROW_ID;
use lance_core::cache::LanceCache;
use lance_index::metrics::NoOpMetricsCollector;
use lance_index::prefilter::NoFilter;
use lance_index::scalar::InvertedIndexParams;
use lance_index::scalar::inverted::query::{FtsSearchParams, Operator, try_collect_query_tokens};
use lance_index::scalar::inverted::{InvertedIndex, InvertedIndexBuilder};
use lance_index::scalar::lance_format::LanceIndexStore;
use lance_io::object_store::ObjectStore;
use object_store::path::Path;
use rayon::prelude::*;
use serde::Serialize;
use serde_json::json;
use tantivy::collector::TopDocs;
use tantivy::query::{BooleanQuery, Occur, Query, TermQuery};
use tantivy::schema::{
    Field, IndexRecordOption, STORED, Schema, TextFieldIndexing, TextOptions, Value,
};
use tantivy::tokenizer::{TextAnalyzer, WhitespaceTokenizer};
use tantivy::{Index, TantivyDocument, Term, doc};
use tokio_stream::wrappers::ReceiverStream;

type AnyResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

const TEXT_COL: &str = "text";
const TOKENIZER: &str = "public_simple";

#[derive(Clone, Copy, Debug)]
enum Engine {
    Lance,
    Tantivy,
}

impl Engine {
    fn parse(value: &str) -> AnyResult<Self> {
        match value {
            "lance" => Ok(Self::Lance),
            "tantivy" => Ok(Self::Tantivy),
            _ => Err(format!("unknown engine {value:?}; expected lance|tantivy").into()),
        }
    }
}

#[derive(Debug)]
struct Args {
    engine: Engine,
    corpus: PathBuf,
    queries: PathBuf,
    index_dir: PathBuf,
    output: PathBuf,
    topk_file: PathBuf,
    k: usize,
    quality_k: usize,
    threads: usize,
    warmup_rounds: usize,
    measured_runs: usize,
    batch_rows: usize,
}

#[derive(Clone, Debug)]
struct InputQuery {
    id: String,
    text: String,
}

#[derive(Clone, Debug, Serialize)]
struct RunStats {
    repetition: usize,
    p50_us: f64,
    p95_us: f64,
    p99_us: f64,
    qps_1: f64,
    qps_n: f64,
}

fn parse_args() -> AnyResult<Args> {
    let mut values = std::env::args().skip(1);
    let mut engine = None;
    let mut corpus = None;
    let mut queries = None;
    let mut index_dir = None;
    let mut output = None;
    let mut topk_file = None;
    let mut k = 10;
    let mut quality_k = 1_000;
    let mut threads = std::thread::available_parallelism()?.get();
    let mut warmup_rounds = 1;
    let mut measured_runs = 3;
    let mut batch_rows = 8_192;
    while let Some(flag) = values.next() {
        let value = values
            .next()
            .ok_or_else(|| format!("missing value for {flag}"))?;
        match flag.as_str() {
            "--engine" => engine = Some(Engine::parse(&value)?),
            "--corpus" => corpus = Some(value.into()),
            "--queries" => queries = Some(value.into()),
            "--index-dir" => index_dir = Some(value.into()),
            "--output" => output = Some(value.into()),
            "--topk-file" => topk_file = Some(value.into()),
            "--k" => k = value.parse()?,
            "--quality-k" => quality_k = value.parse()?,
            "--threads" => threads = value.parse()?,
            "--warmup-rounds" => warmup_rounds = value.parse()?,
            "--measured-runs" => measured_runs = value.parse()?,
            "--batch-rows" => batch_rows = value.parse()?,
            _ => return Err(format!("unknown argument {flag}").into()),
        }
    }
    let args = Args {
        engine: engine.ok_or("--engine is required")?,
        corpus: corpus.ok_or("--corpus is required")?,
        queries: queries.ok_or("--queries is required")?,
        index_dir: index_dir.ok_or("--index-dir is required")?,
        output: output.ok_or("--output is required")?,
        topk_file: topk_file.ok_or("--topk-file is required")?,
        k,
        quality_k,
        threads,
        warmup_rounds,
        measured_runs,
        batch_rows,
    };
    if args.k == 0
        || args.quality_k < args.k
        || args.threads == 0
        || args.warmup_rounds == 0
        || args.measured_runs == 0
        || args.batch_rows == 0
    {
        return Err("k, threads, warmup-rounds, measured-runs, and batch-rows must be positive; quality-k must be at least k".into());
    }
    Ok(args)
}

fn read_queries(path: &FsPath) -> AnyResult<Vec<InputQuery>> {
    let reader = BufReader::new(File::open(path)?);
    let mut queries = Vec::new();
    for (line_number, line) in reader.lines().enumerate() {
        let line = line?;
        let (id, text) = line.split_once('\t').ok_or_else(|| {
            format!(
                "{}:{}: expected qid<TAB>query",
                path.display(),
                line_number + 1
            )
        })?;
        if id.is_empty() || text.is_empty() {
            return Err(format!(
                "{}:{}: empty query id or text",
                path.display(),
                line_number + 1
            )
            .into());
        }
        queries.push(InputQuery {
            id: id.to_owned(),
            text: text.to_owned(),
        });
    }
    if queries.is_empty() {
        return Err(format!("{} contained no queries", path.display()).into());
    }
    Ok(queries)
}

fn percentile(sorted: &[f64], pct: f64) -> f64 {
    let rank = ((pct / 100.0) * sorted.len() as f64).ceil() as usize;
    let index = rank.saturating_sub(1);
    sorted[index.min(sorted.len() - 1)]
}

fn summarize(repetition: usize, mut latencies_us: Vec<f64>, elapsed: f64, qps_nt: f64) -> RunStats {
    latencies_us.sort_by(|left, right| left.total_cmp(right));
    RunStats {
        repetition,
        p50_us: percentile(&latencies_us, 50.0),
        p95_us: percentile(&latencies_us, 95.0),
        p99_us: percentile(&latencies_us, 99.0),
        qps_1: latencies_us.len() as f64 / elapsed,
        qps_n: qps_nt,
    }
}

fn dir_bytes(path: &FsPath) -> AnyResult<u64> {
    let mut total = 0;
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let metadata = entry.metadata()?;
        total += if metadata.is_dir() {
            dir_bytes(&entry.path())?
        } else {
            metadata.len()
        };
    }
    Ok(total)
}

fn ensure_empty_index_dir(path: &FsPath) -> AnyResult<()> {
    if path.exists() && fs::read_dir(path)?.next().is_some() {
        return Err(format!("index directory is not empty: {}", path.display()).into());
    }
    fs::create_dir_all(path)?;
    Ok(())
}

fn corpus_stream(
    path: PathBuf,
    batch_rows: usize,
) -> (
    datafusion::physical_plan::SendableRecordBatchStream,
    Arc<AtomicU64>,
) {
    let schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new(TEXT_COL, DataType::Utf8, false),
        ArrowField::new(ROW_ID, DataType::UInt64, false),
    ]));
    let producer_schema = schema.clone();
    let document_count = Arc::new(AtomicU64::new(0));
    let producer_count = document_count.clone();
    let (sender, receiver) = tokio::sync::mpsc::channel(2);
    tokio::task::spawn_blocking(move || {
        let result = (|| -> AnyResult<()> {
            let reader = BufReader::new(File::open(&path)?);
            let mut docs = Vec::with_capacity(batch_rows);
            let mut row_ids = Vec::with_capacity(batch_rows);
            let mut next_row_id = 0_u64;
            for line in reader.lines() {
                docs.push(line?);
                row_ids.push(next_row_id);
                next_row_id += 1;
                producer_count.fetch_add(1, Ordering::Relaxed);
                if docs.len() == batch_rows {
                    let batch = make_batch(&producer_schema, &mut docs, &mut row_ids)?;
                    sender
                        .blocking_send(Ok(batch))
                        .map_err(|_| "index builder closed input")?;
                }
            }
            if !docs.is_empty() {
                let batch = make_batch(&producer_schema, &mut docs, &mut row_ids)?;
                sender
                    .blocking_send(Ok(batch))
                    .map_err(|_| "index builder closed input")?;
            }
            Ok(())
        })();
        if let Err(error) = result {
            let _ = sender.blocking_send(Err(DataFusionError::External(error)));
        }
    });
    (
        Box::pin(RecordBatchStreamAdapter::new(
            schema,
            ReceiverStream::new(receiver),
        )),
        document_count,
    )
}

fn make_batch(
    schema: &Arc<ArrowSchema>,
    docs: &mut Vec<String>,
    row_ids: &mut Vec<u64>,
) -> AnyResult<RecordBatch> {
    let docs = std::mem::take(docs);
    let row_ids = std::mem::take(row_ids);
    Ok(RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(docs)),
            Arc::new(UInt64Array::from(row_ids)),
        ],
    )?)
}

async fn lance_search(index: Arc<InvertedIndex>, text: String, k: usize) -> AnyResult<Vec<u64>> {
    let mut tokenizer = index.tokenizer();
    let tokens = Arc::new(try_collect_query_tokens(&text, &mut tokenizer)?);
    let params = Arc::new(FtsSearchParams::new().with_limit(Some(k)));
    let (row_ids, _) = index
        .bm25_search(
            tokens,
            params,
            Operator::Or,
            Arc::new(NoFilter),
            Arc::new(NoOpMetricsCollector),
            None,
        )
        .await?;
    Ok(row_ids)
}

async fn lance_parallel_qps(
    index: Arc<InvertedIndex>,
    queries: &[InputQuery],
    k: usize,
    threads: usize,
) -> AnyResult<f64> {
    let started = Instant::now();
    stream::iter(queries.iter().map(|query| {
        let index = index.clone();
        let text = query.text.clone();
        async move { lance_search(index, text, k).await.map(|rows| rows.len()) }
    }))
    .buffer_unordered(threads)
    .try_collect::<Vec<_>>()
    .await?;
    Ok(queries.len() as f64 / started.elapsed().as_secs_f64())
}

async fn run_lance(args: &Args, queries: &[InputQuery]) -> AnyResult<serde_json::Value> {
    ensure_empty_index_dir(&args.index_dir)?;
    let store = Arc::new(LanceIndexStore::new(
        Arc::new(ObjectStore::local()),
        Path::from_filesystem_path(&args.index_dir)?,
        Arc::new(LanceCache::no_cache()),
    ));
    let params = InvertedIndexParams::default()
        .base_tokenizer("whitespace".to_owned())
        .with_position(false)
        .lower_case(false)
        .stem(false)
        .remove_stop_words(false)
        .ascii_folding(false)
        .max_token_length(None);
    let mut builder = InvertedIndexBuilder::new(params);
    let (input, document_count) = corpus_stream(args.corpus.clone(), args.batch_rows);
    let build_started = Instant::now();
    let files = builder.update(input, store.as_ref(), None).await?;
    let build_s = build_started.elapsed().as_secs_f64();
    let index_bytes: u64 = files.iter().map(|file| file.size_bytes).sum();
    let docs = document_count.load(Ordering::Relaxed);
    if docs == 0 {
        return Err(format!("{} contained no documents", args.corpus.display()).into());
    }
    let index = InvertedIndex::load(store, None, &LanceCache::no_cache()).await?;

    for _ in 0..args.warmup_rounds {
        for query in queries {
            lance_search(index.clone(), query.text.clone(), args.k).await?;
        }
    }

    let mut measured = Vec::with_capacity(args.measured_runs);
    for repetition in 1..=args.measured_runs {
        let started = Instant::now();
        let mut latencies = Vec::with_capacity(queries.len());
        for query in queries {
            let query_started = Instant::now();
            lance_search(index.clone(), query.text.clone(), args.k).await?;
            latencies.push(query_started.elapsed().as_secs_f64() * 1.0e6);
        }
        let elapsed = started.elapsed().as_secs_f64();
        let qps_nt = lance_parallel_qps(index.clone(), queries, args.k, args.threads).await?;
        measured.push(summarize(repetition, latencies, elapsed, qps_nt));
    }

    let mut topk = BufWriter::new(File::create(&args.topk_file)?);
    for query in queries {
        let rows = lance_search(index.clone(), query.text.clone(), args.quality_k).await?;
        let values = rows
            .iter()
            .map(u64::to_string)
            .collect::<Vec<_>>()
            .join(" ");
        writeln!(topk, "{}\t{}", query.id, values)?;
    }
    topk.flush()?;
    Ok(json!({
        "impl": "lance",
        "mode": "direct_persisted_index",
        "documents": docs,
        "query_count": queries.len(),
        "k": args.k,
        "quality_k": args.quality_k,
        "threads": args.threads,
        "warmup_rounds": args.warmup_rounds,
        "measured_runs": args.measured_runs,
        "build_seconds": build_s,
        "build_docs_per_second": docs as f64 / build_s,
        "index_bytes": index_bytes,
        "runs": measured,
    }))
}

fn tantivy_schema() -> (Schema, Field, Field) {
    let mut schema = Schema::builder();
    let id = schema.add_u64_field("id", STORED);
    let indexing = TextFieldIndexing::default()
        .set_tokenizer(TOKENIZER)
        .set_index_option(IndexRecordOption::WithFreqs);
    let text = schema.add_text_field(
        TEXT_COL,
        TextOptions::default().set_indexing_options(indexing),
    );
    (schema.build(), id, text)
}

fn register_tantivy_tokenizer(index: &Index) {
    let analyzer = TextAnalyzer::builder(WhitespaceTokenizer::default()).build();
    index.tokenizers().register(TOKENIZER, analyzer);
}

fn tantivy_query(index: &Index, text_field: Field, text: &str) -> Box<dyn Query> {
    let mut analyzer = index.tokenizers().get(TOKENIZER).unwrap();
    let mut stream = analyzer.token_stream(text);
    let mut terms = Vec::new();
    while stream.advance() {
        terms.push(Term::from_field_text(text_field, &stream.token().text));
    }
    if terms.is_empty() {
        return Box::new(TermQuery::new(
            Term::from_field_text(text_field, "__no_such_token__"),
            IndexRecordOption::WithFreqs,
        ));
    }
    if terms.len() == 1 {
        return Box::new(TermQuery::new(
            terms.pop().unwrap(),
            IndexRecordOption::WithFreqs,
        ));
    }
    Box::new(BooleanQuery::new(
        terms
            .into_iter()
            .map(|term| {
                (
                    Occur::Should,
                    Box::new(TermQuery::new(term, IndexRecordOption::WithFreqs)) as Box<dyn Query>,
                )
            })
            .collect(),
    ))
}

fn tantivy_search(
    index: &Index,
    searcher: &tantivy::Searcher,
    id_field: Field,
    text_field: Field,
    text: &str,
    k: usize,
) -> AnyResult<Vec<u64>> {
    let query = tantivy_query(index, text_field, text);
    let hits = searcher.search(query.as_ref(), &TopDocs::with_limit(k))?;
    let mut rows = Vec::with_capacity(hits.len());
    for (_, address) in hits {
        let document: TantivyDocument = searcher.doc(address)?;
        rows.push(
            document
                .get_first(id_field)
                .and_then(|value| value.as_u64())
                .ok_or("tantivy result missing stored id")?,
        );
    }
    Ok(rows)
}

fn tantivy_search_count(
    index: &Index,
    searcher: &tantivy::Searcher,
    text_field: Field,
    text: &str,
    k: usize,
) -> AnyResult<usize> {
    let query = tantivy_query(index, text_field, text);
    Ok(searcher
        .search(query.as_ref(), &TopDocs::with_limit(k))?
        .len())
}

fn run_tantivy(args: &Args, queries: &[InputQuery]) -> AnyResult<serde_json::Value> {
    ensure_empty_index_dir(&args.index_dir)?;
    rayon::ThreadPoolBuilder::new()
        .num_threads(args.threads)
        .build_global()?;
    let (schema, id_field, text_field) = tantivy_schema();
    let index = Index::create_in_dir(&args.index_dir, schema)?;
    register_tantivy_tokenizer(&index);
    let build_started = Instant::now();
    let mut writer = index.writer_with_num_threads(args.threads.min(8), 1 << 30)?;
    let reader = BufReader::new(File::open(&args.corpus)?);
    let mut docs = 0_u64;
    for line in reader.lines() {
        writer.add_document(doc!(id_field => docs, text_field => line?))?;
        docs += 1;
    }
    writer.commit()?;
    drop(writer);
    let build_s = build_started.elapsed().as_secs_f64();
    let reader = index.reader()?;
    let searcher = reader.searcher();

    for _ in 0..args.warmup_rounds {
        for query in queries {
            tantivy_search_count(&index, &searcher, text_field, &query.text, args.k)?;
        }
    }

    let mut measured = Vec::with_capacity(args.measured_runs);
    for repetition in 1..=args.measured_runs {
        let started = Instant::now();
        let mut latencies = Vec::with_capacity(queries.len());
        for query in queries {
            let query_started = Instant::now();
            tantivy_search_count(&index, &searcher, text_field, &query.text, args.k)?;
            latencies.push(query_started.elapsed().as_secs_f64() * 1.0e6);
        }
        let elapsed = started.elapsed().as_secs_f64();
        let parallel_started = Instant::now();
        queries.par_iter().try_for_each(|query| {
            tantivy_search_count(&index, &searcher, text_field, &query.text, args.k).map(|_| ())
        })?;
        let qps_nt = queries.len() as f64 / parallel_started.elapsed().as_secs_f64();
        measured.push(summarize(repetition, latencies, elapsed, qps_nt));
    }

    let mut topk = BufWriter::new(File::create(&args.topk_file)?);
    for query in queries {
        let rows = tantivy_search(
            &index,
            &searcher,
            id_field,
            text_field,
            &query.text,
            args.quality_k,
        )?;
        let values = rows
            .iter()
            .map(u64::to_string)
            .collect::<Vec<_>>()
            .join(" ");
        writeln!(topk, "{}\t{}", query.id, values)?;
    }
    topk.flush()?;
    Ok(json!({
        "impl": "tantivy",
        "mode": "direct_persisted_index",
        "documents": docs,
        "query_count": queries.len(),
        "k": args.k,
        "quality_k": args.quality_k,
        "threads": args.threads,
        "warmup_rounds": args.warmup_rounds,
        "measured_runs": args.measured_runs,
        "build_seconds": build_s,
        "build_docs_per_second": docs as f64 / build_s,
        "index_bytes": dir_bytes(&args.index_dir)?,
        "runs": measured,
    }))
}

#[tokio::main]
async fn main() -> AnyResult<()> {
    let args = parse_args()?;
    let queries = read_queries(&args.queries)?;
    if let Some(parent) = args.output.parent() {
        fs::create_dir_all(parent)?;
    }
    if let Some(parent) = args.topk_file.parent() {
        fs::create_dir_all(parent)?;
    }
    let result = match args.engine {
        Engine::Lance => run_lance(&args, &queries).await?,
        Engine::Tantivy => run_tantivy(&args, &queries)?,
    };
    let encoded = serde_json::to_string_pretty(&result)?;
    fs::write(&args.output, format!("{encoded}\n"))?;
    println!("{encoded}");
    Ok(())
}
