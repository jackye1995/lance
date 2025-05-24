![Lance Logo](assets/lance_logo.png){: style="width: 400px"}

# Lance: modern columnar format for ML workloads

**Lance** is a columnar format that is easy and fast to version, query and train on. It's designed to be used with images, videos, 3D point clouds, audio and of course tabular data. It supports any POSIX file systems, and cloud storage like AWS S3 and Google Cloud Storage. 

The key features of Lance include:

- **High-performance random access:** 100x faster than Parquet.
- **Zero-copy schema evolution:** add and drop columns without copying the entire dataset.
- **Vector search:** find nearest neighbors in under 1 millisecond and combine OLAP-queries with vector search.
- **Ecosystem integrations:** Apache-Arrow, DuckDB and more on the way.

## Installation

You can install Lance via pip:

```bash
pip install pylance
```

For the latest features and bug fixes, you can install the preview version:

```bash
pip install --pre --extra-index-url https://pypi.fury.io/lancedb/ pylance
```

Preview releases receive the same level of testing as regular releases.

## Documentation Structure

### Introduction
- [Quickstart](notebooks/quickstart.md) - Get started with Lance quickly
- [Read and Write](introduction/read_and_write.md) - Basic operations
- [Schema Evolution](introduction/schema_evolution.md) - Evolving your data schema

### Advanced Usage
- [Lance Format Spec](format.md) - Technical specification
- [Blob API](blob.md) - Working with large binary objects
- [Tags](tags.md) - Tagging and metadata
- [Object Store Configuration](object_store.md) - Cloud storage setup
- [Distributed Write](distributed_write.md) - Distributed operations
- [Performance Guide](performance.md) - Optimization tips
- [Tokenizer](tokenizer.md) - Text tokenization
- [Extension Arrays](arrays.md) - Custom array types

### Integrations
- [Huggingface](integrations/huggingface.md) - ML model integration
- [Tensorflow](integrations/tensorflow.md) - TensorFlow integration
- [PyTorch](integrations/pytorch.md) - PyTorch integration
- [Ray](integrations/ray.md) - Distributed computing
- [Spark](integrations/spark.md) - Apache Spark integration

### Examples
- [Examples Overview](examples/examples.md) - All examples
- [API References](api/api.md) - Complete API documentation
- [Contributing](contributing.md) - How to contribute

## Getting Started

Ready to get started? Check out our [Quickstart guide](notebooks/quickstart.md) or explore the [examples](examples/examples.md) to see Lance in action!
