# Python API Guide

`Lance` is a columnar format that is specifically designed for efficient
multi-modal data processing.

## Lance Dataset

The core of Lance is the `LanceDataset` class. User can open a dataset
by using [`lance.dataset`](./py_modules.md#lance.dataset).

For complete API reference, see the [Python Modules](./py_modules.md) page.

### Basic IOs

The following functions are used to read and write data in Lance format.

#### Insert Data

::: lance.dataset.LanceDataset.insert
    options:
      show_root_heading: false

#### Scanner

::: lance.dataset.LanceDataset.scanner
    options:
      show_root_heading: false

#### Convert to Batches

::: lance.dataset.LanceDataset.to_batches
    options:
      show_root_heading: false

#### Convert to Table

::: lance.dataset.LanceDataset.to_table
    options:
      show_root_heading: false

### Random Access

Lance stands out with its super fast random access, unlike other
columnar formats.

#### Take Rows

::: lance.dataset.LanceDataset.take
    options:
      show_root_heading: false

#### Take Blobs

::: lance.dataset.LanceDataset.take_blobs
    options:
      show_root_heading: false

### Schema Evolution

Lance supports schema evolution, which means that you can add new
columns to the dataset cheaply.

#### Add Columns

::: lance.dataset.LanceDataset.add_columns
    options:
      show_root_heading: false

#### Drop Columns

::: lance.dataset.LanceDataset.drop_columns
    options:
      show_root_heading: false

### Indexing and Searching

#### Create Vector Index

::: lance.dataset.LanceDataset.create_index
    options:
      show_root_heading: false

#### Create Scalar Index

::: lance.dataset.LanceDataset.create_scalar_index
    options:
      show_root_heading: false

#### Drop Index

::: lance.dataset.LanceDataset.drop_index
    options:
      show_root_heading: false

## API Reference

More information can be found in the [API reference](./py_modules.md).
