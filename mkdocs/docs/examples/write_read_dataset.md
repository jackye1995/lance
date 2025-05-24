# Writing and reading a dataset using Lance

In this example, we will write a simple lance dataset to disk. Then we
will read it and print out some basic properties like the schema and
sizes for each record batch in the dataset. The example uses only one
record batch, however it should work for larger datasets (multiple
record batches) as well.

## Writing the raw dataset

See the complete example in [write_read_ds.rs](https://github.com/lancedb/lance/blob/main/rust/lance/examples/write_read_ds.rs) on GitHub.

First we define a schema for our dataset, and create a record batch from
that schema. Next we iterate over the record batches (only one in this
case) and write them to disk. We also define the write parameters (set
to overwrite) and then write the dataset to disk.

## Reading a Lance dataset

Now that we have written the dataset to a new directory, we can read it
back and print out some basic properties.

See the complete example in [write_read_ds.rs](https://github.com/lancedb/lance/blob/main/rust/lance/examples/write_read_ds.rs) on GitHub.

First we open the dataset, and create a scanner object. We use it to
create a [batch_stream]{.title-ref} that will let us access each record
batch in the dataset. Then we iterate over the record batches and print
out the size and schema of each one.
