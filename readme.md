## Overview

`pgtransfer` is a simple CLI tool to copy specific column data from a source table with different schemas/columns.

The destination table won't be created before copying, you must create it beforehand using `psql` or similar. Also, this tool requires a reasonable understanding of the structure of your source and destination databases to ensure accurate mapping.

Data will be converted to string and then inserted building a simple `INSERT` sql query, so your table/column destination schema should (ideally) match the source one beforehand.

## Usage

`pgtransfer` uses environment variables for source `PG_FROM_DATABASE` and destination `PG_TO_DATABASE` database connections.
You can also use args `--from` and `--to` to accomplish the same.

```bash
export PG_FROM_DATABASE=postgres://user:password@localhost:5432/fromdatabase
export PG_TO_DATABASE=postgres://user:password@other_db:5432/todatabase
```

The mapping of source to destination tables and columns is done using a simple `source:destination` syntax. It also supports the addition of static `column=value` pairs that will be included in every row transferred.
A more 'complex' column mapping is demonstrated in the below example, with `metadata_jsons.id/name:collections.id/name` which shows how to map a column from a joined table in the source database, where we get the `name` of the collection based on the `id` from `collections` table.

## Example

```bash
pgtransfer --table collections:collections \
--col id:id --col metadata_jsons.id/name:collections.id/name \
--col project_id:project_id --col created_at:timestamp \
--static acolumn=some_data
```

In the above example, collections table from the source database is mapped to the collections table in the destination database. The `id` column in the source will be copied to the `id` column in the destination, same for `project_id`. `created_at` will be copied into `timestamp` column in the destination DB.
The `--static acolumn=some_data` option allows you to specify a static column and value that will be added to every row in the destination database.

This results in the following SQL queries:

Read from source:

```sql
SELECT collections.id, metadata_jsons.name AS metadata_jsons_id_name,
project_id, created_at, 'some_data' AS acolumn FROM collections
INNER JOIN metadata_jsons ON collections.id = metadata_jsons.id
```

Write to dest:

```sql
INSERT INTO collections (id, name, project_id, timestamp, acolumn)
VALUES ('2f7f1a90-f6a3-4f94-83b6-77bc9e289c2d', 'Demo',
'da6cf455-971c-46a4-9ed7-fe0e9b5f5548', '2023-03-07 16:57:27.762190 UTC', 'some_data')
```

## Install / Compile from source

Install Rust by following the instructions in [the getting started page](https://www.rust-lang.org/learn/get-started)

Clone and compile/install.

```bash
## Clone the repository
git clone https://github.com/mpwsh/pgtransfer
## cd into the folder and install with cargo
cd pgtransfer
cargo install --path .
## or just build and run from ./target/debug
cargo build
./target/debug/pgtransfer
```

Use at your own risk. Error handling is practically non-existent, but postgres will complain if somethings wrong.

## License

See [LICENSE](LICENSE)
