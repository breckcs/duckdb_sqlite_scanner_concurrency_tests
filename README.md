This is a Rust project to exercise writing to a SQLite database using a SQLite client while concurrently reading from the same database using either a SQLite client or a DuckDB client in a variet of configurations (e.g., with and without a write-ahead log (WAL)).

This project uses [Rusqlite](https://github.com/rusqlite/rusqlite) for the SQLite client. It uses [duckdb-rs](https://github.com/duckdb/duckdb-rs) for the DuckDB client, which relies on the [sqlite_scanner](https://github.com/duckdb/sqlite_scanner) DuckDB extension.

You can control how long each test runs by editing the row count function:
```rust
    fn get_row_count() -> i64 {
        10000
    }
```

You can control the amount of concurrency by editing how long each thread sleeps between iterations:
```rust
    fn get_sleep_duration() -> Duration {
        Duration::from_millis(1)
    }
```

Run each test individually. For example:
```shell
cargo test test_sqlite_writer_sqlite_reader_with_wal -- --nocapture
```

To rerun a test, delete the corresponding files in the `db` directory before running the test:
```shell
rm db/test_sqlite_writer_sqlite_reader_with_wal*
cargo test test_sqlite_writer_sqlite_reader_with_wal -- --nocapture
```

To run all the tests:
```shell
make
```