fn main() {
    println!("Hello World!");
}

#[cfg(test)]
mod tests {
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_sqlite_writer_sqlite_reader_no_wal() {
        let count = sqlite_writer_sqlite_reader_no_wal();
        assert_eq!(count, get_row_count());
    }

    #[test]
    fn test_sqlite_writer_sqlite_reader_with_wal() {
        let count = sqlite_writer_sqlite_reader_with_wal();
        assert_eq!(count, get_row_count());
    }

    #[test]
    fn test_sqlite_writer_duckdb_reader_no_wal() {
        let count = sqlite_writer_duckdb_reader_no_wal();
        assert_eq!(count, get_row_count());
    }

    #[test]
    fn test_sqlite_writer_duckdb_reader_with_wal() {
        let count = sqlite_writer_duckdb_reader_with_wal();
        assert_eq!(count, get_row_count());
    }

    #[test]
    fn test_duckdb_writer_duckdb_reader_no_wal() {
        let count = duckdb_writer_duckdb_reader_no_wal();
        assert_eq!(count, get_row_count());
    }

    #[test]
    fn test_duckdb_writer_duckdb_reader_with_wal() {
        let count = duckdb_writer_duckdb_reader_with_wal();
        assert_eq!(count, get_row_count());
    }

    fn get_row_count() -> i64 {
        10000
    }
    fn get_sleep_duration() -> Duration {
        Duration::from_millis(1)
    }

    fn sqlite_writer_sqlite_reader_no_wal() -> i64 {
        let sleep_duration = get_sleep_duration();

        // Spawn a thread to write to the DB
        let writer = thread::spawn(move || {
            // Open the DB
            let conn =
                rusqlite::Connection::open("./db/sqlite_writer_sqlite_reader_no_wal.db").unwrap();

            // Create table
            conn.execute_batch("CREATE TABLE IF NOT EXISTS users (name VARCHAR, age INTEGER);")
                .unwrap();

            // Loop inserting rows
            for i in 0..get_row_count() {
                conn.execute("INSERT INTO users VALUES ('Alice', (?));", [i])
                    .unwrap();
                thread::sleep(sleep_duration);
            }
        });

        // Give the writer time to get started
        thread::sleep(Duration::from_secs(2));

        // Spawn a thread to read from the DB
        let reader = thread::spawn(move || {
            // Open the database
            let conn =
                rusqlite::Connection::open("./db/sqlite_writer_sqlite_reader_no_wal.db").unwrap();

            // Count the rows
            let mut stmt = conn.prepare("SELECT COUNT(*) FROM users").unwrap();

            // Loop querying the number of rows
            loop {
                let count: i64 = stmt.query_row([], |row| Ok(row.get(0).unwrap())).unwrap();
                println!("SQLite COUNT : {}", count);
                if count >= get_row_count() {
                    return count;
                };
                thread::sleep(sleep_duration);
            }
        });

        writer.join().unwrap();
        reader.join().unwrap()
    }

    fn sqlite_writer_sqlite_reader_with_wal() -> i64 {
        let sleep_duration = get_sleep_duration();

        // Spawn a thread to write to the DB
        let writer = thread::spawn(move || {
            // Open the DB
            let conn =
                rusqlite::Connection::open("./db/sqlite_writer_sqlite_reader_with_wal.db").unwrap();

            // Enable WAL mode
            conn.pragma_update(None, "journal_mode", "WAL").unwrap();

            // Create table
            conn.execute_batch("CREATE TABLE IF NOT EXISTS users (name VARCHAR, age INTEGER);")
                .unwrap();

            // Loop inserting rows
            for i in 0..get_row_count() {
                conn.execute("INSERT INTO users VALUES ('Alice', (?));", [i])
                    .unwrap();
                thread::sleep(sleep_duration);
            }
        });

        // Give the writer time to get started
        thread::sleep(Duration::from_secs(2));

        // Spawn a thread to read from the DB
        let reader = thread::spawn(move || {
            // Open the database
            let conn =
                rusqlite::Connection::open("./db/sqlite_writer_sqlite_reader_with_wal.db").unwrap();

            // Count the rows
            let mut stmt = conn.prepare("SELECT COUNT(*) FROM users").unwrap();

            // Loop querying the number of rows
            loop {
                let count: i64 = stmt.query_row([], |row| Ok(row.get(0).unwrap())).unwrap();
                println!("SQLite COUNT : {}", count);
                if count >= get_row_count() {
                    return count;
                }
                thread::sleep(sleep_duration);
            }
        });

        writer.join().unwrap();
        reader.join().unwrap()
    }

    fn sqlite_writer_duckdb_reader_no_wal() -> i64 {
        let sleep_duration = get_sleep_duration();

        // Spawn a thread to write to the DB
        let writer = thread::spawn(move || {
            // Open the DB
            let conn =
                rusqlite::Connection::open("./db/sqlite_writer_duckdb_reader_no_wal.db").unwrap();

            // Create table
            conn.execute_batch("CREATE TABLE IF NOT EXISTS users (name VARCHAR, age INTEGER);")
                .unwrap();

            // Loop inserting rows
            for i in 0..get_row_count() {
                conn.execute("INSERT INTO users VALUES ('Alice', (?));", [i])
                    .unwrap();
                thread::sleep(sleep_duration);
            }
        });

        // Give the writer time to get started
        thread::sleep(Duration::from_secs(2));

        // Spawn a thread to read from the DB
        let reader = thread::spawn(move || {
            // Open the DB in memory
            let conn = duckdb::Connection::open_in_memory().unwrap();

            // Attach to the SQLite DB
            conn.execute_batch("
            INSTALL sqlite;
            ATTACH './db/sqlite_writer_duckdb_reader_no_wal.db' AS sqlite_db (TYPE SQLITE, READ_ONLY);
            ").unwrap();

            // Count the rows
            let mut stmt = conn
                .prepare("SELECT COUNT(*) FROM sqlite_db.users")
                .unwrap();

            // Loop querying the number of rows
            loop {
                let count: i64 = stmt.query_row([], |row| Ok(row.get(0).unwrap())).unwrap();
                println!("DuckDB COUNT : {}", count);
                if count >= get_row_count() {
                    return count;
                }
                thread::sleep(sleep_duration);
            }
        });

        writer.join().unwrap();
        reader.join().unwrap()
    }

    fn sqlite_writer_duckdb_reader_with_wal() -> i64 {
        let sleep_duration = get_sleep_duration();

        // Spawn a thread to write to the DB
        let writer = thread::spawn(move || {
            // Open the DB
            let conn =
                rusqlite::Connection::open("./db/sqlite_writer_duckdb_reader_with_wal.db").unwrap();

            // Enable WAL mode
            conn.pragma_update(None, "journal_mode", "WAL").unwrap();

            // Create table
            conn.execute_batch("CREATE TABLE IF NOT EXISTS users (name VARCHAR, age INTEGER);")
                .unwrap();

            // Loop inserting rows
            for i in 0..get_row_count() {
                conn.execute("INSERT INTO users VALUES ('Alice', (?));", [i])
                    .unwrap();
                thread::sleep(sleep_duration);
            }
        });

        // Give the writer time to get started
        thread::sleep(Duration::from_secs(2));

        // Spawn a thread to read from the DB
        let reader = thread::spawn(move || {
            // Open the DB in memory
            let conn = duckdb::Connection::open_in_memory().unwrap();

            // Attach to the SQLite DB
            conn.execute_batch("
            INSTALL sqlite;
            ATTACH './db/sqlite_writer_duckdb_reader_with_wal.db' AS sqlite_db (TYPE SQLITE, READ_ONLY);
            ").unwrap();

            // Count the rows
            let mut stmt = conn
                .prepare("SELECT COUNT(*) FROM sqlite_db.users")
                .unwrap();

            // Loop querying the number of rows
            loop {
                let count: i64 = stmt.query_row([], |row| Ok(row.get(0).unwrap())).unwrap();
                println!("DuckDB COUNT : {}", count);
                if count >= get_row_count() {
                    return count;
                }
                thread::sleep(sleep_duration);
            }
        });

        writer.join().unwrap();
        reader.join().unwrap()
    }

    fn duckdb_writer_duckdb_reader_no_wal() -> i64 {
        let sleep_duration = get_sleep_duration();

        // Spawn a thread to write to the DB
        let writer = thread::spawn(move || {
            // Open the DB
            let conn = duckdb::Connection::open_in_memory().unwrap();

            // Attach to the SQLite DB
            conn.execute_batch(
                "
            INSTALL sqlite;
            ATTACH './db/duckdb_writer_duckdb_reader_no_wal.db' AS sqlite_db (TYPE SQLITE);
            ",
            )
            .unwrap();

            // Create table
            conn.execute_batch(
                "CREATE TABLE IF NOT EXISTS sqlite_db.users (name VARCHAR, age INTEGER);",
            )
            .unwrap();

            // Loop inserting rows
            for i in 0..get_row_count() {
                conn.execute("INSERT INTO sqlite_db.users VALUES ('Alice', (?));", [i])
                    .unwrap();
                thread::sleep(sleep_duration);
            }
        });

        // Give the writer time to get started
        thread::sleep(Duration::from_secs(2));

        // Spawn a thread to read from the DB
        let reader = thread::spawn(move || {
            // Open the DB in memory
            let conn = duckdb::Connection::open_in_memory().unwrap();

            // Attach to the SQLite DB
            conn.execute_batch("
            INSTALL sqlite;
            ATTACH './db/duckdb_writer_duckdb_reader_no_wal.db' AS sqlite_db (TYPE SQLITE, READ_ONLY);
            ").unwrap();

            // Count the rows
            let mut stmt = conn
                .prepare("SELECT COUNT(*) FROM sqlite_db.users")
                .unwrap();

            // Loop querying the number of rows
            loop {
                let count: i64 = stmt.query_row([], |row| Ok(row.get(0).unwrap())).unwrap();
                println!("DuckDB COUNT : {}", count);
                if count >= get_row_count() {
                    return count;
                }
                thread::sleep(sleep_duration);
            }
        });

        writer.join().unwrap();
        reader.join().unwrap()
    }

    fn duckdb_writer_duckdb_reader_with_wal() -> i64 {
        let sleep_duration = get_sleep_duration();

        // There doesn't seem to be a way to open a SQLite DB in WAL mode from DuckDB, so use the SQLite client
        {
            // Open the DB
            let conn =
                rusqlite::Connection::open("./db/duckdb_writer_duckdb_reader_with_wal.db").unwrap();

            // Enable WAL mode
            conn.pragma_update(None, "journal_mode", "WAL").unwrap();

            // Create table
            conn.execute_batch("CREATE TABLE IF NOT EXISTS users (name VARCHAR, age INTEGER);")
                .unwrap();

            // Close the SQLite client
            conn.close().unwrap();
        }

        // Spawn a thread to write to the DB
        let writer = thread::spawn(move || {
            // Open the DB
            let conn = duckdb::Connection::open_in_memory().unwrap();

            // Attach to the SQLite DB
            conn.execute_batch(
                "
            INSTALL sqlite;
            ATTACH './db/duckdb_writer_duckdb_reader_with_wal.db' AS sqlite_db (TYPE SQLITE);
            ",
            )
            .unwrap();

            // Create table
            conn.execute_batch(
                "CREATE TABLE IF NOT EXISTS sqlite_db.users (name VARCHAR, age INTEGER);",
            )
            .unwrap();

            // Loop inserting rows
            for i in 0..get_row_count() {
                conn.execute("INSERT INTO sqlite_db.users VALUES ('Alice', (?));", [i])
                    .unwrap();
                thread::sleep(sleep_duration);
            }
        });

        // Give the writer time to get started
        thread::sleep(Duration::from_secs(2));

        // Spawn a thread to read from the DB
        let reader = thread::spawn(move || {
            // Open the DB in memory
            let conn = duckdb::Connection::open_in_memory().unwrap();

            // Attach to the SQLite DB
            conn.execute_batch("
            INSTALL sqlite;
            ATTACH './db/duckdb_writer_duckdb_reader_with_wal.db' AS sqlite_db (TYPE SQLITE, READ_ONLY);
            ").unwrap();

            // Count the rows
            let mut stmt = conn
                .prepare("SELECT COUNT(*) FROM sqlite_db.users")
                .unwrap();

            // Loop querying the number of rows
            loop {
                let count: i64 = stmt.query_row([], |row| Ok(row.get(0).unwrap())).unwrap();
                println!("DuckDB COUNT : {}", count);
                if count >= get_row_count() {
                    return count;
                }
                thread::sleep(sleep_duration);
            }
        });

        writer.join().unwrap();
        reader.join().unwrap()
    }
}
