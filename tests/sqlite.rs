use rand::distributions::{Alphanumeric, DistString};
use std::path::{Path, PathBuf};

use tokio_sqlite::{Connection, Value};

pub struct TempDir(PathBuf);

impl TempDir {
    #[allow(unused)]
    pub fn join<P: AsRef<Path>>(&self, path: P) -> PathBuf {
        self.0.join(path)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}

#[allow(unused)]
pub fn temp_dir() -> Result<TempDir, std::io::Error> {
    let tmpdir = Path::new(env!("CARGO_TARGET_TMPDIR"));
    let path = loop {
        let path = tmpdir.join(Alphanumeric.sample_string(&mut rand::thread_rng(), 32));
        match std::fs::metadata(&path) {
            Ok(_) => continue,
            Err(v) if v.kind() == std::io::ErrorKind::NotFound => break path,
            Err(v) => return Err(v.into()),
        }
    };
    std::fs::create_dir_all(&path)?;
    Ok(TempDir(path))
}

#[tokio::test(flavor = "multi_thread")]
async fn test_sqlite() {
    let tmpdir = temp_dir().unwrap();
    let mut conn = Connection::open(
        tmpdir
            .join("db.sqlite")
            .into_os_string()
            .into_string()
            .unwrap(),
    )
    .await
    .unwrap();
    conn.execute(
        r#"CREATE TABLE test_tbl (a INTEGER PRIMARY KEY, b TEXT NOT NULL)"#,
        &[],
    )
    .await
    .unwrap();
    conn.execute(
        r#"INSERT INTO test_tbl (b) VALUES ($1), ($2)"#,
        &["test1".to_owned().into(), "test2".to_owned().into()],
    )
    .await
    .unwrap();
    let mut rows = conn
        .query("SELECT a, b FROM test_tbl ORDER BY a", &[])
        .await
        .unwrap();
    assert_eq!(rows.columns(), vec!["a", "b"]);
    assert_eq!(
        rows.next().await.unwrap().unwrap().values(),
        vec![Value::Integer(1), Value::Text("test1".to_owned())]
    );
    assert_eq!(
        rows.next().await.unwrap().unwrap().values(),
        vec![Value::Integer(2), Value::Text("test2".to_owned())]
    );
    assert!(rows.next().await.is_none());
    drop(rows);
    // Check commit.
    let mut tx = conn.transaction().await.unwrap();
    tx.execute(r#"INSERT INTO test_tbl (b) VALUES ("test3")"#, &[])
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let mut rows = conn
        .query("SELECT COUNT(*) FROM test_tbl", &[])
        .await
        .unwrap();
    assert_eq!(
        rows.next().await.unwrap().unwrap().values(),
        vec![Value::Integer(3)]
    );
    drop(rows);
    // Check rollback.
    let mut tx = conn.transaction().await.unwrap();
    tx.execute(r#"INSERT INTO test_tbl (b) VALUES ("test3")"#, &[])
        .await
        .unwrap();
    tx.rollback().await.unwrap();
    let mut rows = conn
        .query("SELECT COUNT(*) FROM test_tbl", &[])
        .await
        .unwrap();
    assert_eq!(
        rows.next().await.unwrap().unwrap().values(),
        vec![Value::Integer(3)]
    );
    drop(rows);
    // Check drop.
    let mut tx = conn.transaction().await.unwrap();
    tx.execute(r#"INSERT INTO test_tbl (b) VALUES ("test3")"#, &[])
        .await
        .unwrap();
    drop(tx);
    let mut rows = conn
        .query("SELECT COUNT(*) FROM test_tbl", &[])
        .await
        .unwrap();
    assert_eq!(
        rows.next().await.unwrap().unwrap().values(),
        vec![Value::Integer(3)]
    );
    drop(rows);
    // Check uncommited.
    let mut tx = conn.transaction().await.unwrap();
    tx.execute(
        r#"INSERT INTO test_tbl (b) VALUES ($1)"#,
        &["test3".to_owned().into()],
    )
    .await
    .unwrap();
    let mut rows = tx
        .query("SELECT COUNT(*) FROM test_tbl", &[])
        .await
        .unwrap();
    assert_eq!(
        rows.next().await.unwrap().unwrap().values(),
        vec![Value::Integer(4)]
    );
}
