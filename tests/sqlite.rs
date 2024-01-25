use tokio_sqlite::{Connection, Value};

#[tokio::test(flavor = "multi_thread")]
async fn test_sqlite() {
    let mut conn = Connection::open(":memory:").await.unwrap();
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
    drop(rows);
    drop(tx);
    // Check empty result.
    let mut rows = conn
        .query("SELECT * FROM test_tbl WHERE a = -1", [])
        .await
        .unwrap();
    assert!(rows.next().await.is_none());
    drop(rows);
    // Check query_row.
    let row = conn
        .query_row("SELECT * FROM test_tbl WHERE a = 1", [])
        .await
        .unwrap();
    assert!(row.is_some());
}
