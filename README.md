# tokio-sqlite

[![crates.io](https://img.shields.io/crates/v/tokio-sqlite.svg)](https://crates.io/crates/tokio-sqlite)
[![codecov](https://codecov.io/gh/udovin/tokio-sqlite/graph/badge.svg?token=QCgfDbPUWH)](https://codecov.io/gh/udovin/tokio-sqlite)

## Usage

```rust
use tokio_sqlite::{Connection, Value};

struct Post {
    id: i32,
    title: String,
    author: Option<i64>,
}

#[tokio::main]
async fn main() {
    let mut conn = Connection::open(":memory:").await.unwrap();
    conn.execute(
        "CREATE TABLE post (
            id     INTEGER PRIMARY KEY,
            title  TEXT NOT NULL,
            author BIGINT
        )",
        [],
    )
    .await
    .unwrap();
    let post = Post {
        id: 1,
        title: "tokio-sqlite".into(),
        author: None,
    };
    let mut rows = conn
        .query(
            "INSERT INTO post (title, author) VALUES ($1, $2) RETURNING id",
            [post.title.into(), post.author.into()],
        )
        .await
        .unwrap();
    let row = rows.next().await.unwrap().unwrap();
    assert_eq!(row.values()[0], Value::Integer(post.id.into()));
}
```

## License

tokio-sqlite is distributed under the terms of both the MIT license and the Apache 2.0 License.
