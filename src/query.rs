use rusqlite::{params_from_iter, Error};
use tokio::sync::{mpsc, oneshot};

use super::{Row, Status, Value};

pub(super) struct ExecuteCommand {
    pub statement: String,
    pub arguments: Vec<Value>,
    pub tx: oneshot::Sender<Result<Status, Error>>,
}

pub(super) struct QueryCommand {
    pub statement: String,
    pub arguments: Vec<Value>,
    pub tx: oneshot::Sender<Result<QueryHandle, Error>>,
}

pub(super) struct QueryHandle {
    columns: Vec<String>,
    rx: mpsc::Receiver<Result<Row, Error>>,
}

impl QueryHandle {
    pub fn columns(&self) -> &[String] {
        &self.columns
    }

    pub async fn next(&mut self) -> Option<Result<Row, Error>> {
        self.rx.recv().await
    }
}

pub(super) struct QueryTask<'a> {
    stmt: rusqlite::Statement<'a>,
    arguments: Vec<Value>,
}

impl<'a> QueryTask<'a> {
    pub fn new(stmt: rusqlite::Statement<'a>, arguments: Vec<Value>) -> Self {
        Self { stmt, arguments }
    }

    pub fn blocking_run(mut self, handle_rx: oneshot::Sender<Result<QueryHandle, Error>>) {
        let columns: Vec<_> = self
            .stmt
            .column_names()
            .iter_mut()
            .map(|v| v.to_owned())
            .collect();
        let columns_len = columns.len();
        let mut rows = match self
            .stmt
            .query(params_from_iter(self.arguments.into_iter()))
        {
            Ok(rows) => rows,
            Err(err) => {
                let _ = handle_rx.send(Err(err));
                return;
            }
        };
        let (tx, rx) = mpsc::channel(1);
        if let Err(_) = handle_rx.send(Ok(QueryHandle { columns, rx })) {
            // Drop query if nobody listens result.
            return;
        }
        loop {
            let row = match rows.next() {
                Ok(Some(row)) => row,
                Ok(None) => return,
                Err(err) => {
                    _ = tx.blocking_send(Err(err));
                    return;
                }
            };
            let mut values = Vec::with_capacity(columns_len);
            for i in 0..columns_len {
                let value = match row.get(i) {
                    Ok(value) => value,
                    Err(err) => {
                        _ = tx.blocking_send(Err(err));
                        return;
                    }
                };
                values.push(value);
            }
            if let Err(_) = tx.blocking_send(Ok(Row { values })) {
                return;
            }
        }
    }
}
