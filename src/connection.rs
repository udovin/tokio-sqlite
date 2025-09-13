use std::path::PathBuf;

use rusqlite::params_from_iter;
use tokio::sync::{mpsc, oneshot};

use crate::Error;

use super::query::{ExecuteCommand, QueryClient, QueryCommand, QueryTask};
use super::transaction::{TransactionClient, TransactionTask};
use super::{Status, Value};

enum ConnectionCommand {
    Transaction {
        tx: oneshot::Sender<Result<TransactionClient, Error>>,
    },
    Execute(ExecuteCommand),
    Query(QueryCommand),
}

pub(super) struct ConnectionClient(mpsc::Sender<ConnectionCommand>);

impl ConnectionClient {
    pub async fn transaction(&mut self) -> Result<TransactionClient, Error> {
        let (tx, rx) = oneshot::channel();
        self.0
            .send(ConnectionCommand::Transaction { tx })
            .await
            .map_err(|_| Error::InvalidQuery)?;
        rx.await.unwrap()
    }

    pub async fn execute(
        &mut self,
        statement: String,
        arguments: Vec<Value>,
    ) -> Result<Status, Error> {
        let (tx, rx) = oneshot::channel();
        self.0
            .send(ConnectionCommand::Execute(ExecuteCommand {
                statement,
                arguments,
                tx,
            }))
            .await
            .map_err(|_| Error::InvalidQuery)?;
        rx.await.unwrap()
    }

    pub async fn query(
        &mut self,
        statement: String,
        arguments: Vec<Value>,
    ) -> Result<QueryClient, Error> {
        let (tx, rx) = oneshot::channel();
        self.0
            .send(ConnectionCommand::Query(QueryCommand {
                statement,
                arguments,
                tx,
            }))
            .await
            .map_err(|_| Error::InvalidQuery)?;
        rx.await.unwrap()
    }
}

impl Drop for ConnectionClient {
    fn drop(&mut self) {}
}

pub(super) struct ConnectionTask {
    path: PathBuf,
}

impl ConnectionTask {
    pub fn new(path: PathBuf) -> Self {
        Self { path }
    }

    pub fn blocking_run(self, handle_rx: oneshot::Sender<Result<ConnectionClient, Error>>) {
        let mut conn = match rusqlite::Connection::open(self.path) {
            Ok(v) => v,
            Err(err) => {
                let _ = handle_rx.send(Err(err));
                return;
            }
        };
        let (tx, mut rx) = mpsc::channel(1);
        if let Err(_) = handle_rx.send(Ok(ConnectionClient(tx))) {
            // Drop connection if nobody listens result.
            return;
        }
        while let Some(cmd) = rx.blocking_recv() {
            match cmd {
                ConnectionCommand::Transaction { tx, .. } => {
                    let task = TransactionTask::new(&mut conn);
                    task.blocking_run(tx);
                }
                ConnectionCommand::Execute(cmd) => {
                    let _ = cmd.tx.send(
                        conn.execute(&cmd.statement, params_from_iter(cmd.arguments.into_iter()))
                            .map(|rows_affected| Status {
                                rows_affected,
                                last_insert_id: Some(conn.last_insert_rowid()),
                            }),
                    );
                }
                ConnectionCommand::Query(cmd) => {
                    let stmt = match conn.prepare(&cmd.statement) {
                        Ok(stmt) => stmt,
                        Err(err) => {
                            let _ = cmd.tx.send(Err(err));
                            continue;
                        }
                    };
                    let task = QueryTask::new(stmt, cmd.arguments);
                    task.blocking_run(cmd.tx);
                }
            }
        }
    }
}
