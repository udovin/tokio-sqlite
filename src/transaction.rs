use rusqlite::params_from_iter;
use tokio::sync::{mpsc, oneshot};

use crate::Error;

use super::query::{ExecuteCommand, QueryClient, QueryCommand, QueryTask};
use super::{Status, Value};

enum TransactionCommand {
    Commit {
        tx: oneshot::Sender<Result<(), Error>>,
    },
    Rollback {
        tx: oneshot::Sender<Result<(), Error>>,
    },
    Execute(ExecuteCommand),
    Query(QueryCommand),
}

pub(super) struct TransactionClient(mpsc::Sender<TransactionCommand>);

impl TransactionClient {
    pub async fn commit(&mut self) -> Result<(), Error> {
        let (tx, rx) = oneshot::channel();
        self.0
            .send(TransactionCommand::Commit { tx })
            .await
            .map_err(|_| Error::InvalidQuery)?;
        rx.await.unwrap()
    }

    pub async fn rollback(&mut self) -> Result<(), Error> {
        let (tx, rx) = oneshot::channel();
        self.0
            .send(TransactionCommand::Rollback { tx })
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
            .send(TransactionCommand::Execute(ExecuteCommand {
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
            .send(TransactionCommand::Query(QueryCommand {
                statement,
                arguments,
                tx,
            }))
            .await
            .map_err(|_| Error::InvalidQuery)?;
        rx.await.unwrap()
    }
}

impl Drop for TransactionClient {
    fn drop(&mut self) {}
}

pub(super) struct TransactionTask<'a> {
    conn: &'a mut rusqlite::Connection,
}

impl<'a> TransactionTask<'a> {
    pub fn new(conn: &'a mut rusqlite::Connection) -> Self {
        Self { conn }
    }

    pub fn blocking_run(self, handle_rx: oneshot::Sender<Result<TransactionClient, Error>>) {
        let transaction = match self
            .conn
            .transaction_with_behavior(rusqlite::TransactionBehavior::Deferred)
        {
            Ok(conn) => conn,
            Err(err) => {
                let _ = handle_rx.send(Err(err));
                return;
            }
        };
        let (tx, mut rx) = mpsc::channel(1);
        if let Err(_) = handle_rx.send(Ok(TransactionClient(tx))) {
            // Drop transaction if nobody listens result.
            return;
        }
        while let Some(cmd) = rx.blocking_recv() {
            match cmd {
                TransactionCommand::Commit { tx } => {
                    let _ = tx.send(transaction.commit());
                    return;
                }
                TransactionCommand::Rollback { tx } => {
                    let _ = tx.send(transaction.rollback());
                    return;
                }
                TransactionCommand::Execute(cmd) => {
                    let _ = cmd.tx.send(
                        transaction
                            .execute(&cmd.statement, params_from_iter(cmd.arguments.into_iter()))
                            .map(|rows_affected| Status {
                                rows_affected,
                                last_insert_id: Some(transaction.last_insert_rowid()),
                            }),
                    );
                }
                TransactionCommand::Query(cmd) => {
                    let stmt = match transaction.prepare(&cmd.statement) {
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
