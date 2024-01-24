use std::marker::PhantomData;
use std::path::Path;

use tokio::sync::oneshot;

use super::connection::{ConnectionHandle, ConnectionTask};
use super::query::QueryHandle;
use super::transaction::TransactionHandle;

pub type Error = rusqlite::Error;

pub type Value = rusqlite::types::Value;

#[derive(Clone, Debug)]
pub struct Row {
    pub(super) values: Vec<Value>,
}

impl Row {
    pub fn values(&self) -> &[Value] {
        &self.values
    }

    pub fn into_values(self) -> Vec<Value> {
        self.values
    }
}

#[derive(Default, Clone)]
pub struct Status {
    pub(super) rows_affected: usize,
    pub(super) last_insert_id: Option<i64>,
}

impl Status {
    pub fn rows_affected(&self) -> usize {
        self.rows_affected
    }

    pub fn last_insert_id(&self) -> Option<i64> {
        self.last_insert_id
    }
}

pub struct Rows<'a> {
    handle: QueryHandle,
    _phantom: PhantomData<&'a ()>,
}

impl<'a> Rows<'a> {
    pub fn columns(&self) -> &[String] {
        self.handle.columns()
    }

    pub async fn next(&mut self) -> Option<Result<Row, Error>> {
        self.handle.next().await
    }
}

impl<'a> Drop for Rows<'a> {
    fn drop(&mut self) {}
}

pub struct Transaction<'a> {
    tx: TransactionHandle,
    _phantom: PhantomData<&'a ()>,
}

impl<'a> Transaction<'a> {
    pub async fn commit(mut self) -> Result<(), Error> {
        self.tx.commit().await
    }

    pub async fn rollback(mut self) -> Result<(), Error> {
        self.tx.rollback().await
    }

    pub async fn execute(&mut self, statement: &str, arguments: &[Value]) -> Result<Status, Error> {
        self.tx
            .execute(statement.to_owned(), arguments.to_owned())
            .await
    }

    pub async fn query<S, A>(&mut self, statement: S, arguments: A) -> Result<Rows, Error>
    where
        S: Into<String>,
        A: Into<Vec<Value>>,
    {
        let handle = self.tx.query(statement.into(), arguments.into()).await?;
        Ok(Rows {
            handle,
            _phantom: PhantomData,
        })
    }
}

impl<'a> Drop for Transaction<'a> {
    fn drop(&mut self) {}
}

pub struct Connection {
    tx: Option<ConnectionHandle>,
    handle: Option<tokio::task::JoinHandle<()>>,
}

impl Connection {
    pub async fn open<P: AsRef<Path>>(path: P) -> Result<Self, Error> {
        let task = ConnectionTask::new(path.as_ref().to_owned());
        let (tx, rx) = oneshot::channel();
        let handle = tokio::task::spawn_blocking(|| task.blocking_run(tx));
        Ok(Connection {
            tx: Some(rx.await.unwrap()?),
            handle: Some(handle),
        })
    }

    pub async fn transaction(&mut self) -> Result<Transaction, Error> {
        let tx = self.tx.as_mut().unwrap().transaction().await?;
        Ok(Transaction {
            tx,
            _phantom: PhantomData,
        })
    }

    pub async fn execute<S, A>(&mut self, statement: S, arguments: A) -> Result<Status, Error>
    where
        S: Into<String>,
        A: Into<Vec<Value>>,
    {
        self.tx
            .as_mut()
            .unwrap()
            .execute(statement.into(), arguments.into())
            .await
    }

    pub async fn query<S, A>(&mut self, statement: S, arguments: A) -> Result<Rows, Error>
    where
        S: Into<String>,
        A: Into<Vec<Value>>,
    {
        let handle = self
            .tx
            .as_mut()
            .unwrap()
            .query(statement.into(), arguments.into())
            .await?;
        Ok(Rows {
            handle,
            _phantom: PhantomData,
        })
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        drop(self.tx.take());
        if let Some(handle) = self.handle.take() {
            let _ =
                tokio::task::block_in_place(|| tokio::runtime::Handle::current().block_on(handle));
        };
    }
}
