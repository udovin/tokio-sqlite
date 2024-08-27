use std::marker::PhantomData;
use std::path::Path;

use tokio::sync::oneshot;

use super::connection::{ConnectionClient, ConnectionTask};
use super::query::QueryClient;
use super::transaction::TransactionClient;

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

/// A result of executing the statement without the resulting query rows.
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

/// An asynchronous stream of resulting query rows.
pub struct Rows<'a> {
    tx: QueryClient,
    _phantom: PhantomData<&'a ()>,
}

impl<'a> Rows<'a> {
    pub fn columns(&self) -> &[String] {
        self.tx.columns()
    }

    pub async fn next(&mut self) -> Option<Result<Row, Error>> {
        self.tx.next().await
    }
}

impl<'a> Drop for Rows<'a> {
    fn drop(&mut self) {}
}

/// An asynchronous SQLite database transaction.
pub struct Transaction<'a> {
    tx: TransactionClient,
    _phantom: PhantomData<&'a ()>,
}

impl<'a> Transaction<'a> {
    /// Consumes the transaction, committing all changes made within it.
    pub async fn commit(mut self) -> Result<(), Error> {
        self.tx.commit().await
    }

    /// Rolls the transaction back, discarding all changes made within it.
    pub async fn rollback(mut self) -> Result<(), Error> {
        self.tx.rollback().await
    }

    /// Executes a statement that does not return the resulting rows.
    ///
    /// Returns an error if the query returns resulting rows.
    pub async fn execute<S, A>(&mut self, statement: S, arguments: A) -> Result<Status, Error>
    where
        S: Into<String>,
        A: Into<Vec<Value>>,
    {
        self.tx.execute(statement.into(), arguments.into()).await
    }

    /// Executes a statement that returns the resulting query rows.
    pub async fn query<S, A>(&mut self, statement: S, arguments: A) -> Result<Rows, Error>
    where
        S: Into<String>,
        A: Into<Vec<Value>>,
    {
        let tx = self.tx.query(statement.into(), arguments.into()).await?;
        Ok(Rows {
            tx,
            _phantom: PhantomData,
        })
    }

    /// Executes a statement that returns zero or one resulting query row.
    ///
    /// Returns an error if the query returns more than one row.
    pub async fn query_row<S, A>(
        &mut self,
        statement: S,
        arguments: A,
    ) -> Result<Option<Row>, Error>
    where
        S: Into<String>,
        A: Into<Vec<Value>>,
    {
        let mut rows = self.query(statement, arguments).await?;
        let row = match rows.next().await {
            Some(v) => v?,
            None => return Ok(None),
        };
        if let Some(_) = rows.next().await {
            return Err(Error::QueryReturnedNoRows);
        }
        Ok(Some(row))
    }
}

impl<'a> Drop for Transaction<'a> {
    fn drop(&mut self) {}
}

/// An asynchronous SQLite client.
pub struct Connection {
    tx: Option<ConnectionClient>,
    handle: Option<tokio::task::JoinHandle<()>>,
}

impl Connection {
    /// Opens a new connection to a SQLite database.
    pub async fn open<P: AsRef<Path>>(path: P) -> Result<Self, Error> {
        let task = ConnectionTask::new(path.as_ref().to_owned());
        let (tx, rx) = oneshot::channel();
        let handle = tokio::task::spawn_blocking(|| task.blocking_run(tx));
        Ok(Connection {
            tx: Some(rx.await.unwrap()?),
            handle: Some(handle),
        })
    }

    /// Begins new transaction.
    pub async fn transaction(&mut self) -> Result<Transaction, Error> {
        let tx = self.tx.as_mut().unwrap().transaction().await?;
        Ok(Transaction {
            tx,
            _phantom: PhantomData,
        })
    }

    /// Executes a statement that does not return the resulting rows.
    ///
    /// Returns an error if the query returns resulting rows.
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

    /// Executes a statement that returns the resulting query rows.
    pub async fn query<S, A>(&mut self, statement: S, arguments: A) -> Result<Rows, Error>
    where
        S: Into<String>,
        A: Into<Vec<Value>>,
    {
        let tx = self
            .tx
            .as_mut()
            .unwrap()
            .query(statement.into(), arguments.into())
            .await?;
        Ok(Rows {
            tx,
            _phantom: PhantomData,
        })
    }

    /// Executes a statement that returns zero or one resulting query row.
    ///
    /// Returns an error if the query returns more than one row.
    pub async fn query_row<S, A>(
        &mut self,
        statement: S,
        arguments: A,
    ) -> Result<Option<Row>, Error>
    where
        S: Into<String>,
        A: Into<Vec<Value>>,
    {
        let mut rows = self.query(statement, arguments).await?;
        let row = match rows.next().await {
            Some(v) => v?,
            None => return Ok(None),
        };
        if let Some(_) = rows.next().await {
            return Err(Error::QueryReturnedNoRows);
        }
        Ok(Some(row))
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        drop(self.tx.take());
        if let Some(handle) = self.handle.take() {
            tokio::task::block_in_place(|| tokio::runtime::Handle::current().block_on(handle))
                .unwrap();
        };
    }
}
