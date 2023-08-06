use anyhow::Result;
use log::error;
use tokio_postgres::{Client, NoTls};

pub async fn connect(server: &str) -> Result<Client> {
    let (client, connection) = tokio_postgres::connect(server, NoTls).await?;
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            error!("connection error: {}", e);
        }
    });
    Ok(client)
}
