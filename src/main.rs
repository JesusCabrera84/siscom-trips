mod config;
mod db;
mod kafka;
mod models;
mod processor;

use config::AppConfig;
use tracing::info;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Load config
    let config = AppConfig::load()?;

    // Init logging
    tracing_subscriber::fmt()
        .with_env_filter(&config.log_level)
        .init();

    info!("Starting Siscom Trips Service (Kafka Edition)...");

    // Init DB
    let pool = db::init_pool(&config.database_url).await?;
    info!("Connected to database");

    // Start Kafka
    kafka::start_kafka_consumer(&config, pool).await?;

    Ok(())
}
