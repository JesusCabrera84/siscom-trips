mod config;
mod db;
mod models;
mod mqtt;
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

    info!("Starting Siscom Trips Service...");

    // Init DB
    let pool = db::init_pool(&config.database_url).await?;
    info!("Connected to database");

    // Start MQTT
    mqtt::start_mqtt_client(&config, pool).await?;

    Ok(())
}
