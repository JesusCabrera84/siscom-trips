use crate::config::AppConfig;
use crate::db::DbPool;
use crate::processor::message_processor;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message;
use std::sync::Arc;
use std::time::Duration;
use tracing::{error, info, warn};

/// Starts the Kafka consumer with SASL/SCRAM authentication and a circuit breaker mechanism.
pub async fn start_kafka_consumer(config: &AppConfig, pool: DbPool) -> anyhow::Result<()> {
    info!("Initializing Kafka consumer for topic: {}", config.kafka_topic);

    let mut client_config = ClientConfig::new();
    client_config
        .set("bootstrap.servers", &config.kafka_bootstrap_servers)
        .set("group.id", &config.kafka_group_id)
        .set("auto.offset.reset", &config.kafka_auto_offset_reset)
        // SASL Configuration
        .set("security.protocol", &config.kafka_security_protocol)
        .set("sasl.mechanism", &config.kafka_sasl_mechanism)
        .set("sasl.username", &config.kafka_username)
        .set("sasl.password", &config.kafka_password);

    // Create the consumer
    let consumer: StreamConsumer = client_config.create()?;

    consumer.subscribe(&[&config.kafka_topic])?;
    info!("Subscribed to topic: {}", config.kafka_topic);

    let pool = Arc::new(pool);
    let mut consecutive_failures = 0;
    let max_retries = config.kafka_max_retries;
    let cooldown_duration = Duration::from_secs(config.kafka_circuit_breaker_cooldown);

    loop {
        // Circuit Breaker Check
        if consecutive_failures >= max_retries {
            warn!(
                "Circuit breaker tripped ({} consecutive failures)! Sleeping for {} seconds...",
                consecutive_failures,
                config.kafka_circuit_breaker_cooldown
            );
            tokio::time::sleep(cooldown_duration).await;
            consecutive_failures = 0;
            info!("Circuit breaker reset. Resuming consumption.");
        }

        match consumer.recv().await {
            Ok(m) => {
                // Success: Reset failure counter
                consecutive_failures = 0;

                let payload = match m.payload() {
                    None => {
                        warn!("Received empty payload from Kafka");
                        continue;
                    }
                    Some(p) => p,
                };

                let pool_clone = pool.clone();
                let payload_vec = payload.to_vec();
                
                // Process the message in a background task to not block the consumer loop
                tokio::spawn(async move {
                    if let Err(e) = message_processor::process_message(&pool_clone, &payload_vec).await {
                        error!("Error processing message: {}", e);
                    }
                });
            }
            Err(e) => {
                error!("Kafka error: {}. Incrementing failure count ({} / {})", e, consecutive_failures + 1, max_retries);
                consecutive_failures += 1;
                
                // Small delay to prevent tight loop in case of minor network glitches
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }
    }
}
