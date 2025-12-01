use crate::config::AppConfig;
use crate::processor::message_processor;
use crate::db::DbPool;
use rumqttc::{AsyncClient, MqttOptions, QoS, Event, Packet};
use std::time::Duration;
use tracing::{info, error};
use std::sync::Arc;
use uuid::Uuid;

pub async fn start_mqtt_client(config: &AppConfig, pool: DbPool) -> anyhow::Result<()> {
    let client_id = format!("siscom-trips-{}", Uuid::new_v4());
    let mut mqttoptions = MqttOptions::new(client_id, &config.mqtt_broker, config.mqtt_port);
    mqttoptions.set_keep_alive(Duration::from_secs(5));
    mqttoptions.set_credentials(&config.mqtt_username, &config.mqtt_password);

    let (client, mut eventloop) = AsyncClient::new(mqttoptions, 100); // Capacidad del canal
    
    client.subscribe(&config.mqtt_topic, QoS::AtLeastOnce).await?;
    info!("Subscribed to {}", config.mqtt_topic);

    let pool = Arc::new(pool);

    loop {
        match eventloop.poll().await {
            Ok(notification) => {
                match notification {
                    Event::Incoming(Packet::Publish(publish)) => {
                        let pool_clone = pool.clone();
                        tokio::spawn(async move {
                            if let Err(e) = message_processor::process_message(&pool_clone, &publish.payload).await {
                                error!("Error processing message: {}", e);
                            }
                        });
                    }
                    Event::Incoming(Packet::ConnAck(_)) => {
                        info!("MQTT Connected!");
                    }
                    Event::Incoming(Packet::SubAck(_)) => {
                        info!("Subscription confirmed!");
                    }
                    _ => {}
                }
            }
            Err(e) => {
                error!("MQTT Connection error: {}", e);
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
}
