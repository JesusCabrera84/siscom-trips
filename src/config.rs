use anyhow::Result;
use dotenvy::dotenv;
use serde::Deserialize;
use std::env;

#[derive(Debug, Deserialize, Clone)]
pub struct AppConfig {
    pub mqtt_broker: String,
    pub mqtt_port: u16,
    pub mqtt_username: String,
    pub mqtt_password: String,
    pub mqtt_topic: String,
    pub database_url: String,
    pub log_level: String,
}

impl AppConfig {
    pub fn load() -> Result<Self> {
        dotenv().ok();

        let mqtt_broker = env::var("MQTT_BROKER").unwrap_or_else(|_| "localhost".to_string());
        let mqtt_port = env::var("MQTT_PORT")
            .unwrap_or_else(|_| "1883".to_string())
            .parse()
            .unwrap_or(1883);
        let mqtt_username = env::var("MQTT_USERNAME").unwrap_or_default();
        let mqtt_password = env::var("MQTT_PASSWORD").unwrap_or_default();
        let mqtt_topic = env::var("MQTT_TOPIC").unwrap_or_else(|_| "siscom/#".to_string());

        let db_host = env::var("DB_HOST").unwrap_or_else(|_| "localhost".to_string());
        let db_port = env::var("DB_PORT").unwrap_or_else(|_| "5432".to_string());
        let db_name = env::var("DB_DATABASE").unwrap_or_else(|_| "siscom_admin".to_string());
        let db_user = env::var("DB_USER").unwrap_or_else(|_| "siscom".to_string());
        let db_pwd = env::var("DB_PWD").unwrap_or_else(|_| "siscom".to_string());

        let database_url = format!(
            "postgres://{}:{}@{}:{}/{}",
            db_user, db_pwd, db_host, db_port, db_name
        );

        let log_level = env::var("LOG_LEVEL").unwrap_or_else(|_| "info".to_string());

        Ok(Self {
            mqtt_broker,
            mqtt_port,
            mqtt_username,
            mqtt_password,
            mqtt_topic,
            database_url,
            log_level,
        })
    }
}
