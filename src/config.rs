use anyhow::Result;
use dotenvy::dotenv;
use serde::Deserialize;
use std::env;

#[derive(Debug, Deserialize, Clone)]
pub struct AppConfig {
    pub kafka_bootstrap_servers: String,
    pub kafka_topic: String,
    pub kafka_group_id: String,
    pub kafka_auto_offset_reset: String,
    pub kafka_sasl_mechanism: String,
    pub kafka_username: String,
    pub kafka_password: String,
    pub kafka_security_protocol: String,
    pub kafka_max_retries: u32,
    pub kafka_circuit_breaker_cooldown: u64,
    pub database_url: String,
    pub log_level: String,
}

impl AppConfig {
    pub fn load() -> Result<Self> {
        dotenv().ok();

        let kafka_bootstrap_servers =
            env::var("KAFKA_BOOTSTRAP_SERVERS").unwrap_or_else(|_| "localhost:9092".to_string());
        let kafka_topic = env::var("KAFKA_TOPIC").unwrap_or_else(|_| "siscom-minimal".to_string());
        let kafka_group_id =
            env::var("KAFKA_GROUP_ID").unwrap_or_else(|_| "siscom-api-consumer".to_string());
        let kafka_auto_offset_reset =
            env::var("KAFKA_AUTO_OFFSET_RESET").unwrap_or_else(|_| "latest".to_string());
        let kafka_sasl_mechanism =
            env::var("KAFKA_SASL_MECHANISM").unwrap_or_else(|_| "SCRAM-SHA-256".to_string());
        let kafka_username = env::var("KAFKA_USERNAME").unwrap_or_default();
        let kafka_password = env::var("KAFKA_PASSWORD").unwrap_or_default();
        let kafka_security_protocol =
            env::var("KAFKA_SECURITY_PROTOCOL").unwrap_or_else(|_| "SASL_PLAINTEXT".to_string());
        let kafka_max_retries = env::var("KAFKA_MAX_RETRIES")
            .unwrap_or_else(|_| "5".to_string())
            .parse()
            .unwrap_or(5);
        let kafka_circuit_breaker_cooldown = env::var("KAFKA_CIRCUIT_BREAKER_COOLDOWN")
            .unwrap_or_else(|_| "300".to_string())
            .parse()
            .unwrap_or(300);

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
            kafka_bootstrap_servers,
            kafka_topic,
            kafka_group_id,
            kafka_auto_offset_reset,
            kafka_sasl_mechanism,
            kafka_username,
            kafka_password,
            kafka_security_protocol,
            kafka_max_retries,
            kafka_circuit_breaker_cooldown,
            database_url,
            log_level,
        })
    }
}
