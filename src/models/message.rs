use serde::{Deserialize, Deserializer, Serialize};
use std::collections::HashMap;
use serde_json::Value;

#[derive(Debug, Deserialize)]
pub struct MqttMessage {
    pub data: Data,
    pub metadata: Metadata,
    pub uuid: String,
}

#[derive(Debug, Deserialize)]
pub struct Data {
    #[serde(rename = "ALERT")]
    pub alert: Option<String>,
    #[serde(rename = "MSG_CLASS")]
    pub msg_class: Option<String>,
    #[serde(rename = "GPS_DATETIME")]
    pub gps_datetime: Option<String>,
    #[serde(rename = "LATITUD", default, deserialize_with = "parse_f64_option")]
    pub latitude: Option<f64>,
    #[serde(rename = "LONGITUD", default, deserialize_with = "parse_f64_option")]
    pub longitude: Option<f64>,
    #[serde(rename = "SPEED", default, deserialize_with = "parse_f64_option")]
    pub speed: Option<f64>,
    #[serde(rename = "ODOMETER", default, deserialize_with = "parse_f64_option")]
    pub odometer: Option<f64>,
    #[serde(rename = "COURSE", default, deserialize_with = "parse_f64_option")]
    pub heading: Option<f64>,
    #[serde(rename = "DEVICE_ID")]
    pub device_id: Option<String>,
    pub raw_code: Option<String>,
    pub correlation_id: Option<String>,
}



#[derive(Debug, Deserialize, Serialize)]
pub struct Metadata {
    #[serde(rename = "DEVICE_ID")]
    pub device_id: Option<String>,
    #[serde(flatten)]
    pub other: HashMap<String, Value>,
}

impl MqttMessage {
    pub fn get_device_id(&self) -> Option<&String> {
        self.data.device_id.as_ref().or(self.metadata.device_id.as_ref())
    }
}

fn parse_f64_option<'de, D>(deserializer: D) -> Result<Option<f64>, D::Error>
where
    D: Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum StringOrFloat {
        String(String),
        Float(f64),
    }

    let v: Option<StringOrFloat> = Option::deserialize(deserializer)?;
    match v {
        Some(StringOrFloat::Float(f)) => Ok(Some(f)),
        Some(StringOrFloat::String(s)) => {
            if s.trim().is_empty() {
                Ok(None)
            } else {
                s.parse::<f64>().map(Some).map_err(serde::de::Error::custom)
            }
        }
        None => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parsing_problematic_payload() {
        let payload = r#"
        {
            "data": {
                "BACKUP_BATTERY_VOLTAGE": "12.34",
                "CELL_ID": "0376C501",
                "COURSE": "0.00",
                "DELIVERY_TYPE": "REAL TIME",
                "DEVICE_ID": "0848086072",
                "ENGINE_STATUS": "OFF",
                "FIRMWARE": "1.0.17",
                "FIX_": "1",
                "GPS_DATETIME": "2025-11-29 06:15:15",
                "GPS_EPOCH": "1764396915",
                "IDLE_TIME": "0",
                "LAC": "5B10",
                "LATITUD": "+20.652494",
                "LONGITUD": "-100.391404",
                "MAIN_BATTERY_VOLTAGE": "252",
                "MCC": "334",
                "MNC": "20",
                "MODEL": "84",
                "MSG_CLASS": "STATUS",
                "MSG_COUNTER": "0123",
                "ODOMETER": "0",
                "RX_LVL": "45",
                "SATELLITES": "9",
                "SPEED": "0.00",
                "SPEED_TIME": "1300",
                "TOTAL_DISTANCE": "0",
                "TRIP_DISTANCE": "0"
            },
            "decoded": {},
            "metadata": {
                "BYTES": 188,
                "CLIENT_IP": "44.204.32.23",
                "CLIENT_PORT": 47884,
                "DECODED_EPOCH": 1764398681921,
                "RECEIVED_EPOCH": 1764398681920,
                "WORKER_ID": 3
            },
            "raw": "...",
            "uuid": "d52b1454-d43d-50fa-99ca-79515c904162"
        }
        "#;

        let msg: MqttMessage = serde_json::from_str(payload).unwrap();
        assert_eq!(msg.data.latitude, Some(20.652494));
        assert_eq!(msg.data.longitude, Some(-100.391404));
        assert_eq!(msg.data.speed, Some(0.0));
        assert_eq!(msg.data.odometer, Some(0.0));
        assert_eq!(msg.data.device_id, Some("0848086072".to_string()));
    }
}
