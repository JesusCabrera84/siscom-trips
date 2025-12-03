use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;
use std::collections::HashMap;

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
        self.data
            .device_id
            .as_ref()
            .or(self.metadata.device_id.as_ref())
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

    #[test]
    fn test_parsing_queclink_ignition_on() {
        let payload = r#"
        {
            "data": {
                "ALERT": "Turn On",
                "ALTITUDE": "1820.7",
                "CELL_ID": "03673721",
                "COURSE": "128",
                "DEVICE_ID": "867564050638581",
                "FIX_": "1",
                "GPS_DATETIME": "2025-12-03 19:58:16",
                "GPS_EPOCH": "1764791896",
                "LAC": "17AE",
                "LATITUD": "20.605243",
                "LONGITUD": "-100.384140",
                "MCC": "0334",
                "MNC": "0050",
                "MSG_CLASS": "ALERT",
                "MSG_COUNTER": "06C5",
                "ODOMETER": "121.8",
                "SPEED": "33.9"
            },
            "decoded": {
                "QueclinkRaw": {
                    "ALTITUDE": "1820.7",
                    "CELL_ID": "03673721",
                    "CRS": "128",
                    "DEVICE_ID": "867564050638581",
                    "FIX": "1",
                    "GPS_DATE_TIME": "20251203195816",
                    "HEADER": "+RESP:GTVGN",
                    "KILOMETERS": "121.8",
                    "LAC": "17AE",
                    "LAT": "20.605243",
                    "LON": "-100.384140",
                    "MCC": "0334",
                    "MNC": "0050",
                    "MSG_NUM": "06C5",
                    "OFF_DURATION": "110",
                    "PROTOCOL_VERSION": "8020070305",
                    "RESERVED": "00",
                    "RPT_TYPE": "4",
                    "SEND_DATE_TIME": "20251203195818",
                    "SPD": "33.9",
                    "TRIP_HOURMETER": ""
                }
            },
            "metadata": {
                "BYTES": 158,
                "CLIENT_IP": "44.204.32.23",
                "CLIENT_PORT": 14362,
                "DECODED_EPOCH": 1764791898674,
                "RECEIVED_EPOCH": 1764791898673,
                "WORKER_ID": 3
            },
            "raw": "+RESP:GTVGN,8020070305,867564050638581,,00,4,110,1,33.9,128,1820.7,-100.384140,20.605243,20251203195816,0334,0050,17AE,03673721,00,,121.8,20251203195818,06C5$",
            "uuid": "40f8ef36-4d01-50cd-88da-06fad8a19bac"
        }
        "#;

        let msg: MqttMessage = serde_json::from_str(payload).unwrap();

        // Verificar parsing de datos básicos
        assert_eq!(msg.data.device_id, Some("867564050638581".to_string()));
        assert_eq!(msg.data.latitude, Some(20.605243));
        assert_eq!(msg.data.longitude, Some(-100.384140));
        assert_eq!(msg.data.speed, Some(33.9));
        assert_eq!(msg.data.heading, Some(128.0));
        assert_eq!(msg.data.odometer, Some(121.8));
        assert_eq!(
            msg.data.gps_datetime,
            Some("2025-12-03 19:58:16".to_string())
        );
        assert_eq!(msg.data.msg_class, Some("ALERT".to_string()));
        assert_eq!(msg.uuid, "40f8ef36-4d01-50cd-88da-06fad8a19bac");

        // Verificar que el alert es "Turn On" (ignition on para Queclink)
        assert_eq!(msg.data.alert, Some("Turn On".to_string()));

        // Verificar que get_device_id funciona correctamente
        assert_eq!(msg.get_device_id(), Some(&"867564050638581".to_string()));
    }
}
