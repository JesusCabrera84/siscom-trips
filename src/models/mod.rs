pub mod trip;
pub mod trip_alerts;
pub mod trip_points;

pub mod siscom {
    pub mod v1 {
        include!(concat!(env!("OUT_DIR"), "/siscom.v1.rs"));
    }
}
