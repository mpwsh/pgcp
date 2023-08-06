use chrono::{DateTime, NaiveDateTime, Utc};
use tokio_postgres::Row;
use uuid::Uuid;

macro_rules! try_string {
    ($row:expr, $col:expr, $type:ty) => {
        $row.try_get::<_, $type>($col)
            .ok()
            .map(|val: $type| val.to_string())
    };
}

pub fn any_to_string(row: &Row, col: &str) -> String {
    try_string!(row, col, String)
        .or_else(|| try_string!(row, col, i32))
        .or_else(|| try_string!(row, col, f32))
        .or_else(|| try_string!(row, col, Uuid))
        .or_else(|| {
            row.try_get::<_, NaiveDateTime>(col)
                .ok()
                .map(|val| DateTime::<Utc>::from_utc(val, Utc).to_string())
        })
        .or_else(|| try_string!(row, col, DateTime<Utc>))
        .unwrap_or_else(|| panic!("Error: could not convert the value in column '{}'", col))
}
