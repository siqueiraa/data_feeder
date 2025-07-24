use serde::{Deserialize, Serialize};

pub type TimestampMS = i64;
pub type Seconds = u64;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FuturesOHLCVCandle {
    pub open_time: TimestampMS,
    pub close_time: TimestampMS,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
    pub number_of_trades: u64,
    pub taker_buy_base_asset_volume: f64,
    pub closed: bool,
}

impl FuturesOHLCVCandle {
    #[allow(clippy::too_many_arguments)]
    pub fn new_from_values(
        open_time: TimestampMS,
        close_time: TimestampMS,
        open: f64,
        high: f64,
        low: f64,
        close: f64,
        volume: f64,
        _timestamp: TimestampMS, // This is a dummy argument for compatibility
        closed: bool,
    ) -> Self {
        Self {
            open_time,
            close_time,
            open,
            high,
            low,
            close,
            volume,
            number_of_trades: 0,
            taker_buy_base_asset_volume: 0.0,
            closed,
        }
    }

    pub fn open_time(&self) -> TimestampMS {
        self.open_time
    }

    pub fn close_time(&self) -> TimestampMS {
        self.close_time
    }

    pub fn open(&self) -> f64 {
        self.open
    }

    pub fn high(&self) -> f64 {
        self.high
    }

    pub fn low(&self) -> f64 {
        self.low
    }

    pub fn close(&self) -> f64 {
        self.close
    }

    pub fn volume(&self) -> f64 {
        self.volume
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct TimeRange {
    pub start: TimestampMS,
    pub end: TimestampMS,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FuturesExchangeTrade {
    pub timestamp: TimestampMS,
    pub price: f64,
    pub size: f64,
    pub is_buyer_maker: bool,
}
