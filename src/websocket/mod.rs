pub mod actor;
pub mod binance;
pub mod gate_io;
pub mod connection;
pub mod types;
pub mod optimized_parser;

#[cfg(test)]
pub mod tests;

pub use actor::{WebSocketActor, WebSocketTell, WebSocketAsk, WebSocketReply};
pub use binance::kline::{BinanceKlineEvent, BinanceKlineData};
pub use gate_io::{GateIoOrderEvent, GateIoPositionEvent, GateIoTradeEvent, GateIoBalanceEvent};
pub use connection::ConnectionManager;
pub use types::{StreamType, WebSocketError, ConnectionStatus};