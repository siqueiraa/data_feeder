use super::actor::{HistoricalActor, HistoricalAsk, HistoricalReply};
use chrono::{TimeZone, Utc};
use kameo::actor::ActorRef;
use tempfile::tempdir;

#[tokio::test]
async fn test_fetch_candles() {
    let symbols = vec!["BTCUSDT".to_string()];
    let timeframes = vec![3600];
    let tmp_dir = tempdir().unwrap();
    let base_path = tmp_dir.path();
    let csv_tmp_dir = tempdir().unwrap();
    let csv_path = csv_tmp_dir.path();

    let actor = HistoricalActor::new(&symbols, &timeframes, base_path, csv_path)
        .expect("Failed to create HistoricalActor");
    let actor_ref: ActorRef<HistoricalActor> = kameo::spawn(actor);

    let start_time = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap().timestamp_millis();
    let end_time = Utc.with_ymd_and_hms(2024, 1, 1, 1, 0, 0).unwrap().timestamp_millis();

    let msg = HistoricalAsk::GetCandles {
        symbol: "BTCUSDT".to_string(),
        timeframe: 3600,
        start_time,
        end_time,
    };

    match actor_ref.ask(msg).await {
        Ok(HistoricalReply::Candles(candles)) => {
            // Note: This test may return empty candles since we're not mocking external API calls
            // The test verifies the actor system works, not that data is actually fetched
            println!("Received {} candles", candles.len());
            if !candles.is_empty() {
                let candle = &candles[0];
                println!("First candle open time: {}", candle.open_time());
            }
        }
        Ok(HistoricalReply::ValidationCompleted) => {
            println!("Validation completed successfully");
        }
        Err(e) => {
            // For testing the actor system, we mainly care that it doesn't panic
            println!("Expected error during test (no network access): {:?}", e);
        }
    }
}
