use chrono::NaiveDate;
use tokio_postgres::{Client, Error as PostgresError};
use tracing::{debug, error, info, warn};

use super::structs::{VolumeProfileData, ValueArea};
use crate::historical::structs::TimestampMS;

// Remove the custom error conversion - we'll handle this differently

/// Database operations for volume profiles
#[derive(Debug, Clone)]
pub struct VolumeProfileDatabase {
    /// Table name for volume profiles
    table_name: String,
}

impl VolumeProfileDatabase {
    /// Create new database operations handler
    pub fn new() -> Self {
        Self {
            table_name: "volume_profiles".to_string(),
        }
    }

    /// SQL to create volume profiles table
    pub fn get_create_table_sql(&self) -> String {
        format!(
            r#"
            CREATE TABLE IF NOT EXISTS {} (
                symbol VARCHAR(20) NOT NULL,
                date DATE NOT NULL,
                total_volume DECIMAL(20,8) NOT NULL,
                vwap DECIMAL(20,8) NOT NULL,
                poc DECIMAL(20,8) NOT NULL,
                value_area_high DECIMAL(20,8) NOT NULL,
                value_area_low DECIMAL(20,8) NOT NULL,
                value_area_volume_percentage DECIMAL(8,4) NOT NULL,
                value_area_volume DECIMAL(20,8) NOT NULL,
                price_increment DECIMAL(20,8) NOT NULL,
                min_price DECIMAL(20,8) NOT NULL,
                max_price DECIMAL(20,8) NOT NULL,
                candle_count INTEGER NOT NULL,
                last_updated BIGINT NOT NULL,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                
                PRIMARY KEY (symbol, date)
            );

            -- Create indexes for optimal query performance
            CREATE INDEX IF NOT EXISTS idx_{}_date ON {} (date);
            CREATE INDEX IF NOT EXISTS idx_{}_symbol ON {} (symbol);
            CREATE INDEX IF NOT EXISTS idx_{}_symbol_date ON {} (symbol, date);
            
            -- Create partial index for recent data (last 30 days)
            CREATE INDEX IF NOT EXISTS idx_{}_recent 
            ON {} (symbol, date) 
            WHERE date >= CURRENT_DATE - INTERVAL '30 days';

            -- Create index on VWAP for price analysis
            CREATE INDEX IF NOT EXISTS idx_{}_vwap 
            ON {} (vwap);
            
            -- Create index on POC for volume analysis
            CREATE INDEX IF NOT EXISTS idx_{}_poc 
            ON {} (poc);
            "#,
            self.table_name,
            self.table_name, self.table_name,
            self.table_name, self.table_name,
            self.table_name, self.table_name,
            self.table_name, self.table_name,
            self.table_name, self.table_name,
            self.table_name, self.table_name
        )
    }

    /// SQL to create or replace the update trigger function
    pub fn get_create_trigger_sql(&self) -> String {
        format!(
            r#"
            -- Create or replace trigger function to update updated_at timestamp
            CREATE OR REPLACE FUNCTION update_{}_updated_at()
            RETURNS TRIGGER AS $$
            BEGIN
                NEW.updated_at = CURRENT_TIMESTAMP;
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;

            -- Drop trigger if exists and create new one
            DROP TRIGGER IF EXISTS trigger_update_{}_updated_at ON {};
            CREATE TRIGGER trigger_update_{}_updated_at
                BEFORE UPDATE ON {}
                FOR EACH ROW
                EXECUTE FUNCTION update_{}_updated_at();
            "#,
            self.table_name,
            self.table_name, self.table_name,
            self.table_name, self.table_name,
            self.table_name
        )
    }

    /// Upsert volume profile data
    pub async fn upsert_volume_profile(
        &self,
        client: &Client,
        symbol: &str,
        date: NaiveDate,
        profile_data: &VolumeProfileData,
    ) -> Result<u64, PostgresError> {
        debug!("Upserting volume profile: {} on {} ({} price levels)", 
               symbol, date, profile_data.price_levels.len());

        // Prepare upsert SQL with individual columns
        let sql = format!(
            r#"
            INSERT INTO {} (
                symbol, 
                date, 
                total_volume,
                vwap,
                poc,
                value_area_high,
                value_area_low,
                value_area_volume_percentage,
                value_area_volume,
                price_increment, 
                min_price, 
                max_price, 
                candle_count,
                last_updated
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
            ON CONFLICT (symbol, date) 
            DO UPDATE SET
                total_volume = EXCLUDED.total_volume,
                vwap = EXCLUDED.vwap,
                poc = EXCLUDED.poc,
                value_area_high = EXCLUDED.value_area_high,
                value_area_low = EXCLUDED.value_area_low,
                value_area_volume_percentage = EXCLUDED.value_area_volume_percentage,
                value_area_volume = EXCLUDED.value_area_volume,
                price_increment = EXCLUDED.price_increment,
                min_price = EXCLUDED.min_price,
                max_price = EXCLUDED.max_price,
                candle_count = EXCLUDED.candle_count,
                last_updated = EXCLUDED.last_updated,
                updated_at = CURRENT_TIMESTAMP
            "#,
            self.table_name
        );

        // Execute upsert
        let rows_affected = client
            .execute(
                &sql,
                &[
                    &symbol,
                    &date,
                    &profile_data.total_volume,
                    &profile_data.vwap,
                    &profile_data.poc,
                    &profile_data.value_area.high,
                    &profile_data.value_area.low,
                    &profile_data.value_area.volume_percentage,
                    &profile_data.value_area.volume,
                    &profile_data.price_increment,
                    &profile_data.min_price,
                    &profile_data.max_price,
                    &(profile_data.candle_count as i32),
                    &profile_data.last_updated,
                ],
            )
            .await?;

        if rows_affected > 0 {
            debug!("Successfully upserted volume profile: {} on {} ({} rows affected)", 
                   symbol, date, rows_affected);
        } else {
            warn!("Volume profile upsert returned 0 rows affected: {} on {}", symbol, date);
        }

        Ok(rows_affected)
    }

    /// Batch upsert multiple volume profiles
    pub async fn batch_upsert_volume_profiles(
        &self,
        client: &mut Client,
        profiles: &[(String, NaiveDate, VolumeProfileData)],
    ) -> Result<u64, PostgresError> {
        if profiles.is_empty() {
            return Ok(0);
        }

        info!("Batch upserting {} volume profiles", profiles.len());

        // Start transaction for atomic batch operation
        let transaction = client.transaction().await?;

        let mut total_rows_affected = 0;

        // Process each profile in the batch
        for (symbol, date, profile_data) in profiles {
            match self.upsert_volume_profile_in_transaction(&transaction, symbol, *date, profile_data).await {
                Ok(rows) => {
                    total_rows_affected += rows;
                }
                Err(e) => {
                    error!("Failed to upsert volume profile in batch: {} on {}: {}", symbol, date, e);
                    // Rollback transaction and return error
                    transaction.rollback().await?;
                    return Err(e);
                }
            }
        }

        // Commit transaction
        transaction.commit().await?;

        info!("Successfully batch upserted {} volume profiles ({} total rows affected)", 
              profiles.len(), total_rows_affected);

        Ok(total_rows_affected)
    }

    /// Upsert volume profile within a transaction
    async fn upsert_volume_profile_in_transaction(
        &self,
        transaction: &tokio_postgres::Transaction<'_>,
        symbol: &str,
        date: NaiveDate,
        profile_data: &VolumeProfileData,
    ) -> Result<u64, PostgresError> {
        // Prepare upsert SQL with individual columns
        let sql = format!(
            r#"
            INSERT INTO {} (
                symbol, 
                date, 
                total_volume,
                vwap,
                poc,
                value_area_high,
                value_area_low,
                value_area_volume_percentage,
                value_area_volume,
                price_increment, 
                min_price, 
                max_price, 
                candle_count,
                last_updated
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
            ON CONFLICT (symbol, date) 
            DO UPDATE SET
                total_volume = EXCLUDED.total_volume,
                vwap = EXCLUDED.vwap,
                poc = EXCLUDED.poc,
                value_area_high = EXCLUDED.value_area_high,
                value_area_low = EXCLUDED.value_area_low,
                value_area_volume_percentage = EXCLUDED.value_area_volume_percentage,
                value_area_volume = EXCLUDED.value_area_volume,
                price_increment = EXCLUDED.price_increment,
                min_price = EXCLUDED.min_price,
                max_price = EXCLUDED.max_price,
                candle_count = EXCLUDED.candle_count,
                last_updated = EXCLUDED.last_updated,
                updated_at = CURRENT_TIMESTAMP
            "#,
            self.table_name
        );

        // Execute upsert within transaction
        let rows_affected = transaction
            .execute(
                &sql,
                &[
                    &symbol,
                    &date,
                    &profile_data.total_volume,
                    &profile_data.vwap,
                    &profile_data.poc,
                    &profile_data.value_area.high,
                    &profile_data.value_area.low,
                    &profile_data.value_area.volume_percentage,
                    &profile_data.value_area.volume,
                    &profile_data.price_increment,
                    &profile_data.min_price,
                    &profile_data.max_price,
                    &(profile_data.candle_count as i32),
                    &profile_data.last_updated,
                ],
            )
            .await?;

        Ok(rows_affected)
    }

    /// Get volume profile for specific symbol and date
    pub async fn get_volume_profile(
        &self,
        client: &Client,
        symbol: &str,
        date: NaiveDate,
    ) -> Result<Option<VolumeProfileData>, PostgresError> {
        debug!("Retrieving volume profile: {} on {}", symbol, date);

        let sql = format!(
            "SELECT total_volume, vwap, poc, value_area_high, value_area_low, value_area_volume_percentage, value_area_volume, price_increment, min_price, max_price, candle_count, last_updated FROM {} WHERE symbol = $1 AND date = $2",
            self.table_name
        );

        match client.query_opt(&sql, &[&symbol, &date]).await? {
            Some(row) => {
                let total_volume: f64 = row.get("total_volume");
                let vwap: f64 = row.get("vwap");
                let poc: f64 = row.get("poc");
                let value_area_high: f64 = row.get("value_area_high");
                let value_area_low: f64 = row.get("value_area_low");
                let value_area_volume_percentage: f64 = row.get("value_area_volume_percentage");
                let value_area_volume: f64 = row.get("value_area_volume");
                let price_increment: f64 = row.get("price_increment");
                let min_price: f64 = row.get("min_price");
                let max_price: f64 = row.get("max_price");
                let candle_count: i32 = row.get("candle_count");
                let last_updated: i64 = row.get("last_updated");

                let profile_data = VolumeProfileData {
                    date: date.format("%Y-%m-%d").to_string(),
                    price_levels: Vec::new(), // Empty since we only store aggregated data
                    total_volume,
                    vwap,
                    poc,
                    value_area: ValueArea {
                        high: value_area_high,
                        low: value_area_low,
                        volume_percentage: value_area_volume_percentage,
                        volume: value_area_volume,
                    },
                    price_increment,
                    min_price,
                    max_price,
                    candle_count: candle_count as u32,
                    last_updated: last_updated as TimestampMS,
                };

                debug!("Successfully retrieved volume profile: {} on {}", symbol, date);
                Ok(Some(profile_data))
            }
            None => {
                debug!("No volume profile found: {} on {}", symbol, date);
                Ok(None)
            }
        }
    }

    /// Get volume profiles for symbol within date range
    pub async fn get_volume_profiles_in_range(
        &self,
        client: &Client,
        symbol: &str,
        start_date: NaiveDate,
        end_date: NaiveDate,
    ) -> Result<Vec<VolumeProfileData>, PostgresError> {
        debug!("Retrieving volume profiles: {} from {} to {}", symbol, start_date, end_date);

        let sql = format!(
            "SELECT date, total_volume, vwap, poc, value_area_high, value_area_low, value_area_volume_percentage, value_area_volume, price_increment, min_price, max_price, candle_count, last_updated FROM {} WHERE symbol = $1 AND date >= $2 AND date <= $3 ORDER BY date",
            self.table_name
        );

        let rows = client.query(&sql, &[&symbol, &start_date, &end_date]).await?;
        let mut profiles = Vec::with_capacity(rows.len());

        for row in rows {
            let date: NaiveDate = row.get("date");
            let total_volume: f64 = row.get("total_volume");
            let vwap: f64 = row.get("vwap");
            let poc: f64 = row.get("poc");
            let value_area_high: f64 = row.get("value_area_high");
            let value_area_low: f64 = row.get("value_area_low");
            let value_area_volume_percentage: f64 = row.get("value_area_volume_percentage");
            let value_area_volume: f64 = row.get("value_area_volume");
            let price_increment: f64 = row.get("price_increment");
            let min_price: f64 = row.get("min_price");
            let max_price: f64 = row.get("max_price");
            let candle_count: i32 = row.get("candle_count");
            let last_updated: i64 = row.get("last_updated");

            let profile_data = VolumeProfileData {
                date: date.format("%Y-%m-%d").to_string(),
                price_levels: Vec::new(), // Empty since we only store aggregated data
                total_volume,
                vwap,
                poc,
                value_area: ValueArea {
                    high: value_area_high,
                    low: value_area_low,
                    volume_percentage: value_area_volume_percentage,
                    volume: value_area_volume,
                },
                price_increment,
                min_price,
                max_price,
                candle_count: candle_count as u32,
                last_updated: last_updated as TimestampMS,
            };

            profiles.push(profile_data);
        }

        info!("Retrieved {} volume profiles for {} from {} to {}", 
              profiles.len(), symbol, start_date, end_date);

        Ok(profiles)
    }

    /// Delete old volume profiles (cleanup operation)
    pub async fn delete_old_profiles(
        &self,
        client: &Client,
        cutoff_date: NaiveDate,
    ) -> Result<u64, PostgresError> {
        info!("Deleting volume profiles older than {}", cutoff_date);

        let sql = format!("DELETE FROM {} WHERE date < $1", self.table_name);
        let rows_deleted = client.execute(&sql, &[&cutoff_date]).await?;

        if rows_deleted > 0 {
            info!("Deleted {} old volume profiles (older than {})", rows_deleted, cutoff_date);
        } else {
            debug!("No old volume profiles to delete");
        }

        Ok(rows_deleted)
    }

    /// Get database statistics
    pub async fn get_statistics(
        &self,
        client: &Client,
    ) -> Result<VolumeProfileDatabaseStats, PostgresError> {
        debug!("Retrieving volume profile database statistics");

        // Get total count and date range
        let stats_sql = format!(
            r#"
            SELECT 
                COUNT(*) as total_profiles,
                COUNT(DISTINCT symbol) as unique_symbols,
                MIN(date) as earliest_date,
                MAX(date) as latest_date,
                AVG(total_volume) as avg_daily_volume,
                AVG(candle_count) as avg_candle_count
            FROM {}
            "#,
            self.table_name
        );

        let row = client.query_one(&stats_sql, &[]).await?;

        let total_profiles: i64 = row.get("total_profiles");
        let unique_symbols: i64 = row.get("unique_symbols");
        let earliest_date: Option<NaiveDate> = row.get("earliest_date");
        let latest_date: Option<NaiveDate> = row.get("latest_date");
        let avg_daily_volume: Option<f64> = row.get("avg_daily_volume");
        let avg_candle_count: Option<f64> = row.get("avg_candle_count");

        // Get table size
        let size_sql = format!(
            r#"
            SELECT 
                pg_total_relation_size('{}') as table_size_bytes,
                pg_size_pretty(pg_total_relation_size('{}')) as table_size_pretty
            "#,
            self.table_name, self.table_name
        );

        let size_row = client.query_one(&size_sql, &[]).await?;
        let table_size_bytes: i64 = size_row.get("table_size_bytes");
        let table_size_pretty: String = size_row.get("table_size_pretty");

        let stats = VolumeProfileDatabaseStats {
            total_profiles: total_profiles as u64,
            unique_symbols: unique_symbols as u64,
            earliest_date,
            latest_date,
            avg_daily_volume: avg_daily_volume.unwrap_or(0.0),
            avg_candle_count: avg_candle_count.unwrap_or(0.0) as u32,
            table_size_bytes: table_size_bytes as u64,
            table_size_pretty,
        };

        debug!("Volume profile database statistics: {:?}", stats);
        Ok(stats)
    }

    /// Check if table exists and is properly set up
    pub async fn verify_table_schema(&self, client: &Client) -> Result<bool, PostgresError> {
        debug!("Verifying volume profile table schema");

        let sql = r#"
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = $1
            )
        "#;

        let row = client.query_one(sql, &[&self.table_name]).await?;
        let table_exists: bool = row.get(0);

        if !table_exists {
            warn!("Volume profile table '{}' does not exist", self.table_name);
            return Ok(false);
        }

        // Check if required columns exist
        let columns_sql = r#"
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_schema = 'public' 
            AND table_name = $1
            ORDER BY ordinal_position
        "#;

        let rows = client.query(columns_sql, &[&self.table_name]).await?;
        let mut required_columns = vec!["symbol", "date", "total_volume", "vwap", "poc", "value_area_high", "value_area_low"];
        
        for row in rows {
            let column_name: String = row.get("column_name");
            required_columns.retain(|&col| col != column_name);
        }

        if !required_columns.is_empty() {
            error!("Volume profile table missing required columns: {:?}", required_columns);
            return Ok(false);
        }

        info!("Volume profile table schema verified successfully");
        Ok(true)
    }
}

impl Default for VolumeProfileDatabase {
    fn default() -> Self {
        Self::new()
    }
}

/// Database statistics for volume profiles
#[derive(Debug, Clone)]
pub struct VolumeProfileDatabaseStats {
    pub total_profiles: u64,
    pub unique_symbols: u64,
    pub earliest_date: Option<NaiveDate>,
    pub latest_date: Option<NaiveDate>,
    pub avg_daily_volume: f64,
    pub avg_candle_count: u32,
    pub table_size_bytes: u64,
    pub table_size_pretty: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::volume_profile::structs::{PriceLevelData, ValueArea};

    fn create_test_profile_data() -> VolumeProfileData {
        VolumeProfileData {
            date: "2025-01-15".to_string(),
            price_levels: vec![
                PriceLevelData {
                    price: 50000.0,
                    volume: 1000.0,
                    percentage: 60.0,
                },
                PriceLevelData {
                    price: 50001.0,
                    volume: 666.67,
                    percentage: 40.0,
                },
            ],
            total_volume: 1666.67,
            vwap: 50000.4,
            poc: 50000.0,
            value_area: ValueArea {
                high: 50001.0,
                low: 50000.0,
                volume_percentage: 70.0,
                volume: 1166.67,
            },
            price_increment: 0.01,
            min_price: 50000.0,
            max_price: 50001.0,
            candle_count: 100,
            last_updated: 1737072000000,
        }
    }

    #[test]
    fn test_database_creation() {
        let db = VolumeProfileDatabase::new();
        assert_eq!(db.table_name, "volume_profiles");
    }

    #[test]
    fn test_create_table_sql() {
        let db = VolumeProfileDatabase::new();
        let sql = db.get_create_table_sql();
        
        assert!(sql.contains("CREATE TABLE IF NOT EXISTS volume_profiles"));
        assert!(sql.contains("symbol VARCHAR(20) NOT NULL"));
        assert!(sql.contains("date DATE NOT NULL"));
        assert!(sql.contains("total_volume DECIMAL(20,8) NOT NULL"));
        assert!(sql.contains("vwap DECIMAL(20,8) NOT NULL"));
        assert!(sql.contains("poc DECIMAL(20,8) NOT NULL"));
        assert!(sql.contains("PRIMARY KEY (symbol, date)"));
    }

    #[test]
    fn test_create_trigger_sql() {
        let db = VolumeProfileDatabase::new();
        let sql = db.get_create_trigger_sql();
        
        assert!(sql.contains("CREATE OR REPLACE FUNCTION update_volume_profiles_updated_at()"));
        assert!(sql.contains("CREATE TRIGGER trigger_update_volume_profiles_updated_at"));
        assert!(sql.contains("BEFORE UPDATE ON volume_profiles"));
    }

    #[test]
    fn test_json_serialization() {
        let profile_data = create_test_profile_data();
        
        // Test that profile data can be serialized to JSON
        let json_result = serde_json::to_value(&profile_data);
        assert!(json_result.is_ok());
        
        // Test that it can be deserialized back
        let json_value = json_result.unwrap();
        let json_str = serde_json::to_string(&json_value).unwrap();
        let deserialized_result = serde_json::from_str::<VolumeProfileData>(&json_str);
        assert!(deserialized_result.is_ok());
        
        let deserialized = deserialized_result.unwrap();
        assert_eq!(deserialized.date, profile_data.date);
        assert_eq!(deserialized.total_volume, profile_data.total_volume);
        assert_eq!(deserialized.price_levels.len(), profile_data.price_levels.len());
    }

    // Note: Database integration tests would require a running PostgreSQL instance
    // These would be implemented in integration test files
}