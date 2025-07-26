use thiserror::Error;

#[derive(Error, Debug)]
pub enum HistoricalDataError {
    #[error("Database not found: {0}")]
    DatabaseNotFound(String),
    #[error("Heed error: {0}")]
    Heed(#[from] heed::Error),
    #[error("Reqwest error: {0}")]
    Reqwest(#[from] reqwest::Error),
    #[error("HTTP error: {0} - {1}")]
    HttpError(reqwest::StatusCode, String),
    #[error("File not found on server: {0}")]
    NotFound(String),
    #[error("Unexpected content type: {0}")]
    UnexpectedContentType(String),
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Zip error: {0}")]
    Zip(#[from] zip::result::ZipError),
    #[error("CSV error: {0}")]
    Csv(#[from] csv::Error),
    #[error("Parse error: {0}")]
    Parse(#[from] std::num::ParseFloatError),
    #[error("Parse error: {0}")]
    ParseString(String),
    #[error("Parse int error: {0}")]
    ParseInt(#[from] std::num::ParseIntError),
    #[error("Task join error: {0}")]
    TaskJoinError(tokio::task::JoinError),
    #[error("Validation error: {0}")]
    Validation(String),
    #[error("Not supported")]
    NotSupported,
    #[error("Checksum mismatch: {0}")]
    ChecksumError(String),
    #[error("No data found: {0}")]
    NoData(String),
    #[error("Storage error: {0}")]
    StorageError(String),
    #[error("Database initialization error: {0}")]
    DatabaseInitialization(String),
    #[error("Database error: {0}")]
    DatabaseError(String),
    #[error("Directory creation error: {0}")]
    DirectoryCreation(String),
    #[error("Semaphore acquisition error: {0}")]
    SemaphoreAcquisition(String),
}

impl From<tokio::task::JoinError> for HistoricalDataError {
    fn from(err: tokio::task::JoinError) -> Self {
        HistoricalDataError::TaskJoinError(err)
    }
}
