use crate::historical::errors::HistoricalDataError;

/// Common error mapping utilities to reduce repetitive .map_err() patterns
/// Map IO errors to HistoricalDataError with context
pub fn map_io_error(_context: &str) -> impl Fn(std::io::Error) -> HistoricalDataError + '_ {
    move |e| HistoricalDataError::Io(e)
}

/// Map database initialization errors with context
pub fn map_db_init_error(context: &str) -> impl Fn(heed::Error) -> HistoricalDataError + '_ {
    move |e| HistoricalDataError::DatabaseInitialization(format!("{}: {}", context, e))
}

/// Map directory creation errors with context
pub fn map_dir_creation_error(path: &std::path::Path) -> impl Fn(std::io::Error) -> HistoricalDataError + '_ {
    let path_str = path.display().to_string();
    move |e| HistoricalDataError::DirectoryCreation(format!("Failed to create directory '{}': {}", path_str, e))
}

/// Map semaphore acquisition errors with context
pub fn map_semaphore_error(context: &str) -> impl Fn(tokio::sync::AcquireError) -> HistoricalDataError + '_ {
    move |e| HistoricalDataError::SemaphoreAcquisition(format!("{}: {}", context, e))
}

/// Helper trait for chaining error conversions
pub trait ErrorContext<T> {
    fn with_db_context(self, context: &str) -> Result<T, HistoricalDataError>;
    fn with_io_context(self, context: &str) -> Result<T, HistoricalDataError>;
    fn with_semaphore_context(self, context: &str) -> Result<T, HistoricalDataError>;
}

impl<T> ErrorContext<T> for Result<T, heed::Error> {
    fn with_db_context(self, context: &str) -> Result<T, HistoricalDataError> {
        self.map_err(|e| HistoricalDataError::DatabaseInitialization(format!("{}: {}", context, e)))
    }
    
    fn with_io_context(self, _context: &str) -> Result<T, HistoricalDataError> {
        // This conversion doesn't make sense but required by trait
        self.map_err(HistoricalDataError::Heed)
    }
    
    fn with_semaphore_context(self, _context: &str) -> Result<T, HistoricalDataError> {
        // This conversion doesn't make sense but required by trait
        self.map_err(HistoricalDataError::Heed)
    }
}

impl<T> ErrorContext<T> for Result<T, std::io::Error> {
    fn with_db_context(self, _context: &str) -> Result<T, HistoricalDataError> {
        // This conversion doesn't make sense but required by trait
        self.map_err(HistoricalDataError::Io)
    }
    
    fn with_io_context(self, _context: &str) -> Result<T, HistoricalDataError> {
        self.map_err(HistoricalDataError::Io)
    }
    
    fn with_semaphore_context(self, _context: &str) -> Result<T, HistoricalDataError> {
        // This conversion doesn't make sense but required by trait
        self.map_err(HistoricalDataError::Io)
    }
}

impl<T> ErrorContext<T> for Result<T, tokio::sync::AcquireError> {
    fn with_db_context(self, _context: &str) -> Result<T, HistoricalDataError> {
        // This conversion doesn't make sense but required by trait
        self.map_err(|e| HistoricalDataError::SemaphoreAcquisition(format!("Semaphore acquisition error: {}", e)))
    }
    
    fn with_io_context(self, _context: &str) -> Result<T, HistoricalDataError> {
        // This conversion doesn't make sense but required by trait
        self.map_err(|e| HistoricalDataError::SemaphoreAcquisition(format!("Semaphore acquisition error: {}", e)))
    }
    
    fn with_semaphore_context(self, context: &str) -> Result<T, HistoricalDataError> {
        self.map_err(|e| HistoricalDataError::SemaphoreAcquisition(format!("{}: {}", context, e)))
    }
}