use glob;
use std::io;

/// KvError
#[derive(Debug)]
pub enum KvError {
  /// IO error
  Io(io::Error),
  /// Serialization or deserialization error
  Serde(serde_json::Error),
  /// Removing non-existent key error
  KeyNotFound,
  /// Unexpected action error.
  /// It indicated a corrupted log or a program bug.
  UnexpectedAction,
  /// Unable to glob dirpath error
  PatternError(glob::PatternError),
}

impl From<io::Error> for KvError {
  fn from(err: io::Error) -> KvError {
    KvError::Io(err)
  }
}

impl From<serde_json::Error> for KvError {
  fn from(err: serde_json::Error) -> KvError {
    KvError::Serde(err)
  }
}

impl From<glob::PatternError> for KvError {
  fn from(err: glob::PatternError) -> KvError {
    KvError::PatternError(err)
  }
}

/// Result type for kvs
pub type Result<T> = std::result::Result<T, KvError>;
