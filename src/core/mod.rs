use std::cmp;

pub enum CoreError {
    CorruptData(String)
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InternalKey {
    pub user_key: String,
    pub seq_num: u64,
}

impl Ord for InternalKey {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        match self.user_key.cmp(&other.user_key) {
            cmp::Ordering::Equal => other.seq_num.cmp(&self.seq_num),
            ord => ord,
        }
    }
}

impl PartialOrd for InternalKey {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

pub trait KvIterator: Send {
    /// Returns the next Key, Value, and Sequence Number
    fn next(&mut self) -> Option<(String, Record, u64)>;

    /// True if the iterator is exhausted
    fn is_valid(&self) -> bool;
}

/// Represents the physical byte tag written to disk for a Record
#[repr(u8)]
#[derive(Debug, PartialEq, Eq)]
pub enum RecordTag {
    Delete = 0,
    Put = 1,
}

impl TryFrom<u8> for RecordTag {
    type Error = CoreError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(RecordTag::Delete),
            1 => Ok(RecordTag::Put),
            _ => Err(CoreError::CorruptData(format!(
                "Encountered invalid RecordTag byte: {}",
                value
            ))),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum Record {
    Put(Vec<u8>),
    Delete,
}

impl Record {
    pub fn tag(&self) -> RecordTag {
        match self {
            Record::Delete => RecordTag::Delete,
            Record::Put(_) => RecordTag::Put,
        }
    }
}