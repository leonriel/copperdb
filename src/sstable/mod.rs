use crate::sstable::{block::BlockError};

pub mod block;
pub mod reader;
pub mod writer;

#[derive(thiserror::Error, Debug)]
pub enum SSTableError {
    #[error("SSTable block error: {0}")]
    Block(#[from] BlockError),
}