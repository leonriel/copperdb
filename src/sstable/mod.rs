use std::mem::size_of;

use crate::sstable::{block::BlockError, reader::ReaderError, writer::WriterError};

pub mod block;
pub mod reader;
pub mod writer;

pub(crate) type IndexOffset = u64;
pub(crate) type MagicNumber = u64;

pub(crate) const MAGIC_NUMBER: MagicNumber       = 0xDEADBEEFCAFEBABE;

pub(crate) const INDEX_OFFSET_SIZE: usize = size_of::<IndexOffset>();
pub(crate) const MAGIC_SIZE: usize        = size_of::<MagicNumber>();
pub(crate) const FOOTER_SIZE: usize       = INDEX_OFFSET_SIZE + MAGIC_SIZE;

#[derive(thiserror::Error, Debug)]
pub enum SSTableError {
    #[error("SSTable block error: {0}")]
    Block(#[from] BlockError),

    #[error("SSTable writer error: {0}")]
    Writer(#[from] WriterError),

    #[error("SSTable reader error: {0}")]
    Reader(#[from] ReaderError),
}
