use std::mem::size_of;

use crate::sstable::{block::BlockError, reader::ReaderError, writer::WriterError};

pub mod block;
pub mod reader;
pub mod writer;

type IndexOffset = u64;
type MagicNumber = u64;

const MAGIC_NUMBER: MagicNumber = 0xDEADBEEFCAFEBABE;

const INDEX_OFFSET_SIZE: usize = size_of::<IndexOffset>();
const MAGIC_SIZE: usize = size_of::<MagicNumber>();
const FOOTER_SIZE: usize = INDEX_OFFSET_SIZE + MAGIC_SIZE;

#[derive(thiserror::Error, Debug)]
pub enum SSTableError {
    #[error("SSTable block error: {0}")]
    Block(#[from] BlockError),

    #[error("SSTable writer error: {0}")]
    Writer(#[from] WriterError),

    #[error("SSTable reader error: {0}")]
    Reader(#[from] ReaderError),
}
