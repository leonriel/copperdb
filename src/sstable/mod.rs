use std::mem::size_of;

pub mod block;
pub mod reader;
pub mod writer;
pub(crate) mod iter;

type MetaOffset = u64;
type IndexOffset = u64;
type MagicNumber = u64;

pub(crate) const MAGIC_NUMBER: MagicNumber = 0xDEADBEEFCAFEBABE;

pub(crate) const META_OFFSET_SIZE: usize  = size_of::<MetaOffset>();
pub(crate) const INDEX_OFFSET_SIZE: usize = size_of::<IndexOffset>();
pub(crate) const MAGIC_SIZE: usize        = size_of::<MagicNumber>();
pub(crate) const FOOTER_SIZE: usize       = META_OFFSET_SIZE + INDEX_OFFSET_SIZE + MAGIC_SIZE;
