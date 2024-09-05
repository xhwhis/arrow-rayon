use arrow_array::types::{UInt16Type, UInt32Type, UInt64Type, UInt8Type};
use arrow_array::{UInt16Array, UInt32Array, UInt64Array, UInt8Array};
use rayon::iter::ParallelIterator;

use crate::parallel_primitive_array::{ParallelPrimitiveArray, ParallelPrimitiveArrayRef};

pub type ParallelUInt8Array = ParallelPrimitiveArray<UInt8Type>;
pub type ParallelUInt8ArrayRef<'data> = ParallelPrimitiveArrayRef<'data, UInt8Type>;
pub type ParallelUInt16Array = ParallelPrimitiveArray<UInt16Type>;
pub type ParallelUInt16ArrayRef<'data> = ParallelPrimitiveArrayRef<'data, UInt16Type>;
pub type ParallelUInt32Array = ParallelPrimitiveArray<UInt32Type>;
pub type ParallelUInt32ArrayRef<'data> = ParallelPrimitiveArrayRef<'data, UInt32Type>;
pub type ParallelUInt64Array = ParallelPrimitiveArray<UInt64Type>;
pub type ParallelUInt64ArrayRef<'data> = ParallelPrimitiveArrayRef<'data, UInt64Type>;

pub trait UInt8ArrayRefParallelIterator<'data> {
    type Iter: ParallelIterator<Item = Option<u8>>;

    fn par_iter(&'data self) -> Self::Iter;
}

impl<'data> UInt8ArrayRefParallelIterator<'data> for UInt8Array {
    type Iter = ParallelUInt8ArrayRef<'data>;

    fn par_iter(&'data self) -> Self::Iter {
        ParallelUInt8ArrayRef::new(self)
    }
}

pub trait UInt16ArrayRefParallelIterator<'data> {
    type Iter: ParallelIterator<Item = Option<u16>>;

    fn par_iter(&'data self) -> Self::Iter;
}

impl<'data> UInt16ArrayRefParallelIterator<'data> for UInt16Array {
    type Iter = ParallelUInt16ArrayRef<'data>;

    fn par_iter(&'data self) -> Self::Iter {
        ParallelUInt16ArrayRef::new(self)
    }
}

pub trait UInt32ArrayRefParallelIterator<'data> {
    type Iter: ParallelIterator<Item = Option<u32>>;

    fn par_iter(&'data self) -> Self::Iter;
}

impl<'data> UInt32ArrayRefParallelIterator<'data> for UInt32Array {
    type Iter = ParallelUInt32ArrayRef<'data>;

    fn par_iter(&'data self) -> Self::Iter {
        ParallelUInt32ArrayRef::new(self)
    }
}

pub trait UInt64ArrayRefParallelIterator<'data> {
    type Iter: ParallelIterator<Item = Option<u64>>;

    fn par_iter(&'data self) -> Self::Iter;
}

impl<'data> UInt64ArrayRefParallelIterator<'data> for UInt64Array {
    type Iter = ParallelUInt64ArrayRef<'data>;

    fn par_iter(&'data self) -> Self::Iter {
        ParallelUInt64ArrayRef::new(self)
    }
}
