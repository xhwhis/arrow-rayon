use arrow_array::types::{Date32Type, Date64Type};
use arrow_array::{Date32Array, Date64Array};
use rayon::iter::ParallelIterator;

use crate::parallel_primitive_array::{ParallelPrimitiveArray, ParallelPrimitiveArrayRef};

pub type ParallelDate32Array = ParallelPrimitiveArray<Date32Type>;
pub type ParallelDate32ArrayRef<'data> = ParallelPrimitiveArrayRef<'data, Date32Type>;
pub type ParallelDate64Array = ParallelPrimitiveArray<Date64Type>;
pub type ParallelDate64ArrayRef<'data> = ParallelPrimitiveArrayRef<'data, Date64Type>;

pub trait Date32ArrayRefParallelIterator<'data> {
    type Iter: ParallelIterator<Item = Option<i32>>;

    fn par_iter(&'data self) -> Self::Iter;
}

impl<'data> Date32ArrayRefParallelIterator<'data> for Date32Array {
    type Iter = ParallelDate32ArrayRef<'data>;

    fn par_iter(&'data self) -> Self::Iter {
        ParallelDate32ArrayRef::new(self)
    }
}

pub trait Date64ArrayRefParallelIterator<'data> {
    type Iter: ParallelIterator<Item = Option<i64>>;

    fn par_iter(&'data self) -> Self::Iter;
}

impl<'data> Date64ArrayRefParallelIterator<'data> for Date64Array {
    type Iter = ParallelDate64ArrayRef<'data>;

    fn par_iter(&'data self) -> Self::Iter {
        ParallelDate64ArrayRef::new(self)
    }
}
