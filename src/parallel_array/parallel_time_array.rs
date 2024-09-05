use arrow_array::types::{
    Time32MillisecondType, Time32SecondType, Time64MicrosecondType, Time64NanosecondType,
};
use arrow_array::{
    Time32MillisecondArray, Time32SecondArray, Time64MicrosecondArray, Time64NanosecondArray,
};
use rayon::iter::ParallelIterator;

use crate::parallel_primitive_array::{ParallelPrimitiveArray, ParallelPrimitiveArrayRef};

pub type ParallelTime32SecondArray = ParallelPrimitiveArray<Time32SecondType>;
pub type ParallelTime32SecondArrayRef<'data> = ParallelPrimitiveArrayRef<'data, Time32SecondType>;
pub type ParallelTime32MillisecondArray = ParallelPrimitiveArray<Time32MillisecondType>;
pub type ParallelTime32MillisecondArrayRef<'data> =
    ParallelPrimitiveArrayRef<'data, Time32MillisecondType>;
pub type ParallelTime64MicrosecondArray = ParallelPrimitiveArray<Time64MicrosecondType>;
pub type ParallelTime64MicrosecondArrayRef<'data> =
    ParallelPrimitiveArrayRef<'data, Time64MicrosecondType>;
pub type ParallelTime64NanosecondArray = ParallelPrimitiveArray<Time64NanosecondType>;
pub type ParallelTime64NanosecondArrayRef<'data> =
    ParallelPrimitiveArrayRef<'data, Time64NanosecondType>;

pub trait Time32SecondArrayRefParallelIterator<'data> {
    type Iter: ParallelIterator<Item = Option<i32>>;

    fn par_iter(&'data self) -> Self::Iter;
}

impl<'data> Time32SecondArrayRefParallelIterator<'data> for Time32SecondArray {
    type Iter = ParallelTime32SecondArrayRef<'data>;

    fn par_iter(&'data self) -> Self::Iter {
        ParallelTime32SecondArrayRef::new(self)
    }
}

pub trait Time32MillisecondArrayRefParallelIterator<'data> {
    type Iter: ParallelIterator<Item = Option<i32>>;

    fn par_iter(&'data self) -> Self::Iter;
}

impl<'data> Time32MillisecondArrayRefParallelIterator<'data> for Time32MillisecondArray {
    type Iter = ParallelTime32MillisecondArrayRef<'data>;

    fn par_iter(&'data self) -> Self::Iter {
        ParallelTime32MillisecondArrayRef::new(self)
    }
}

pub trait Time64MicrosecondArrayRefParallelIterator<'data> {
    type Iter: ParallelIterator<Item = Option<i64>>;

    fn par_iter(&'data self) -> Self::Iter;
}

impl<'data> Time64MicrosecondArrayRefParallelIterator<'data> for Time64MicrosecondArray {
    type Iter = ParallelTime64MicrosecondArrayRef<'data>;

    fn par_iter(&'data self) -> Self::Iter {
        ParallelTime64MicrosecondArrayRef::new(self)
    }
}

pub trait Time64NanosecondArrayRefParallelIterator<'data> {
    type Iter: ParallelIterator<Item = Option<i64>>;

    fn par_iter(&'data self) -> Self::Iter;
}

impl<'data> Time64NanosecondArrayRefParallelIterator<'data> for Time64NanosecondArray {
    type Iter = ParallelTime64NanosecondArrayRef<'data>;

    fn par_iter(&'data self) -> Self::Iter {
        ParallelTime64NanosecondArrayRef::new(self)
    }
}
