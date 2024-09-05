use arrow_array::types::{
    TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
    TimestampSecondType,
};
use arrow_array::{
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray,
};
use rayon::iter::ParallelIterator;

use crate::parallel_primitive_array::{ParallelPrimitiveArray, ParallelPrimitiveArrayRef};

pub type ParallelTimestampSecondArray = ParallelPrimitiveArray<TimestampSecondType>;
pub type ParallelTimestampSecondArrayRef<'data> =
    ParallelPrimitiveArrayRef<'data, TimestampSecondType>;
pub type ParallelTimestampMillisecondArray = ParallelPrimitiveArray<TimestampMillisecondType>;
pub type ParallelTimestampMillisecondArrayRef<'data> =
    ParallelPrimitiveArrayRef<'data, TimestampMillisecondType>;
pub type ParallelTimestampMicrosecondArray = ParallelPrimitiveArray<TimestampMicrosecondType>;
pub type ParallelTimestampMicrosecondArrayRef<'data> =
    ParallelPrimitiveArrayRef<'data, TimestampMicrosecondType>;
pub type ParallelTimestampNanosecondArray = ParallelPrimitiveArray<TimestampNanosecondType>;
pub type ParallelTimestampNanosecondArrayRef<'data> =
    ParallelPrimitiveArrayRef<'data, TimestampNanosecondType>;

pub trait TimestampSecondArrayRefParallelIterator<'data> {
    type Iter: ParallelIterator<Item = Option<i64>>;

    fn par_iter(&'data self) -> Self::Iter;
}

impl<'data> TimestampSecondArrayRefParallelIterator<'data> for TimestampSecondArray {
    type Iter = ParallelTimestampSecondArrayRef<'data>;

    fn par_iter(&'data self) -> Self::Iter {
        ParallelTimestampSecondArrayRef::new(self)
    }
}

pub trait TimestampMillisecondArrayRefParallelIterator<'data> {
    type Iter: ParallelIterator<Item = Option<i64>>;

    fn par_iter(&'data self) -> Self::Iter;
}

impl<'data> TimestampMillisecondArrayRefParallelIterator<'data> for TimestampMillisecondArray {
    type Iter = ParallelTimestampMillisecondArrayRef<'data>;

    fn par_iter(&'data self) -> Self::Iter {
        ParallelTimestampMillisecondArrayRef::new(self)
    }
}

pub trait TimestampMicrosecondArrayRefParallelIterator<'data> {
    type Iter: ParallelIterator<Item = Option<i64>>;

    fn par_iter(&'data self) -> Self::Iter;
}

impl<'data> TimestampMicrosecondArrayRefParallelIterator<'data> for TimestampMicrosecondArray {
    type Iter = ParallelTimestampMicrosecondArrayRef<'data>;

    fn par_iter(&'data self) -> Self::Iter {
        ParallelTimestampMicrosecondArrayRef::new(self)
    }
}

pub trait TimestampNanosecondArrayRefParallelIterator<'data> {
    type Iter: ParallelIterator<Item = Option<i64>>;

    fn par_iter(&'data self) -> Self::Iter;
}

impl<'data> TimestampNanosecondArrayRefParallelIterator<'data> for TimestampNanosecondArray {
    type Iter = ParallelTimestampNanosecondArrayRef<'data>;

    fn par_iter(&'data self) -> Self::Iter {
        ParallelTimestampNanosecondArrayRef::new(self)
    }
}
