use arrow_array::types::{
    DurationMicrosecondType, DurationMillisecondType, DurationNanosecondType, DurationSecondType,
};
use arrow_array::{
    DurationMicrosecondArray, DurationMillisecondArray, DurationNanosecondArray,
    DurationSecondArray,
};
use rayon::iter::ParallelIterator;

use crate::parallel_primitive_array::{ParallelPrimitiveArray, ParallelPrimitiveArrayRef};

pub type ParallelDurationSecondArray = ParallelPrimitiveArray<DurationSecondType>;
pub type ParallelDurationSecondArrayRef<'data> =
    ParallelPrimitiveArrayRef<'data, DurationSecondType>;
pub type ParallelDurationMillisecondArray = ParallelPrimitiveArray<DurationMillisecondType>;
pub type ParallelDurationMillisecondArrayRef<'data> =
    ParallelPrimitiveArrayRef<'data, DurationMillisecondType>;
pub type ParallelDurationMicrosecondArray = ParallelPrimitiveArray<DurationMicrosecondType>;
pub type ParallelDurationMicrosecondArrayRef<'data> =
    ParallelPrimitiveArrayRef<'data, DurationMicrosecondType>;
pub type ParallelDurationNanosecondArray = ParallelPrimitiveArray<DurationNanosecondType>;
pub type ParallelDurationNanosecondArrayRef<'data> =
    ParallelPrimitiveArrayRef<'data, DurationNanosecondType>;

pub trait DurationSecondArrayRefParallelIterator<'data> {
    type Iter: ParallelIterator<Item = Option<i64>>;

    fn par_iter(&'data self) -> Self::Iter;
}

impl<'data> DurationSecondArrayRefParallelIterator<'data> for DurationSecondArray {
    type Iter = ParallelDurationSecondArrayRef<'data>;

    fn par_iter(&'data self) -> Self::Iter {
        ParallelDurationSecondArrayRef::new(self)
    }
}

pub trait DurationMillisecondArrayRefParallelIterator<'data> {
    type Iter: ParallelIterator<Item = Option<i64>>;

    fn par_iter(&'data self) -> Self::Iter;
}

impl<'data> DurationMillisecondArrayRefParallelIterator<'data> for DurationMillisecondArray {
    type Iter = ParallelDurationMillisecondArrayRef<'data>;

    fn par_iter(&'data self) -> Self::Iter {
        ParallelDurationMillisecondArrayRef::new(self)
    }
}

pub trait DurationMicrosecondArrayRefParallelIterator<'data> {
    type Iter: ParallelIterator<Item = Option<i64>>;

    fn par_iter(&'data self) -> Self::Iter;
}

impl<'data> DurationMicrosecondArrayRefParallelIterator<'data> for DurationMicrosecondArray {
    type Iter = ParallelDurationMicrosecondArrayRef<'data>;

    fn par_iter(&'data self) -> Self::Iter {
        ParallelDurationMicrosecondArrayRef::new(self)
    }
}

pub trait DurationNanosecondArrayRefParallelIterator<'data> {
    type Iter: ParallelIterator<Item = Option<i64>>;

    fn par_iter(&'data self) -> Self::Iter;
}

impl<'data> DurationNanosecondArrayRefParallelIterator<'data> for DurationNanosecondArray {
    type Iter = ParallelDurationNanosecondArrayRef<'data>;

    fn par_iter(&'data self) -> Self::Iter {
        ParallelDurationNanosecondArrayRef::new(self)
    }
}
