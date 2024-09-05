use arrow_array::types::{IntervalDayTimeType, IntervalMonthDayNanoType, IntervalYearMonthType};
use arrow_array::{IntervalDayTimeArray, IntervalMonthDayNanoArray, IntervalYearMonthArray};
use arrow_buffer::{IntervalDayTime, IntervalMonthDayNano};
use rayon::iter::ParallelIterator;

use crate::parallel_primitive_array::{ParallelPrimitiveArray, ParallelPrimitiveArrayRef};

pub type ParallelIntervalYearMonthArray = ParallelPrimitiveArray<IntervalYearMonthType>;
pub type ParallelIntervalYearMonthArrayRef<'data> =
    ParallelPrimitiveArrayRef<'data, IntervalYearMonthType>;
pub type ParallelIntervalDayTimeArray = ParallelPrimitiveArray<IntervalDayTimeType>;
pub type ParallelIntervalDayTimeArrayRef<'data> =
    ParallelPrimitiveArrayRef<'data, IntervalDayTimeType>;
pub type ParallelIntervalMonthDayNanoArray = ParallelPrimitiveArray<IntervalMonthDayNanoType>;
pub type ParallelIntervalMonthDayNanoArrayRef<'data> =
    ParallelPrimitiveArrayRef<'data, IntervalMonthDayNanoType>;

pub trait IntervalYearMonthArrayRefParallelIterator<'data> {
    type Iter: ParallelIterator<Item = Option<i32>>;

    fn par_iter(&'data self) -> Self::Iter;
}

impl<'data> IntervalYearMonthArrayRefParallelIterator<'data> for IntervalYearMonthArray {
    type Iter = ParallelIntervalYearMonthArrayRef<'data>;

    fn par_iter(&'data self) -> Self::Iter {
        ParallelIntervalYearMonthArrayRef::new(self)
    }
}

pub trait IntervalDayTimeArrayRefParallelIterator<'data> {
    type Iter: ParallelIterator<Item = Option<IntervalDayTime>>;

    fn par_iter(&'data self) -> Self::Iter;
}

impl<'data> IntervalDayTimeArrayRefParallelIterator<'data> for IntervalDayTimeArray {
    type Iter = ParallelIntervalDayTimeArrayRef<'data>;

    fn par_iter(&'data self) -> Self::Iter {
        ParallelIntervalDayTimeArrayRef::new(self)
    }
}

pub trait IntervalMonthDayNanoArrayRefParallelIterator<'data> {
    type Iter: ParallelIterator<Item = Option<IntervalMonthDayNano>>;

    fn par_iter(&'data self) -> Self::Iter;
}

impl<'data> IntervalMonthDayNanoArrayRefParallelIterator<'data> for IntervalMonthDayNanoArray {
    type Iter = ParallelIntervalMonthDayNanoArrayRef<'data>;

    fn par_iter(&'data self) -> Self::Iter {
        ParallelIntervalMonthDayNanoArrayRef::new(self)
    }
}
