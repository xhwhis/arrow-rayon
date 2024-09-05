use arrow_array::types::{IntervalDayTimeType, IntervalMonthDayNanoType, IntervalYearMonthType};

use super::primitive_array::{ParallelPrimitiveArray, ParallelPrimitiveArrayRef};

pub type ParallelIntervalYearMonthArray = ParallelPrimitiveArray<IntervalYearMonthType>;
pub type ParallelIntervalYearMonthArrayRef<'data> =
    ParallelPrimitiveArrayRef<'data, IntervalYearMonthType>;
pub type ParallelIntervalDayTimeArray = ParallelPrimitiveArray<IntervalDayTimeType>;
pub type ParallelIntervalDayTimeArrayRef<'data> =
    ParallelPrimitiveArrayRef<'data, IntervalDayTimeType>;
pub type ParallelIntervalMonthDayNanoArray = ParallelPrimitiveArray<IntervalMonthDayNanoType>;
pub type ParallelIntervalMonthDayNanoArrayRef<'data> =
    ParallelPrimitiveArrayRef<'data, IntervalMonthDayNanoType>;
