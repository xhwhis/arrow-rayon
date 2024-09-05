use arrow_array::types::{
    DurationMicrosecondType, DurationMillisecondType, DurationNanosecondType, DurationSecondType,
};

use super::primitive_array::{ParallelPrimitiveArray, ParallelPrimitiveArrayRef};

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
