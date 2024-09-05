use arrow_array::types::{
    TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
    TimestampSecondType,
};

use super::primitive_array::{ParallelPrimitiveArray, ParallelPrimitiveArrayRef};

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
