use arrow_array::types::{
    Time32MillisecondType, Time32SecondType, Time64MicrosecondType, Time64NanosecondType,
};

use super::primitive_array::{ParallelPrimitiveArray, ParallelPrimitiveArrayRef};

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
