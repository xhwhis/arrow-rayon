use arrow_array::types::{Date32Type, Date64Type};

use super::primitive_array::{ParallelPrimitiveArray, ParallelPrimitiveArrayRef};

pub type ParallelDate32Array = ParallelPrimitiveArray<Date32Type>;
pub type ParallelDate32ArrayRef<'data> = ParallelPrimitiveArrayRef<'data, Date32Type>;
pub type ParallelDate64Array = ParallelPrimitiveArray<Date64Type>;
pub type ParallelDate64ArrayRef<'data> = ParallelPrimitiveArrayRef<'data, Date64Type>;
