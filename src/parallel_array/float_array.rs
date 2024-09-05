use arrow_array::types::{Float16Type, Float32Type, Float64Type};

use super::primitive_array::{ParallelPrimitiveArray, ParallelPrimitiveArrayRef};

pub type ParallelFloat16Array = ParallelPrimitiveArray<Float16Type>;
pub type ParallelFloat16ArrayRef<'data> = ParallelPrimitiveArrayRef<'data, Float16Type>;
pub type ParallelFloat32Array = ParallelPrimitiveArray<Float32Type>;
pub type ParallelFloat32ArrayRef<'data> = ParallelPrimitiveArrayRef<'data, Float32Type>;
pub type ParallelFloat64Array = ParallelPrimitiveArray<Float64Type>;
pub type ParallelFloat64ArrayRef<'data> = ParallelPrimitiveArrayRef<'data, Float64Type>;
