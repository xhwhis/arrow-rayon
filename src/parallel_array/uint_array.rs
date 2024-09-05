use arrow_array::types::{UInt16Type, UInt32Type, UInt64Type, UInt8Type};

use super::primitive_array::{ParallelPrimitiveArray, ParallelPrimitiveArrayRef};

pub type ParallelUInt8Array = ParallelPrimitiveArray<UInt8Type>;
pub type ParallelUInt8ArrayRef<'data> = ParallelPrimitiveArrayRef<'data, UInt8Type>;
pub type ParallelUInt16Array = ParallelPrimitiveArray<UInt16Type>;
pub type ParallelUInt16ArrayRef<'data> = ParallelPrimitiveArrayRef<'data, UInt16Type>;
pub type ParallelUInt32Array = ParallelPrimitiveArray<UInt32Type>;
pub type ParallelUInt32ArrayRef<'data> = ParallelPrimitiveArrayRef<'data, UInt32Type>;
pub type ParallelUInt64Array = ParallelPrimitiveArray<UInt64Type>;
pub type ParallelUInt64ArrayRef<'data> = ParallelPrimitiveArrayRef<'data, UInt64Type>;
