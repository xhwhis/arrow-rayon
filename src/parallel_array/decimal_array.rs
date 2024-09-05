use arrow_array::types::{Decimal128Type, Decimal256Type};

use super::primitive_array::{ParallelPrimitiveArray, ParallelPrimitiveArrayRef};

pub type ParallelDecimal128Array = ParallelPrimitiveArray<Decimal128Type>;
pub type ParallelDecimal128ArrayRef<'data> = ParallelPrimitiveArrayRef<'data, Decimal128Type>;
pub type ParallelDecimal256Array = ParallelPrimitiveArray<Decimal256Type>;
pub type ParallelDecimal256ArrayRef<'data> = ParallelPrimitiveArrayRef<'data, Decimal256Type>;
