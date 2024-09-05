use arrow_array::types::{Decimal128Type, Decimal256Type};
use arrow_array::{Decimal128Array, Decimal256Array};
use arrow_buffer::i256;
use rayon::iter::ParallelIterator;

use crate::parallel_primitive_array::{ParallelPrimitiveArray, ParallelPrimitiveArrayRef};

pub type ParallelDecimal128Array = ParallelPrimitiveArray<Decimal128Type>;
pub type ParallelDecimal128ArrayRef<'data> = ParallelPrimitiveArrayRef<'data, Decimal128Type>;
pub type ParallelDecimal256Array = ParallelPrimitiveArray<Decimal256Type>;
pub type ParallelDecimal256ArrayRef<'data> = ParallelPrimitiveArrayRef<'data, Decimal256Type>;

pub trait Decimal128ArrayRefParallelIterator<'data> {
    type Iter: ParallelIterator<Item = Option<i128>>;

    fn par_iter(&'data self) -> Self::Iter;
}

impl<'data> Decimal128ArrayRefParallelIterator<'data> for Decimal128Array {
    type Iter = ParallelDecimal128ArrayRef<'data>;

    fn par_iter(&'data self) -> Self::Iter {
        ParallelDecimal128ArrayRef::new(self)
    }
}

pub trait Decimal256ArrayRefParallelIterator<'data> {
    type Iter: ParallelIterator<Item = Option<i256>>;

    fn par_iter(&'data self) -> Self::Iter;
}

impl<'data> Decimal256ArrayRefParallelIterator<'data> for Decimal256Array {
    type Iter = ParallelDecimal256ArrayRef<'data>;

    fn par_iter(&'data self) -> Self::Iter {
        ParallelDecimal256ArrayRef::new(self)
    }
}
