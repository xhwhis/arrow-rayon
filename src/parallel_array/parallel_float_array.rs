use arrow_array::types::{Float16Type, Float32Type, Float64Type};
use arrow_array::{Float16Array, Float32Array, Float64Array};
use half::f16;
use rayon::iter::ParallelIterator;

use crate::parallel_primitive_array::{ParallelPrimitiveArray, ParallelPrimitiveArrayRef};

pub type ParallelFloat16Array = ParallelPrimitiveArray<Float16Type>;
pub type ParallelFloat16ArrayRef<'data> = ParallelPrimitiveArrayRef<'data, Float16Type>;
pub type ParallelFloat32Array = ParallelPrimitiveArray<Float32Type>;
pub type ParallelFloat32ArrayRef<'data> = ParallelPrimitiveArrayRef<'data, Float32Type>;
pub type ParallelFloat64Array = ParallelPrimitiveArray<Float64Type>;
pub type ParallelFloat64ArrayRef<'data> = ParallelPrimitiveArrayRef<'data, Float64Type>;

pub trait Float16ArrayRefParallelIterator<'data> {
    type Iter: ParallelIterator<Item = Option<f16>>;

    fn par_iter(&'data self) -> Self::Iter;
}

impl<'data> Float16ArrayRefParallelIterator<'data> for Float16Array {
    type Iter = ParallelFloat16ArrayRef<'data>;

    fn par_iter(&'data self) -> Self::Iter {
        ParallelFloat16ArrayRef::new(self)
    }
}

pub trait Float32ArrayRefParallelIterator<'data> {
    type Iter: ParallelIterator<Item = Option<f32>>;

    fn par_iter(&'data self) -> Self::Iter;
}

impl<'data> Float32ArrayRefParallelIterator<'data> for Float32Array {
    type Iter = ParallelFloat32ArrayRef<'data>;

    fn par_iter(&'data self) -> Self::Iter {
        ParallelFloat32ArrayRef::new(self)
    }
}

pub trait Float64ArrayRefParallelIterator<'data> {
    type Iter: ParallelIterator<Item = Option<f64>>;

    fn par_iter(&'data self) -> Self::Iter;
}

impl<'data> Float64ArrayRefParallelIterator<'data> for Float64Array {
    type Iter = ParallelFloat64ArrayRef<'data>;

    fn par_iter(&'data self) -> Self::Iter {
        ParallelFloat64ArrayRef::new(self)
    }
}
