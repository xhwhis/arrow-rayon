use std::any::Any;
use std::sync::Arc;

use arrow_array::{Array, ArrayRef, ArrowPrimitiveType, NativeAdapter, PrimitiveArray};
use arrow_buffer::NullBuffer;
use arrow_data::ArrayData;
use arrow_schema::DataType;
use rayon::iter::plumbing::{bridge, Consumer, ProducerCallback, UnindexedConsumer};
use rayon::iter::{
    FromParallelIterator, IndexedParallelIterator, IntoParallelIterator, IntoParallelRefIterator,
    ParallelIterator,
};

#[derive(Clone)]
pub struct ParallelPrimitiveArray<T: ArrowPrimitiveType> {
    inner: PrimitiveArray<T>,
}

impl<T: ArrowPrimitiveType> ParallelPrimitiveArray<T> {
    pub fn new(inner: PrimitiveArray<T>) -> Self {
        Self { inner }
    }

    pub fn into_inner(self) -> PrimitiveArray<T> {
        self.inner
    }
}

impl<T: ArrowPrimitiveType> From<PrimitiveArray<T>> for ParallelPrimitiveArray<T> {
    fn from(array: PrimitiveArray<T>) -> Self {
        Self::new(array)
    }
}

impl<T: ArrowPrimitiveType> std::fmt::Debug for ParallelPrimitiveArray<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        self.inner.fmt(f)
    }
}

impl<T: ArrowPrimitiveType> Array for ParallelPrimitiveArray<T> {
    fn as_any(&self) -> &dyn Any {
        self.inner.as_any()
    }

    fn to_data(&self) -> ArrayData {
        self.inner.to_data()
    }

    fn into_data(self) -> ArrayData {
        self.inner.into_data()
    }

    fn data_type(&self) -> &DataType {
        self.inner.data_type()
    }

    fn slice(&self, offset: usize, length: usize) -> ArrayRef {
        Arc::new(self.inner.slice(offset, length))
    }

    fn len(&self) -> usize {
        self.inner.len()
    }

    fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    fn offset(&self) -> usize {
        self.inner.offset()
    }

    fn nulls(&self) -> Option<&NullBuffer> {
        self.inner.nulls()
    }

    fn get_buffer_memory_size(&self) -> usize {
        self.inner.get_buffer_memory_size()
    }

    fn get_array_memory_size(&self) -> usize {
        self.inner.get_array_memory_size()
    }
}

impl<T: ArrowPrimitiveType> IntoParallelIterator for ParallelPrimitiveArray<T> {
    type Item = Option<T::Native>;
    type Iter = ParallelPrimitiveArrayIter<T>;

    fn into_par_iter(self) -> Self::Iter {
        ParallelPrimitiveArrayIter::new(self.inner)
    }
}

#[derive(Debug, Clone)]
pub struct ParallelPrimitiveArrayIter<T: ArrowPrimitiveType> {
    inner: PrimitiveArray<T>,
}

impl<T: ArrowPrimitiveType> ParallelPrimitiveArrayIter<T> {
    fn new(inner: PrimitiveArray<T>) -> Self {
        Self { inner }
    }
}

impl<T: ArrowPrimitiveType> ParallelIterator for ParallelPrimitiveArrayIter<T> {
    type Item = Option<T::Native>;

    fn drive_unindexed<C>(self, consumer: C) -> C::Result
    where
        C: UnindexedConsumer<Self::Item>,
    {
        bridge(self, consumer)
    }

    fn opt_len(&self) -> Option<usize> {
        Some(self.inner.len())
    }
}

impl<T: ArrowPrimitiveType> IndexedParallelIterator for ParallelPrimitiveArrayIter<T> {
    fn len(&self) -> usize {
        self.inner.len()
    }

    fn drive<C: Consumer<Self::Item>>(self, consumer: C) -> C::Result {
        (0..self.inner.len())
            .into_par_iter()
            .map(|i| {
                if self.inner.is_null(i) {
                    None
                } else {
                    Some(unsafe { self.inner.value_unchecked(i) })
                }
            })
            .drive(consumer)
    }

    fn with_producer<CB: ProducerCallback<Self::Item>>(self, callback: CB) -> CB::Output {
        (0..self.inner.len())
            .into_par_iter()
            .map(|i| {
                if self.inner.is_null(i) {
                    None
                } else {
                    Some(unsafe { self.inner.value_unchecked(i) })
                }
            })
            .with_producer(callback)
    }
}

impl<T: ArrowPrimitiveType, Ptr: Into<NativeAdapter<T>> + Send> FromParallelIterator<Ptr>
    for ParallelPrimitiveArray<T>
{
    fn from_par_iter<I>(par_iter: I) -> Self
    where
        I: IntoParallelIterator<Item = Ptr>,
    {
        // HACK
        let vec = Vec::<Ptr>::from_par_iter(par_iter);
        let iter = vec.into_iter();

        Self::new(PrimitiveArray::from_iter(iter))
    }
}

#[derive(Debug, Clone)]
pub struct ParallelPrimitiveArrayRef<'data, T: ArrowPrimitiveType> {
    inner: &'data PrimitiveArray<T>,
}

impl<'data, T: ArrowPrimitiveType> ParallelPrimitiveArrayRef<'data, T> {
    pub fn new(inner: &'data PrimitiveArray<T>) -> Self {
        Self { inner }
    }
}

impl<'data, T: ArrowPrimitiveType> From<&'data PrimitiveArray<T>>
    for ParallelPrimitiveArrayRef<'data, T>
{
    fn from(array: &'data PrimitiveArray<T>) -> Self {
        Self::new(array)
    }
}

impl<T: ArrowPrimitiveType> ParallelIterator for ParallelPrimitiveArrayRef<'_, T> {
    type Item = Option<T::Native>;

    fn drive_unindexed<C>(self, consumer: C) -> C::Result
    where
        C: UnindexedConsumer<Self::Item>,
    {
        bridge(self, consumer)
    }

    fn opt_len(&self) -> Option<usize> {
        Some(self.inner.len())
    }
}

impl<T: ArrowPrimitiveType> IndexedParallelIterator for ParallelPrimitiveArrayRef<'_, T> {
    fn len(&self) -> usize {
        self.inner.len()
    }

    fn drive<C: Consumer<Self::Item>>(self, consumer: C) -> C::Result {
        (0..self.inner.len())
            .into_par_iter()
            .map(|i| {
                if self.inner.is_null(i) {
                    None
                } else {
                    Some(unsafe { self.inner.value_unchecked(i) })
                }
            })
            .drive(consumer)
    }

    fn with_producer<CB: ProducerCallback<Self::Item>>(self, callback: CB) -> CB::Output {
        (0..self.inner.len())
            .into_par_iter()
            .map(|i| {
                if self.inner.is_null(i) {
                    None
                } else {
                    Some(unsafe { self.inner.value_unchecked(i) })
                }
            })
            .with_producer(callback)
    }
}

impl<'data, T: ArrowPrimitiveType> IntoParallelRefIterator<'data>
    for ParallelPrimitiveArrayRef<'data, T>
{
    type Item = Option<T::Native>;
    type Iter = ParallelPrimitiveArrayRef<'data, T>;

    fn par_iter(&'data self) -> Self::Iter {
        ParallelPrimitiveArrayRef::new(self.inner)
    }
}

pub trait PrimitiveArrayRefParallelIterator<'data, T: ArrowPrimitiveType> {
    type Iter: ParallelIterator<Item = Option<T::Native>>;

    fn par_iter(&'data self) -> Self::Iter;
}

impl<'data, T: ArrowPrimitiveType> PrimitiveArrayRefParallelIterator<'data, T>
    for PrimitiveArray<T>
{
    type Iter = ParallelPrimitiveArrayRef<'data, T>;

    fn par_iter(&'data self) -> Self::Iter {
        ParallelPrimitiveArrayRef::new(self)
    }
}
