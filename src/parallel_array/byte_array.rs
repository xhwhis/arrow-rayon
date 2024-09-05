use std::any::Any;
use std::sync::Arc;

use arrow_array::types::ByteArrayType;
use arrow_array::{Array, ArrayRef, GenericByteArray};
use arrow_buffer::NullBuffer;
use arrow_data::ArrayData;
use arrow_schema::DataType;
use rayon::iter::plumbing::{bridge, Consumer, ProducerCallback, UnindexedConsumer};
use rayon::iter::{
    FromParallelIterator, IndexedParallelIterator, IntoParallelIterator, IntoParallelRefIterator,
    ParallelIterator,
};

#[derive(Clone)]
pub struct ParallelGenericByteArray<T: ByteArrayType> {
    inner: GenericByteArray<T>,
}

impl<T: ByteArrayType> ParallelGenericByteArray<T> {
    fn new(inner: GenericByteArray<T>) -> Self {
        Self { inner }
    }

    pub fn into_inner(self) -> GenericByteArray<T> {
        self.inner
    }
}

impl<T: ByteArrayType> From<GenericByteArray<T>> for ParallelGenericByteArray<T> {
    fn from(array: GenericByteArray<T>) -> Self {
        Self::new(array)
    }
}

impl<T: ByteArrayType> std::fmt::Debug for ParallelGenericByteArray<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        self.inner.fmt(f)
    }
}

impl<T: ByteArrayType> Array for ParallelGenericByteArray<T> {
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

impl<T: ByteArrayType, Ptr: AsRef<T::Native> + Send> FromParallelIterator<Ptr>
    for ParallelGenericByteArray<T>
{
    fn from_par_iter<I>(par_iter: I) -> Self
    where
        I: IntoParallelIterator<Item = Ptr>,
    {
        // HACK
        let vec = Vec::<Ptr>::from_par_iter(par_iter);
        let iter = vec.into_iter();

        Self::new(GenericByteArray::from_iter_values(iter))
    }
}

#[derive(Debug, Clone)]
pub struct ParallelGenericByteArrayRef<'data, T: ByteArrayType> {
    inner: &'data GenericByteArray<T>,
}

impl<'data, T: ByteArrayType> ParallelGenericByteArrayRef<'data, T> {
    pub fn new(inner: &'data GenericByteArray<T>) -> Self {
        Self { inner }
    }
}

impl<'data, T: ByteArrayType> From<&'data GenericByteArray<T>>
    for ParallelGenericByteArrayRef<'data, T>
{
    fn from(array: &'data GenericByteArray<T>) -> Self {
        Self::new(array)
    }
}

impl<'data, T: ByteArrayType> ParallelIterator for ParallelGenericByteArrayRef<'data, T> {
    type Item = Option<&'data T::Native>;

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

impl<T: ByteArrayType> IndexedParallelIterator for ParallelGenericByteArrayRef<'_, T> {
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

impl<'data, T: ByteArrayType> IntoParallelRefIterator<'data>
    for ParallelGenericByteArrayRef<'data, T>
{
    type Item = Option<&'data T::Native>;
    type Iter = ParallelGenericByteArrayRef<'data, T>;

    fn par_iter(&'data self) -> Self::Iter {
        ParallelGenericByteArrayRef::new(self.inner)
    }
}

pub trait GenericByteArrayRefParallelIterator<'data, T: ByteArrayType> {
    type Iter: ParallelIterator<Item = Option<&'data T::Native>>;

    fn par_iter(&'data self) -> Self::Iter;
}

impl<'data, T: ByteArrayType> GenericByteArrayRefParallelIterator<'data, T>
    for GenericByteArray<T>
{
    type Iter = ParallelGenericByteArrayRef<'data, T>;

    fn par_iter(&'data self) -> Self::Iter {
        ParallelGenericByteArrayRef::new(self)
    }
}
