use std::any::Any;
use std::borrow::Borrow;
use std::sync::Arc;

use arrow_array::{Array, ArrayRef, BooleanArray};
use arrow_buffer::NullBuffer;
use arrow_data::ArrayData;
use arrow_schema::DataType;
use rayon::iter::plumbing::{bridge, Consumer, ProducerCallback, UnindexedConsumer};
use rayon::iter::{
    FromParallelIterator, IndexedParallelIterator, IntoParallelIterator, IntoParallelRefIterator,
    ParallelIterator,
};

#[derive(Debug, Clone)]
pub struct ParallelBooleanArray {
    inner: BooleanArray,
}

impl ParallelBooleanArray {
    pub fn new(inner: BooleanArray) -> Self {
        Self { inner }
    }

    pub fn into_inner(self) -> BooleanArray {
        self.inner
    }
}

impl From<BooleanArray> for ParallelBooleanArray {
    fn from(array: BooleanArray) -> Self {
        Self::new(array)
    }
}

impl Array for ParallelBooleanArray {
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

impl IntoParallelIterator for ParallelBooleanArray {
    type Item = Option<bool>;
    type Iter = ParallelBooleanArrayIter;

    fn into_par_iter(self) -> Self::Iter {
        ParallelBooleanArrayIter::new(self.inner)
    }
}

#[derive(Debug, Clone)]
pub struct ParallelBooleanArrayIter {
    inner: BooleanArray,
}

impl ParallelBooleanArrayIter {
    fn new(inner: BooleanArray) -> Self {
        Self { inner }
    }
}

impl ParallelIterator for ParallelBooleanArrayIter {
    type Item = Option<bool>;

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

impl IndexedParallelIterator for ParallelBooleanArrayIter {
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

impl<Ptr: Borrow<Option<bool>> + Send> FromParallelIterator<Ptr> for ParallelBooleanArray {
    fn from_par_iter<I>(par_iter: I) -> Self
    where
        I: IntoParallelIterator<Item = Ptr>,
    {
        // HACK
        let vec = Vec::<Ptr>::from_par_iter(par_iter);
        let iter = vec.into_iter();

        Self::new(BooleanArray::from_iter(iter))
    }
}

pub trait BooleanArrayIntoParallelIterator {
    type Iter: IntoParallelIterator<Item = Option<bool>>;

    fn into_par_iter(self) -> Self::Iter;
}

impl BooleanArrayIntoParallelIterator for BooleanArray {
    type Iter = ParallelBooleanArray;

    fn into_par_iter(self) -> Self::Iter {
        ParallelBooleanArray::new(self)
    }
}

#[derive(Debug, Clone)]
pub struct ParallelBooleanArrayRef<'data> {
    inner: &'data BooleanArray,
}

impl<'data> ParallelBooleanArrayRef<'data> {
    pub fn new(inner: &'data BooleanArray) -> Self {
        Self { inner }
    }
}

impl<'data> From<&'data BooleanArray> for ParallelBooleanArrayRef<'data> {
    fn from(array: &'data BooleanArray) -> Self {
        Self::new(array)
    }
}

impl ParallelIterator for ParallelBooleanArrayRef<'_> {
    type Item = Option<bool>;

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

impl IndexedParallelIterator for ParallelBooleanArrayRef<'_> {
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

impl<'data> IntoParallelRefIterator<'data> for ParallelBooleanArrayRef<'data> {
    type Item = Option<bool>;
    type Iter = ParallelBooleanArrayRef<'data>;

    fn par_iter(&'data self) -> Self::Iter {
        ParallelBooleanArrayRef::new(self.inner)
    }
}

pub trait BooleanArrayRefParallelIterator<'data> {
    type Iter: ParallelIterator<Item = Option<bool>>;

    fn par_iter(&'data self) -> Self::Iter;
}

impl<'data> BooleanArrayRefParallelIterator<'data> for BooleanArray {
    type Iter = ParallelBooleanArrayRef<'data>;

    fn par_iter(&'data self) -> Self::Iter {
        ParallelBooleanArrayRef::new(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_par_iter() {
        let array = BooleanArray::from(vec![Some(true), None, Some(false)]);
        let items: Vec<bool> = array
            .par_iter()
            .map(|item| item.map_or(true, |item| !item))
            .collect();
        assert_eq!(items, vec![false, true, true]);
    }

    #[test]
    fn test_collect_array() {
        let array = BooleanArray::from(vec![Some(true), None, Some(false)]);
        let collected_array: ParallelBooleanArray = array
            .par_iter()
            .map(|item| item.map(|item| !item))
            .collect();
        let boolean_array = collected_array.into_inner();
        assert!(!boolean_array.value(0));
        assert!(boolean_array.is_null(1));
        assert!(boolean_array.value(2));
    }
}
