use std::borrow::Borrow;

use arrow_array::{Array, BooleanArray};
use arrow_buffer::{bit_util, MutableBuffer};
use arrow_data::ArrayData;
use arrow_schema::DataType;
use rayon::iter::plumbing::{Consumer, ProducerCallback, UnindexedConsumer};
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
}

impl From<BooleanArray> for ParallelBooleanArray {
    fn from(array: BooleanArray) -> Self {
        Self::new(array)
    }
}

impl ParallelIterator for ParallelBooleanArray {
    type Item = Option<bool>;

    fn drive_unindexed<C>(self, consumer: C) -> C::Result
    where
        C: UnindexedConsumer<Self::Item>,
    {
        (0..self.inner.len())
            .into_par_iter()
            .map(|i| {
                if self.inner.is_null(i) {
                    None
                } else {
                    Some(unsafe { self.inner.value_unchecked(i) })
                }
            })
            .drive_unindexed(consumer)
    }

    fn opt_len(&self) -> Option<usize> {
        Some(self.inner.len())
    }
}

impl IndexedParallelIterator for ParallelBooleanArray {
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
        let par_iter = par_iter.into_par_iter();
        let data_len = par_iter.opt_len().expect("parallel iterator must be sized");

        let num_bytes = bit_util::ceil(data_len, 8);
        let mut null_builder = MutableBuffer::from_len_zeroed(num_bytes);
        let mut val_builder = MutableBuffer::from_len_zeroed(num_bytes);

        let data = val_builder.as_slice_mut();

        let null_slice = null_builder.as_slice_mut();
        // par_iter.enumerate().for_each(|(i, item)| {
        //     if let Some(a) = item.borrow() {
        //         bit_util::set_bit(null_slice, i);
        //         if *a {
        //             bit_util::set_bit(data, i);
        //         }
        //     }
        // });

        let data = unsafe {
            ArrayData::new_unchecked(
                DataType::Boolean,
                data_len,
                None,
                Some(null_builder.into()),
                0,
                vec![val_builder.into()],
                vec![],
            )
        };
        Self::new(BooleanArray::from(data))
    }
}

pub trait BooleanArrayParallelIterator {
    type Iter: ParallelIterator<Item = Option<bool>>;

    fn into_par_iter(self) -> Self::Iter;
}

impl BooleanArrayParallelIterator for BooleanArray {
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
        (0..self.inner.len())
            .into_par_iter()
            .map(|i| {
                if self.inner.is_null(i) {
                    None
                } else {
                    Some(unsafe { self.inner.value_unchecked(i) })
                }
            })
            .drive_unindexed(consumer)
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
        ParallelBooleanArrayRef::new(&self.inner)
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
    }
}
