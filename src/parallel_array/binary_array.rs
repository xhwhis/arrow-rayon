use arrow_array::types::GenericBinaryType;
use arrow_array::{BinaryArray, LargeBinaryArray};
use rayon::iter::ParallelIterator;

use super::byte_array::{ParallelGenericByteArray, ParallelGenericByteArrayRef};

pub type ParallelGenericBinaryArray<OffsetSize> =
    ParallelGenericByteArray<GenericBinaryType<OffsetSize>>;
pub type ParallelGenericBinaryArrayRef<'data, OffsetSize> =
    ParallelGenericByteArrayRef<'data, GenericBinaryType<OffsetSize>>;

pub type ParallelBinaryArray = ParallelGenericBinaryArray<i32>;
pub type ParallelBinaryArrayRef<'data> = ParallelGenericBinaryArrayRef<'data, i32>;
pub type ParallelLargeBinaryArray = ParallelGenericBinaryArray<i64>;
pub type ParallelLargeBinaryArrayRef<'data> = ParallelGenericBinaryArrayRef<'data, i64>;

pub trait BinaryArrayRefParallelIterator<'data> {
    type Iter: ParallelIterator<Item = Option<&'data [u8]>>;

    fn par_iter(&'data self) -> Self::Iter;
}

impl<'data> BinaryArrayRefParallelIterator<'data> for BinaryArray {
    type Iter = ParallelBinaryArrayRef<'data>;

    fn par_iter(&'data self) -> Self::Iter {
        ParallelBinaryArrayRef::new(self)
    }
}

pub trait LargeBinaryArrayRefParallelIterator<'data> {
    type Iter: ParallelIterator<Item = Option<&'data [u8]>>;

    fn par_iter(&'data self) -> Self::Iter;
}

impl<'data> LargeBinaryArrayRefParallelIterator<'data> for LargeBinaryArray {
    type Iter = ParallelLargeBinaryArrayRef<'data>;

    fn par_iter(&'data self) -> Self::Iter {
        ParallelLargeBinaryArrayRef::new(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_par_iter() {
        let array =
            BinaryArray::from_opt_vec(vec![Some(b"one"), None, Some(b"two"), Some(b"three")]);
        let items: Vec<String> = array
            .par_iter()
            .map(|item| {
                item.map_or_else(String::new, |item| {
                    String::from_utf8_lossy(item).to_string()
                })
            })
            .collect();
        assert_eq!(
            items,
            vec![
                "one".to_owned(),
                "".to_owned(),
                "two".to_owned(),
                "three".to_owned()
            ]
        );
    }

    #[test]
    fn test_collect_array() {
        let array =
            BinaryArray::from_opt_vec(vec![Some(b"one"), None, Some(b"two"), Some(b"three")]);
        let collected_array: ParallelBinaryArray = array
            .par_iter()
            .map(|item| item.map_or_else(Vec::new, |item| item.to_vec()))
            .collect();
        let binary_array = collected_array.into_inner();
        assert_eq!(binary_array.value(0), b"one");
        assert_eq!(binary_array.value(1), b"");
        assert_eq!(binary_array.value(2), b"two");
        assert_eq!(binary_array.value(3), b"three");
    }
}
