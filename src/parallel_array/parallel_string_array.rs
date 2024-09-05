use arrow_array::types::GenericStringType;
use arrow_array::{LargeStringArray, StringArray};
use rayon::iter::ParallelIterator;

use crate::parallel_byte_array::{ParallelGenericByteArray, ParallelGenericByteArrayRef};

pub type ParallelGenericStringArray<OffsetSize> =
    ParallelGenericByteArray<GenericStringType<OffsetSize>>;
pub type ParallelGenericStringArrayRef<'data, OffsetSize> =
    ParallelGenericByteArrayRef<'data, GenericStringType<OffsetSize>>;

pub type ParallelStringArray = ParallelGenericStringArray<i32>;
pub type ParallelStringArrayRef<'data> = ParallelGenericStringArrayRef<'data, i32>;
pub type ParallelLargeStringArray = ParallelGenericStringArray<i64>;
pub type ParallelLargeStringArrayRef<'data> = ParallelGenericStringArrayRef<'data, i64>;

pub trait StringArrayRefParallelIterator<'data> {
    type Iter: ParallelIterator<Item = Option<&'data str>>;

    fn par_iter(&'data self) -> Self::Iter;
}

impl<'data> StringArrayRefParallelIterator<'data> for StringArray {
    type Iter = ParallelStringArrayRef<'data>;

    fn par_iter(&'data self) -> Self::Iter {
        ParallelStringArrayRef::new(self)
    }
}

pub trait LargeStringArrayRefParallelIterator<'data> {
    type Iter: ParallelIterator<Item = Option<&'data str>>;

    fn par_iter(&'data self) -> Self::Iter;
}

impl<'data> LargeStringArrayRefParallelIterator<'data> for LargeStringArray {
    type Iter = ParallelLargeStringArrayRef<'data>;

    fn par_iter(&'data self) -> Self::Iter {
        ParallelLargeStringArrayRef::new(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_par_iter() {
        let array = StringArray::from(vec![Some("one"), None, Some("two"), Some("three")]);
        let items: Vec<String> = array
            .par_iter()
            .map(|item| item.map_or_else(String::new, |item| item.to_uppercase()))
            .collect();
        assert_eq!(
            items,
            vec![
                "ONE".to_owned(),
                "".to_owned(),
                "TWO".to_owned(),
                "THREE".to_owned()
            ]
        );
    }

    #[test]
    fn test_collect_array() {
        let array = StringArray::from(vec![Some("one"), None, Some("two"), Some("three")]);
        let collected_array: ParallelStringArray = array
            .par_iter()
            .map(|item| item.map_or_else(String::new, |item| item.to_uppercase()))
            .collect();
        let string_array = collected_array.into_inner();
        assert_eq!(string_array.value(0), "ONE");
        assert_eq!(string_array.value(1), "");
        assert_eq!(string_array.value(2), "TWO");
        assert_eq!(string_array.value(3), "THREE");
    }
}
