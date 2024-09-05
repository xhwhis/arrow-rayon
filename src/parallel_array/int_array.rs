use arrow_array::types::{Int16Type, Int32Type, Int64Type, Int8Type};
use arrow_array::Int8Array;
use rayon::iter::ParallelIterator;

use super::primitive_array::{ParallelPrimitiveArray, ParallelPrimitiveArrayRef};

pub type ParallelInt8Array = ParallelPrimitiveArray<Int8Type>;
pub type ParallelInt8ArrayRef<'data> = ParallelPrimitiveArrayRef<'data, Int8Type>;
pub type ParallelInt16Array = ParallelPrimitiveArray<Int16Type>;
pub type ParallelInt16ArrayRef<'data> = ParallelPrimitiveArrayRef<'data, Int16Type>;
pub type ParallelInt32Array = ParallelPrimitiveArray<Int32Type>;
pub type ParallelInt32ArrayRef<'data> = ParallelPrimitiveArrayRef<'data, Int32Type>;
pub type ParallelInt64Array = ParallelPrimitiveArray<Int64Type>;
pub type ParallelInt64ArrayRef<'data> = ParallelPrimitiveArrayRef<'data, Int64Type>;

pub trait Int8ArrayRefParallelIterator<'data> {
    type Iter: ParallelIterator<Item = Option<i8>>;

    fn par_iter(&'data self) -> Self::Iter;
}

impl<'data> Int8ArrayRefParallelIterator<'data> for Int8Array {
    type Iter = ParallelInt8ArrayRef<'data>;

    fn par_iter(&'data self) -> Self::Iter {
        ParallelInt8ArrayRef::new(self)
    }
}

#[cfg(test)]
mod tests {
    use rayon::iter::IndexedParallelIterator;

    use super::*;

    #[test]
    fn test_par_iter() {
        let array1 = Int8Array::from(vec![Some(1), None, Some(2), Some(3)]);
        let array2 = Int8Array::from(vec![Some(2), Some(4), None, Some(5)]);
        let items: Vec<i8> = array1
            .par_iter()
            .zip(array2.par_iter())
            .map(|opt| {
                match opt {
                    (Some(item1), Some(item2)) => item1 + item2,
                    (Some(item1), None) => item1,
                    (None, Some(item2)) => item2,
                    (None, None) => 0,
                }
            })
            .collect();
        assert_eq!(items, vec![3, 4, 2, 8]);
    }

    #[test]
    fn test_collect_array() {
        let array = Int8Array::from(vec![Some(1), None, Some(2), Some(3)]);
        let collected_array: ParallelInt8Array = array
            .par_iter()
            .map(|item| item.map_or(0, |item| item * 2))
            .collect();
        let int8_array = collected_array.into_inner();
        assert_eq!(int8_array.value(0), 2);
        assert_eq!(int8_array.value(1), 0);
        assert_eq!(int8_array.value(2), 4);
        assert_eq!(int8_array.value(3), 6);
    }
}
