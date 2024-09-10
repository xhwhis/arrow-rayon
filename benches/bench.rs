use arrow_array::BinaryArray;
use arrow_rayon::parallel_binary_array::BinaryArrayRefParallelIterator;
use arrow_rayon::parallel_float_array::ParallelFloat64Array;
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use geos::Geom;
use rand::{thread_rng, Rng};
use rayon::iter::{IndexedParallelIterator, ParallelIterator};

fn gen_wkb_array(len: usize) -> BinaryArray {
    let mut rng = thread_rng();
    BinaryArray::from_iter_values((0..len).map(|_| {
        let (x, y) = rng.gen::<(f64, f64)>();
        geos::Geometry::new_from_wkt(&format!("POINT ({x} {y})"))
            .unwrap()
            .to_wkb()
            .unwrap()
    }))
}

fn criterion_benchmark(c: &mut Criterion) {
    for len in [1_000, 10_000, 100_000] {
        let wkb_arr1 = gen_wkb_array(len);
        let wkb_arr2 = gen_wkb_array(len);

        c.bench_function(&format!("wkb array value distance(len = {len})"), |b| {
            b.iter(|| {
                let res: ParallelFloat64Array = wkb_arr1
                    .par_iter()
                    .zip(wkb_arr2.par_iter())
                    .map(|(wkb1, wkb2)| {
                        let geom1 = geos::Geometry::new_from_wkb(wkb1.unwrap()).unwrap();
                        let geom2 = geos::Geometry::new_from_wkb(wkb2.unwrap()).unwrap();
                        geom1.distance(&geom2).ok()
                    })
                    .collect();
                black_box(res);
            });
        });
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
