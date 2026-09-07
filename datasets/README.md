# Datasets

Place ANN benchmark datasets (HDF5 format) in the `downloads/` subdirectory.

## Supported Datasets

- sift-128-euclidean.hdf5
- sift-256-euclidean.hdf5
- sift-512-euclidean.hdf5
- sift-768-euclidean.hdf5
- sift-960-euclidean.hdf5
- gist-960-euclidean.hdf5
- glove-25-angular.hdf5
- glove-50-angular.hdf5
- glove-100-angular.hdf5
- glove-200-angular.hdf5
- deep-image-96-angular.hdf5

## Download

```bash
cd downloads
wget http://ann-benchmarks.com/sift-128-euclidean.hdf5
wget http://ann-benchmarks.com/gist-960-euclidean.hdf5
wget http://ann-benchmarks.com/glove-100-angular.hdf5
```

## Format

Each HDF5 file contains:
- `/train` - Base vectors (float32)
- `/test` - Query vectors (float32)
- `/neighbors` - Ground truth neighbor indices (int32)
- `/distances` - Ground truth distances (float32, optional)