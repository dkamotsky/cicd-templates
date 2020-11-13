try:

    import tensorflow as tf
    from overrides import overrides
    from abc import abstractmethod, ABC
    from typing import Tuple, Mapping, Any, Optional
    from contextlib import AbstractContextManager
    from pyspark.sql import SparkSession, DataFrame
    from petastorm.transform import TransformSpec
    from petastorm.spark import SparkDatasetConverter
    from petastorm.spark.spark_dataset_converter import TFDatasetContextManager, make_spark_converter


    class AbstractTensorflowDataHolder(AbstractContextManager):
        """
        Abstract context manager which wraps a TensorFlow Dataset,
        and returns it when entering context.
        """
        @abstractmethod
        def __enter__(self) -> Tuple[tf.data.Dataset, int]:
            """
                When this context manager's context is entered, returns
                a tuple of the wrapped Tensorflow dataset, and an int, indicating
                the number of batches per one epoch.

                :return: returns a tuple of the wrapped Tensorflow dataset, and an int of the number of batches per epoch
            """
            raise NotImplementedError

        def __exit__(self, *args, **kwargs) -> None:
            """
                When this context manager's context is exited, cleans up the state, associated with
                the wrapped Tensorflow dataset.

                :return: returns nothing
            """
            pass


    class TensorflowDataProvider(ABC):
        """
        Abstract data provider which extracts a Tensorflow dataset
        from a given Spark session.
        """
        def train_metadata(self, spark: SparkSession) -> Mapping[str, Any]:
            """
                Retrieves provenance metadata about the managed dataset.

                :param spark: Spark session owning the data set
                :return: returns dictionary of str keys and arbitrary values, describing the provenance of the managed data set.
                         This dict will be directly recorded as experiment params in MLflow.
            """
            return {}

        @abstractmethod
        def for_training(self, spark: SparkSession) -> Tuple[AbstractTensorflowDataHolder, AbstractTensorflowDataHolder]:
            """
                Retrieves training and validation datasets.

                :param spark: Spark session owning the data set
                :return: returns a tuple of Tensorflow data holders, the first for training set, the second for validation set.
                         Tensorflow data holders are Python context managers, which return the actual Tensorflow dataset when
                         their context is entered.
            """
            raise NotImplementedError

        @abstractmethod
        def for_evaluation(self, spark: SparkSession) -> AbstractTensorflowDataHolder:
            """
                Retrieves testing dataset.

                :param spark: Spark session owning the data set
                :return: returns a Tensorflow data holder wrapping the testing data set.
                         Tensorflow data holders are Python context managers, which return the actual Tensorflow dataset when
                         their context is entered.
            """
            raise NotImplementedError


    class WrappedTensorflowDataHolder(AbstractTensorflowDataHolder):
        """
        Concrete implementation of AbstractTensorflowDataHolder.
        Wraps a pre-existing Tensorflow dataset.
        """
        def __init__(self,
                     ds: tf.data.Dataset,
                     ds_len: int,
                     limit: Optional[int],
                     target_batch_size: int,
                     shuffle: bool) -> None:
            """
                Wraps the given Tensorflow dataset. Will unbatch it, and rebatch with the given target batch size.
                Will also limit to the given number of unbatched items, if the limit is provided.

                :param ds: Tensorflow dataset to wrap
                :param ds_len: int number of items in the underlying dataset; if the Tensorflow dataset is infinitely looping,
                               must provide the number of items in the original data used to loop over
                :param limit: if specified, use only up to this number of items from the given Tensorflow dataset, after unbatching
                :param target_batch_size: int batch size of the batches in the generated Tensorflow dataset; will discard
                                          those items from the original dataset which do not fit in a whole number of batches
                :param shuffle: bool flag to indicate that a shuffle is requested for each pass over the given Tensorflow dataset.
                                If the given dataset is infinitely looping, then shuffle will have effect only if limit is specified.
                :return: returns nothing
            """
            self.ds: tf.data.Dataset = ds
            self.ds_len: int = ds_len
            self.limit: Optional[int] = limit
            if self.limit:
                self.ds_len = min(self.limit, self.ds_len)
            self.target_batch_size: int = target_batch_size
            self.shuffle: bool = shuffle
            self.oneshot: bool = False

        @overrides
        def __enter__(self) -> Tuple[tf.data.Dataset, int]:
            """
                When this context manager's context is entered, returns
                a tuple of the wrapped Tensorflow dataset, and an int, indicating
                the number of batches per one epoch.

                :return: returns a tuple of the wrapped Tensorflow dataset, and an int of the number of batches per epoch
            """
            assert self.ds_len >= self.target_batch_size, f"Dataset length {self.ds_len} must be greater or equal to the target batch size {self.target_batch_size}."
            ds: tf.data.Dataset = self.ds.unbatch()
            if self.limit:
                ds = ds.take(self.limit)
            ds = ds.batch(self.target_batch_size, drop_remainder=True)
            if self.shuffle:
                ds = ds.shuffle(10, seed=111, reshuffle_each_iteration=True)
            return ds if self.oneshot else ds.repeat(), self.ds_len // self.target_batch_size


    class PetastormDataHolder(AbstractTensorflowDataHolder):
        """
        Concrete implementation of AbstractTensorflowDataHolder.
        Wraps a Spark DataFrame using Petastorm to generate the Tensorflow dataset.
        """
        def __init__(self,
                     df: DataFrame,
                     ts: TransformSpec,
                     read_batch_size: int,
                     target_batch_size: int,
                     shuffle: bool) -> None:
            """
                Wraps the given Spark dataframe. Will unbatch it, and rebatch with the given target batch size.
                Will also limit to the given number of unbatched items, if the limit is provided.

                :param ds: Tensorflow dataset to wrap
                :param ds_len: int number of items in the underlying dataset; if the Tensorflow dataset is infinitely looping,
                               must provide the number of items in the original data used to loop over
                :param limit: if specified, use only up to this number of items from the given Tensorflow dataset, after unbatching
                :param target_batch_size: int batch size of the batches in the generated Tensorflow dataset; will discard
                                          those items from the original dataset which do not fit in a whole number of batches
                :param shuffle: bool flag to indicate that a shuffle is requested for each pass over the given Tensorflow dataset.
                                If the given dataset is infinitely looping, then shuffle will have effect only if limit is specified.
                :return: returns nothing
            """
            self.df: DataFrame = df
            self.ts: TransformSpec = ts
            self.read_batch_size: int = read_batch_size
            self.target_batch_size: int = target_batch_size
            self.shuffle: bool = shuffle
            self.cnv: Optional[SparkDatasetConverter] = None
            self.ctx: Optional[TFDatasetContextManager] = None

        @overrides
        def __enter__(self) -> Tuple[tf.data.Dataset, int]:
            """
                When this context manager's context is entered, returns
                a tuple of the wrapped Tensorflow dataset, and an int, indicating
                the number of batches per one epoch.

                :return: returns a tuple of the wrapped Tensorflow dataset, and an int of the number of batches per epoch
            """
            self.cnv = make_spark_converter(self.df)
            assert len(self.cnv) >= self.target_batch_size, f"Dataset length {len(self.cnv)} must be greater or equal to the target batch size {self.target_batch_size}."
            self.ctx = self.cnv.make_tf_dataset(transform_spec=self.ts, batch_size=self.read_batch_size)
            ds: tf.data.Dataset = self.ctx.__enter__().map(tuple).unbatch().batch(self.target_batch_size, drop_remainder=True)
            if self.shuffle:
                ds = ds.shuffle(10, seed=111, reshuffle_each_iteration=True)
            return ds.repeat(), len(self.cnv) // self.target_batch_size

        @overrides
        def __exit__(self, *args, **kwargs) -> None:
            """
                Deletes Petastorm cache.

                :return: returns nothing
            """
            if self.ctx:
                self.ctx.__exit__(*args, **kwargs)
            if self.cnv:
                self.cnv.delete()

except ImportError as e:
    print(f"Detected import error: {e}. Tensorflow dataset providers will not be available.")
