import pytest
import numpy as np
from oktest import ok
from itertools import cycle


DS_LEN: int = 131
DS_LIMIT: int = 1000
DS_READ_BATCH: int = 11
DS_WRITE_BATCH: int = 7


@pytest.fixture
def dataset():
    import tensorflow as tf
    iterator = cycle(range(DS_LEN))
    def _gen():
        yield from iterator
    return tf.data.Dataset.from_generator(_gen, tf.int32, tf.TensorShape([])).batch(DS_READ_BATCH)


def test_wrapper(dataset):
    import tensorflow as tf
    from {{cookiecutter.project_slug}}.data import WrappedTensorflowDataHolder
    wrapper = WrappedTensorflowDataHolder(dataset, DS_LEN, DS_LIMIT, DS_WRITE_BATCH, False)
    wrapper.oneshot = True
    with wrapper as (ds, one_epoch_in_batches):
        ok(one_epoch_in_batches) == DS_LEN // DS_WRITE_BATCH
        ok((next(ds.take(1).as_numpy_iterator()) == np.array(range(DS_WRITE_BATCH), np.int32)).all()).is_truthy()
        ok(ds.reduce(0, lambda x, _: x + 1).numpy().item()) == DS_LIMIT // DS_WRITE_BATCH
    print("\nSUCCESS")
