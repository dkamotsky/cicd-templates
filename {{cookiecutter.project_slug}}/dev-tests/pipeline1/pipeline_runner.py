from overrides import overrides
from typing import Any, Mapping
from pyspark.sql import SparkSession, DataFrame
from {{cookiecutter.project_slug}}.test_shared import Harness

class MyHarness(Harness):

    @overrides
    def _train(self, spark: SparkSession, conf: Mapping[str, Any]) -> str:
        raise NotImplementedError

    @overrides
    def _eval(self, spark: SparkSession, experiment_data: DataFrame, conf: Mapping[str, Any]) -> str:
        raise NotImplementedError

MyHarness()()
