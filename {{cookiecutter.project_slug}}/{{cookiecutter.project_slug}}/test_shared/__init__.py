"""
test_shared module contains test code and test harnesses for functional and acceptance tests
"""

import os
import sys
from abc import ABC, abstractmethod
from typing import MutableMapping, Any, Optional, Mapping
from pyspark.sql import SparkSession, DataFrame
from mlflow.tracking import MlflowClient
from ..config import read_config, setup_mlflow, delta_jar_location

try:

    import tensorflow as tf
    assert tf.__version__ == '2.2.0', "Tensorflow 2.2.0 is required!"
    from glob import glob
    from pathlib import Path
    from urllib.parse import urlparse
    from petastorm.spark import SparkDatasetConverter
    import petastorm.spark.spark_dataset_converter as petastorm_internal

    # The following import resolves only with databricks-connect
    # pylint: disable=no-name-in-module, import-error
    from pyspark.dbutils import DBUtils


    def make_spark_converter(df,
                             parquet_row_group_size_bytes=petastorm_internal.DEFAULT_ROW_GROUP_SIZE_BYTES,
                             compression_codec=None,
                             dtype='float32'):
        """
           Copy-paste of the Petastorm source code, with modification as shown in the comment below.
        """
        parent_cache_dir_url = petastorm_internal._get_parent_cache_dir_url()

        # TODO: Improve default behavior to be automatically choosing the best way.
        compression_codec = compression_codec or "uncompressed"

        if compression_codec.lower() not in \
                ['uncompressed', 'bzip2', 'gzip', 'lz4', 'snappy', 'deflate']:
            raise RuntimeError(
                "compression_codec should be None or one of the following values: "
                "'uncompressed', 'bzip2', 'gzip', 'lz4', 'snappy', 'deflate'")

        dataset_cache_dir_url = petastorm_internal._cache_df_or_retrieve_cache_data_url(
            df, parent_cache_dir_url, parquet_row_group_size_bytes, compression_codec, dtype)

        spark = petastorm_internal._get_spark_session()

        dbutils = DBUtils(spark.sparkContext)
        dbutils.fs.cp(dataset_cache_dir_url.replace("file:///dbfs/", "dbfs:/"), dataset_cache_dir_url, True)

        spark_df = spark.read.parquet(dataset_cache_dir_url)
        dataset_size = spark_df.count()

        #parquet_file_url_list = list(spark_df._jdf.inputFiles())
        parquet_file_url_list = [Path(f).as_uri() for f in glob(urlparse(dataset_cache_dir_url).path+"/*.parquet")]

        petastorm_internal._check_dataset_file_median_size(parquet_file_url_list)

        return SparkDatasetConverter(dataset_cache_dir_url, parquet_file_url_list, dataset_size)


    def adapt_petastorm_to_databricks_connect() -> None:
        """
            Makes Petastorm work over Databricks Connect by pulling Petastorm-materialized dataset to the
            local volume using dbutils.

            :return: returns nothing
        """
        import petastorm
        assert petastorm.__version__ == '0.9.2', "Supports only petastorm==0.9.2"
        petastorm.spark.spark_dataset_converter.make_spark_converter = make_spark_converter


    def autopatch_petastorm(config_location: str, petastorm_cache: str, **kwargs) -> None:
        """
            Detects running in Databricks Connect environment by checking whether config
            location is on DBFS or on local filesystem. If config location is on the local
            filesystem, reconfigures Petastorm to pull materialized data set using dbutils.

            :param config_location: app configuration location
            :param petastorm_cache: file:// URL for the local petastorm cache location
            :return: returns nothing
        """
        try:
            config_scheme: str = urlparse(config_location).scheme
        except:
            config_scheme = ""
        if not config_scheme:
            config_scheme = "file"
        if config_scheme != "dbfs":
            print(f"Config location {config_location} is a {config_scheme} URL, not a DBFS URL. Monkey-patching Petastorm for local debugging.")
            adapt_petastorm_to_databricks_connect()
            petastorm_cache = urlparse(petastorm_cache).path
            if not os.path.isdir(petastorm_cache):
                os.makedirs(petastorm_cache, exist_ok=True)

except ImportError as e:

    print(f"Detected import error: {e}. Petastorm operations will not be available.")

    def autopatch_petastorm(config_location: str, petastorm_cache: str, **kwargs) -> None:
        pass


def autoinstall_delta_lake(spark: SparkSession, config_location: str, **kwargs) -> None:
    """
        Detects running in Databricks Connect environment by checking whether config
        location is on DBFS or on local filesystem. If config location is on the local
        filesystem, adds Delta Lake libraries to Python path from JAR.

        :param config_location: app configuration location
        :return: returns nothing
    """
    try:
        config_scheme: str = urlparse(config_location).scheme
    except:
        config_scheme = ""
    if not config_scheme:
        config_scheme = "file"
    if config_scheme != "dbfs":
        delta_jar: str = delta_jar_location()
        print(f"Config location {config_location} is a {config_scheme} URL, not a DBFS URL. Installing Delta Lake API from {delta_jar}.")
        spark.sparkContext.addPyFile(delta_jar)


class Harness(ABC):
    """
    Test Harness for training, evaluating, and promoting an ML model.
    """
    def __init__(self, root: Optional[str] = None) -> None:
        """
            Creates a test harness.

            :param root: location of the app configuration config.yaml, if not specified, will use argv[1]
        """
        self.root: str = root if root else sys.argv[1]

    @abstractmethod
    def _train(self, spark: SparkSession, conf: Mapping[str, Any]) -> str:
        """
            Trains an ML model.

            :param spark: Spark session managing traning and validation data set
            :param conf: app configuration
            :return: returns str MLflow run ID
        """
        raise NotImplementedError

    @abstractmethod
    def _eval(self, spark: SparkSession, experiment_data: DataFrame, conf: Mapping[str, Any]) -> str:
        """
            Evaluates and promotes an ML model.

            :param spark: Spark session managing testing data set
            :param experiment_data: Spark dataframe containing MLflow experiment metadata
            :param conf: app configuration
            :return: returns str model name in the MLflow Registry
        """
        raise NotImplementedError

    def __call__(self) -> None:
        """
            Trains, evaluates, and promotes an ML model, checks assertions.

            :return: returns nothing
        """
        print("Starting test run")

        conf: MutableMapping[str, Any] = read_config('config.yaml', self.root)
        conf['config_location'] = self.root

        autopatch_petastorm(**conf)

        app_name: str = conf['app_name']
        print(f"Test application name: {app_name}")

        spark: SparkSession = SparkSession.builder.appName(app_name).getOrCreate()

        autoinstall_delta_lake(spark, **conf)

        experiment_id: str = setup_mlflow(**conf)
        print(f"Context set to MLFlow experiment {experiment_id}.")

        run_id: str = self._train(spark, conf)

        spark_df: DataFrame = spark.read.format("mlflow-experiment").load(experiment_id)

        #print(f"MLFlow experiment {experiment_id} contents:")
        #print(spark_df) #this is SLOW on databricks-connect!!!

        assert spark_df.\
                   where(f"run_id='{run_id}'").\
                   where("tags.candidate='true'").\
                   where("status='FINISHED'").\
                   count() == 1, "Model training did not produce any candidates."

        model_name: str = self._eval(spark, spark_df, conf)

        assert spark_df.where("tags.candidate='true'").count() == 0, \
            "Model candidates still exist after evaluation."

        assert len(MlflowClient().get_latest_versions(model_name, stages=['Staging'])) > 0, \
            "Staging model does not exist after evaluation."

        print(f"Finished {app_name} test run.")
