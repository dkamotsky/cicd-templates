import io
import json
import yaml
import time
import os
import sys
from abc import ABC, abstractmethod
from overrides import final
from urllib.parse import urlparse
from argparse import ArgumentParser
from typing import MutableMapping, Mapping, Any, Optional, Union
from pyspark.sql import SparkSession

try:
    # pylint: disable=no-name-in-module, import-error
    import petastorm
    if petastorm.__version__ not in {'0.9.2', '0.9.5'}:
        del petastorm
except:
    pass


def locate_code() -> None:
    p: ArgumentParser = ArgumentParser()
    p.add_argument("--conf-file", required=True, type=str)
    namespace = p.parse_known_args()[0]
    basedir: str = os.path.normpath(os.path.dirname(namespace.conf_file.replace("dbfs:/", "/dbfs/")))
    while os.path.exists(os.path.join(basedir, "__init__.py")):
        basedir = os.path.normpath(os.path.join(basedir, ".."))
    sys.path.insert(0, basedir)
    print(f"Added common test code location {basedir} to PYTHONPATH")


# abstract class for jobs
class Job(ABC):

    def __init__(self, spark: Optional[SparkSession] = None, conf: Optional[str] = None) -> None:
        self.spark: SparkSession = SparkSession.builder.getOrCreate() if spark is None else spark
        self.logger = self._prepare_logger() #log4j logger, not Python!!!
        self.config_location: str = self._get_conf_file(conf)
        self.conf: MutableMapping[str, Any] = self._provide_config()
        self.configure()
        self.spark.sparkContext.setLogLevel(self.conf.get("spark_log_level", "WARN"))
        self.experiment_id: Optional[str] = self._setup_mlflow()
        self.autoinstall_delta_lake_api()
        self.autopatch_petastorm()
        self._log_conf()

    @staticmethod
    def __is_dbfs(conf: Optional[str]) -> bool:
        try:
            config_scheme: Union[str, bytes] = urlparse(conf).scheme
        except:
            config_scheme = ""
        return config_scheme == "dbfs"

    def _get_conf_file(self, conf: Optional[str] = None) -> str:
        if not conf:
            p = ArgumentParser()
            p.add_argument("--conf-file", required=True, type=str)
            namespace = p.parse_known_args()[0]
            conf = namespace.conf_file
        assert conf, "Configuration location must not be empty."
        return conf

    def _provide_config(self) -> MutableMapping[str, Any]:
        self.logger.info(f"Reading configuration from {self.config_location}")
        return self._read_config(self.spark, self.config_location)

    @staticmethod
    def _read_config(spark: SparkSession, conf: str) -> MutableMapping[str, Any]:
        config: MutableMapping[str, Any] = {}
        if Job.__is_dbfs(conf):
            raw_content: str = "\n".join(spark.read.format("text").load(conf).toPandas()["value"].tolist())
        else:
            with open(conf, "r") as f:
                raw_content = f.read()
        if conf.endswith(".json"):
            json_conf: Mapping[str, Any] = json.loads(raw_content)
            # Add conf from all files included in the given JSON
            for include_file in json_conf.get("includes", []):
                config.update(Job._read_config(spark, os.path.join(os.path.dirname(conf), include_file)))
            # Finally add conf from the given JSON itself
            config.update(json_conf)
        elif conf.endswith(".yml") or conf.endswith(".yaml"):
            with io.StringIO(raw_content) as cf:
                yaml_conf: Mapping[str, Any] = yaml.load(cf, Loader=yaml.FullLoader)
            # Add conf from all files included in the given YAML
            for include_file in yaml_conf.get("includes", []):
                config.update(Job._read_config(spark, os.path.join(os.path.dirname(conf), include_file)))
            # Finally add conf from the given YAML itself
            config.update(yaml_conf)
        else:
            raise ValueError(f"Unknown config file format: {conf}. Only .json and .yaml are supported.")
        return {k:v for k,v in config.items() if k!="includes"}

    def _prepare_logger(self):
        log4j_logger = self.spark._jvm.org.apache.log4j
        return log4j_logger.LogManager.getLogger(self.__class__.__name__)

    def _log_conf(self) -> None:
        # log parameters
        self.logger.info("Launching job with configuration parameters:")
        for key, item in self.conf.items():
            self.logger.info("\t Parameter: %-30s with value => %-30s" % (key, item))
        if "experiment_path" not in self.conf:
            self.logger.warn("\t Parameter: %-30s is missing. %-30s" % ('experiment_path', 'MLflow will not have experiment context.'))

    def _setup_mlflow(self) -> Optional[str]:
        import mlflow #place import here, so that unit tests don't generate unneeded local directories
        mlflow.set_tracking_uri("databricks")
        if "experiment_path" in self.conf:
            experiment_path: str = self.conf["experiment_path"]
            mlflow.set_experiment(experiment_path)
            try:
                experiment_id = mlflow.get_experiment_by_name(experiment_path).experiment_id
                return experiment_id
            except FileNotFoundError:
                time.sleep(10)
                experiment_id = mlflow.get_experiment_by_name(experiment_path).experiment_id
                return experiment_id
        else:
            return None

    @staticmethod
    def delta_jar_location() -> str:
        return os.path.normpath(os.path.join(os.path.dirname(__file__), "../../lib/delta-core_2.12-0.7.0.jar"))

    @final
    def autoinstall_delta_lake_api(self) -> None:
        """
            Detects running in Databricks Connect environment by checking whether config
            location is on DBFS or on local filesystem. If config location is on the local
            filesystem, adds Delta Lake libraries to Python path from JAR.

            :param config_location: app configuration location
            :return: returns nothing
        """
        try:
            from delta.tables import DeltaTable
        except ImportError:
            delta_jar: str = self.delta_jar_location()
            self.logger.warn(f"Delta API is not installed. Installing Delta Lake API from {delta_jar}.")
            assert os.path.exists(delta_jar), f"Delta JAR {delta_jar} does not exist."
            self.spark.sparkContext.addPyFile(delta_jar)

    def _adapt_petastorm_to_databricks_connect(self, petastorm_module, dbutils_constructor) -> None:
        """
            Makes Petastorm work over Databricks Connect by pulling Petastorm-materialized dataset to the
            local volume using dbutils.

            :return: returns nothing
        """
        from glob import glob
        from pathlib import Path
        # pylint: disable=no-name-in-module, import-error
        from petastorm.spark import SparkDatasetConverter
        # pylint: disable=no-name-in-module, import-error
        import petastorm.spark.spark_dataset_converter as petastorm_internal

        assert "petastorm_cache" in self.conf, \
            f"When Petastorm is installed, and Job operates in a Databricks Connect environment, 'petastorm_cache' application configuration property must be present."
        self.spark.conf.set(SparkDatasetConverter.PARENT_CACHE_DIR_URL_CONF, self.conf['petastorm_cache'])
        petastorm_cache = urlparse(self.conf['petastorm_cache']).path
        if not os.path.isdir(petastorm_cache):
            os.makedirs(petastorm_cache, exist_ok=True)

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

            dbutils = dbutils_constructor(spark.sparkContext)
            dbutils.fs.cp(dataset_cache_dir_url.replace("file:///dbfs/", "dbfs:/"), dataset_cache_dir_url, True)

            spark_df = spark.read.parquet(dataset_cache_dir_url)
            dataset_size = spark_df.count()

            # parquet_file_url_list = list(spark_df._jdf.inputFiles())
            parquet_file_url_list = [Path(f).as_uri() for f in
                                     glob(urlparse(dataset_cache_dir_url).path + "/*.parquet")]

            petastorm_internal._check_dataset_file_median_size(parquet_file_url_list)

            return SparkDatasetConverter(dataset_cache_dir_url, parquet_file_url_list, dataset_size)

        petastorm_module.spark.spark_dataset_converter.make_spark_converter = make_spark_converter

    @final
    def autopatch_petastorm(self) -> None:
        """
            Detects running in Databricks Connect environment by checking whether config
            location is on DBFS or on local filesystem. If config location is on the local
            filesystem, reconfigures Petastorm to pull materialized data set using dbutils.

            :param config_location: app configuration location
            :param petastorm_cache: file:// URL for the local petastorm cache location
            :return: returns nothing
        """
        if self.__is_dbfs(self.config_location):
            self.logger.info(f"Configuration {self.config_location} is a DBFS location. Assuming we are inside Databricks. No need to patch Petastorm.")
        else:
            try:
                global petastorm
                petastorm.__version__
            except NameError:
                self.logger.info(f"Correct version of Petastorm is not present. No need to patch Petastorm.")
            else:
                try:
                    # The following import resolves only within Databricks or with databricks-connect
                    # pylint: disable=no-name-in-module, import-error
                    from pyspark.dbutils import DBUtils
                except ImportError:
                    self.logger.info("Databricks Connect not detected. No need to patch Petastorm.")
                else:
                    self.logger.warn(f"Configuration {self.config_location} is not a DBFS location, so we are not in Databricks. "
                                     f"But pyspark.dbutils is present, so we are operating Databricks Connect."
                                     f"Monkeypatching Petastorm, so that Databricks Connect is able to train local "
                                     f"Keras models using remote Databricks data.")
                    self._adapt_petastorm_to_databricks_connect(petastorm, DBUtils)

    def configure(self) -> None:
        pass

    @abstractmethod
    def launch(self) -> None:
        raise NotImplementedError("launch() must be implemented in the concrete subclass")
