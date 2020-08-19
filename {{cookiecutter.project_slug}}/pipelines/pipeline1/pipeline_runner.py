import sys
from typing import Mapping, Any
from pyspark.sql import SparkSession
from {{cookiecutter.project_slug}}.config import read_config, setup_mlflow


spark: SparkSession = SparkSession.builder.appName('{{cookiecutter.project_name}}').getOrCreate()
conf: Mapping[str, Any] = read_config('config.yaml', sys.argv[1])
setup_mlflow(**conf)

#Add your training code here
