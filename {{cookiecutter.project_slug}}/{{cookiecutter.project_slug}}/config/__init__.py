"""
config module contains convenience functions for reading application settings
"""


import mlflow
import yaml
import time
from typing import MutableMapping, Any
from . import settings


def read_config(config_file_name: str, root: str) -> MutableMapping[str, Any]:
    """
        Reads application configuration first from settings.py in this package, then
        overrides values from config_file_name located in root. Applies
        inlcudes in config_file_name if the YAML contains "include: [file1, ..., fileN]",
        where file1, ..., fileN are relative paths to included YAMLs, starting
        from the given root. If included files contain keys which already exist
        in the config_file_name, the included values override config_file_name values.

        :param config_file_name: str config file name
        :param root: str location to look for the config_file_name, if starts with dbfs:, will look in the FUSE mount
        :return: returns application configuration dict
    """
    try:
        root = root.replace('dbfs:', '/dbfs') + '/'
        with open(root + config_file_name) as conf_file:
            # First get conf from settings.py
            conf = dict(settings.__dict__)
            yaml_conf = yaml.load(conf_file, Loader=yaml.FullLoader)
            # Then add conf from all files included in the given YAML
            for inc in yaml_conf.get("include", []):
                with open(root + inc) as include_file:
                    conf.update(yaml.load(include_file))
            # Finally add conf from the given YAML itself
            conf.update(yaml_conf)
            return conf
    except FileNotFoundError as e:
        raise FileNotFoundError(f"{e}. Please include a config file!")


def setup_mlflow(experiment_path: str, **kwargs) -> str:
    """
        Configures MLflow experiment.

        :param experiment_path: str MLflow experiment path
        :return: returns str MLflow experiment ID
    """
    mlflow.set_tracking_uri("databricks")
    mlflow.set_experiment(experiment_path)
    try:
        experiment_id = mlflow.get_experiment_by_name(experiment_path).experiment_id
        return experiment_id
    except FileNotFoundError:
        time.sleep(10)
        experiment_id = mlflow.get_experiment_by_name(experiment_path).experiment_id
        return experiment_id
