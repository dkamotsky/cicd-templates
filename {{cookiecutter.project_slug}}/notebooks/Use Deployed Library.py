# Databricks notebook source
import re
from mlflow.tracking import MlflowClient
mlflow_client = MlflowClient()
ci_holder_name = "{{cookiecutter.project_slug}}_cicd"
versions = mlflow_client.get_latest_versions(ci_holder_name, stages=["Production"])
assert len(versions) == 1
ci_holder = versions[0]
source_run = mlflow_client.get_run(ci_holder.run_id)
dist_info = mlflow_client.list_artifacts(source_run.info.run_id, 'dist')
assert len(dist_info) == 1
dist_info = dist_info[0]
lib_path = f"{source_run.info.artifact_uri}/{dist_info.path}"
lib_path = re.sub(r"^dbfs:/", "/dbfs/", lib_path)
%pip install -U $lib_path
