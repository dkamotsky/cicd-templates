# Databricks notebook source
import re
from mlflow.tracking import MlflowClient
mlflow_client = MlflowClient()
ci_holder_name = "cet_debris_detection_cicd"
versions = mlflow_client.get_latest_versions(ci_holder_name, stages=["Production"])
assert len(versions) == 1
ci_holder = versions[0]
source_run = mlflow_client.get_run(ci_holder.run_id)
dist_info = [fi for fi in mlflow_client.list_artifacts(source_run.info.run_id, 'dist') if fi.path.endswith('.whl')]
assert len(dist_info) == 1
dist_info = dist_info[0]
lib_path = f"{source_run.info.artifact_uri}/{dist_info.path}"
lib_path = re.sub(r"^dbfs:/", "/dbfs/", lib_path)
job_info = [fi for fi in mlflow_client.list_artifacts(source_run.info.run_id, 'job') if fi.path.endswith('runtime_requirements.txt')]
assert len(job_info) == 1
job_info = job_info[0]
req_path = f"{source_run.info.artifact_uri}/{job_info.path}"
req_path = re.sub(r"^dbfs:/", "/dbfs/", req_path)
print(lib_path)
print(req_path)
%pip install -r $req_path
%pip install -U $lib_path
