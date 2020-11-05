# Databricks notebook source
import re
from mlflow.tracking import MlflowClient
mlflow_client = MlflowClient()
experiment = mlflow_client.get_experiment_by_name('{{cookiecutter.workspace_dir}}')
run_infos = mlflow_client.list_run_infos(experiment.experiment_id)
run_infos = [ri for ri in run_infos if ri.status=='FINISHED']
assert len(run_infos) > 0
source_run_info = run_infos[0]
dist_info = [fi for fi in mlflow_client.list_artifacts(source_run_info.run_id, 'dist') if fi.path.endswith('.whl')]
assert len(dist_info) == 1
dist_info = dist_info[0]
lib_path = f"{source_run_info.artifact_uri}/{dist_info.path}"
lib_path = re.sub(r"^dbfs:/", "/dbfs/", lib_path)
job_info = [fi for fi in mlflow_client.list_artifacts(source_run_info.run_id, 'conf') if fi.path.endswith('cloud-requirements.txt')]
assert len(job_info) == 1
job_info = job_info[0]
req_path = f"{source_run_info.artifact_uri}/{job_info.path}"
req_path = re.sub(r"^dbfs:/", "/dbfs/", req_path)
print(lib_path)
print(req_path)
%pip install -r $req_path
%pip install -U $lib_path
