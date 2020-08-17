#!/usr/bin/env python
import mlflow
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("model_name", help="Registered MLflow model name")
args = parser.parse_args()
print(args)

mlflow.set_tracking_uri("databricks")
client = mlflow.tracking.MlflowClient()

for mv in client.search_model_versions(f"name='{args.model_name}'"):
    try:
        client.transition_model_version_stage(name=args.model_name, version=mv.version, stage="Archived")
        print(f"Archived {args.model_name} version {mv.version}")
    except Exception as e:
        print(f"Failed to archive {args.model_name} version {mv.version}: {e}")

# Delete a registered model along with all its versions
client.delete_registered_model(name=args.model_name)
print(f"Deleted {args.model_name}")
