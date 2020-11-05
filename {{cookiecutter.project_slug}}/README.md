# {{cookiecutter.project_name}}

This is a sample project for Databricks, generated via cookiecutter.

While using this project, you need Python 3.X and `pip` or `conda` for package management.

## Installing project requirements

```bash
pip install -r requirements.txt
```

## Install project package in a developer mode

```bash
pip install -e .
```

## Testing

For local unit testing, please use `pytest`:
```
pytest tests/unit
```

For a functional test on interactive cluster, use the following command:
```
dbx execute \
    --cluster-name=<name of interactive cluster> \
    --job={{cookiecutter.corning_division.lower()}}_{{cookiecutter.project_slug}}_sample_func_test \
    --requirements-file=conf/cloud-requirements.txt
```

For a test on a automated job cluster, use `launch` instead of `execute`:
```
dbx launch \
    --job={{cookiecutter.corning_division.lower()}}_{{cookiecutter.project_slug}}_sample_func_test \
    --requirements-file=conf/cloud-requirements.txt
```

## Interactive execution and development

1. `dbx` expects that cluster for interactive execution supports `%pip` and `%conda` magic [commands](https://docs.databricks.com/libraries/notebooks-python-libraries.html).
2. Please configure your job in `conf/deployment.json` file. 
2. To execute the code interactively, provide either `--cluster-id` or `--cluster-name`.
```bash
dbx execute \
    --cluster-name="<some-cluster-name>" \
    --job=job-name \
    --requirements-file=conf/cloud-requirements.txt
```

Multiple users also can use the same cluster for development. Libraries will be isolated per each execution context.

## Preparing deployment file

Next step would be to configure your deployment objects. To make this process easy and flexible, we're using JSON for configuration.

By default, deployment configuration is stored in `conf/deployment.json`.

## Deployment

To start new deployment, launch the following command:  

```bash
dbx deploy --jobs={{cookiecutter.corning_division.lower()}}_{{cookiecutter.project_slug}}_sample_production --requirements-file=conf/cloud-requirements.txt
```

You can optionally provide requirements.txt via `--requirements` option, all requirements will be automatically added to the job definition.

## Launch

After the deploy, launch the job via the following command:

```
dbx launch --job={{cookiecutter.corning_division.lower()}}_{{cookiecutter.project_slug}}_sample_production
```

## CICD pipeline settings

Please set the following masked variable:
Follow the documentation for [GitLab](https://docs.gitlab.com/ee/ci/variables/#mask-a-custom-variable):
- `DATABRICKS_TOKEN`

## Databricks Connect

You can run code locally in your IDE using Databricks Connect.
- First uninstall pyspark:
```bash
pip uninstall pyspark
```
- Install Databricks Connect (this version must match expectations in requirements.txt, 7.3 CPU ML by default):
```bash
pip install databricks-connect==7.3.3b0
```
- Configure Databricks Connect:
```bash
databricks-connect configure
```
- Make sure java 1.8 is installed and is on your PATH!
- Test Databricks Connect:
```bash
databricks-connect test
```
- If the test is successful, now you can run and debug code directly from IDE
