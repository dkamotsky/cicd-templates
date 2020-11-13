from dbx.commands.execute import *
from dbx.commands.execute import _preprocess_cluster_args
from dbx.commands.deploy import _adjust_job_definitions


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help="Runs given code on the existing cluster. Different from 'execute' in how configuration is handled. Corning implementation.")
@click.option("--cluster-id", required=False, type=str, help="Cluster ID.")
@click.option("--cluster-name", required=False, type=str, help="Cluster name.")
@click.option("--job", required=True, type=str, help="Test job name to be executed")
@click.option("--deployment-file", required=False, type=str,
              help="Path to deployment file in json format", default=DEFAULT_DEPLOYMENT_FILE_PATH)
@click.option("--requirements-file", required=False, type=str, default="requirements.txt")
@click.option("--no-rebuild", is_flag=True, help="Disable job rebuild")
@environment_option
def run(environment: str,
        cluster_id: str,
        cluster_name: str,
        job: str,
        deployment_file: str,
        requirements_file: str,
        no_rebuild: bool):
    api_client = prepare_environment(environment)

    cluster_id = _preprocess_cluster_args(api_client, cluster_name, cluster_id)

    dbx_echo("Executing job: %s with environment: %s on cluster: %s" % (job, environment, cluster_id))

    handle_package(no_rebuild)

    env_data = DeploymentFile(deployment_file).get_environment(environment)

    if not env_data:
        raise Exception(
            f"Environment {environment} is not provided in deployment file {deployment_file}" +
            " please add this environment first"
        )

    env_jobs = env_data.get("jobs")
    if not env_jobs:
        raise Exception(f"No jobs section found in environment {environment}, please check the deployment file")

    found_jobs = [j for j in env_data["jobs"] if j["name"] == job]

    if not found_jobs:
        raise Exception(f"Job {job} was not found in environment jobs, please check the deployment file")

    job_payload = found_jobs[0]
    entrypoint_file = job_payload.get("spark_python_task").get("python_file", "")
    if not entrypoint_file:
        raise ValueError(f"No entrypoint file provided in job {job}. "
                         f"Please add one under spark_python_task.python_file section")

    cluster_service = ClusterService(api_client)

    dbx_echo("Preparing interactive cluster to accept jobs")
    awake_cluster(cluster_service, cluster_id)

    v1_client = ApiV1Client(api_client)
    context_id = get_context_id(v1_client, cluster_id, "python")
    file_uploader = FileUploader(api_client)

    with mlflow.start_run() as execution_run:

        artifact_base_uri = execution_run.info.artifact_uri

        _adjust_job_definitions(found_jobs, artifact_base_uri, [], [], file_uploader)
        job_payload = found_jobs[0]
        argv = [entrypoint_file] + job_payload.get("spark_python_task").get("parameters", [])

        localized_base_path = artifact_base_uri.replace("dbfs:/", "/dbfs/")
        requirements_fp = pathlib.Path(requirements_file)
        if requirements_fp.exists():
            file_uploader.upload_file(requirements_fp)
            localized_requirements_path = "%s/%s" % (localized_base_path, str(requirements_fp))
            installation_command = "%pip install -U -r {path}".format(path=localized_requirements_path)
            dbx_echo("Installing provided requirements")
            execute_command(v1_client, cluster_id, context_id, installation_command, verbose=False)
            dbx_echo("Provided requirements installed")
        else:
            dbx_echo(f"Requirements file {requirements_fp} is not provided" +
                     ", following the execution without any additional packages")

        project_package_path = list(pathlib.Path(".").rglob("dist/*.whl"))[0]
        file_uploader.upload_file(project_package_path)
        localized_package_path = "%s/%s" % (localized_base_path, project_package_path.as_posix())
        installation_command = "%pip install --upgrade {path}".format(path=localized_package_path)
        execute_command(v1_client, cluster_id, context_id, installation_command, verbose=False)
        dbx_echo("Package installed")

        tags = {
            "dbx_action_type": "execute",
            "dbx_environment": environment
        }

        mlflow.set_tags(tags)

        execute_command(v1_client, cluster_id, context_id, f'import mlflow')
        execute_command(v1_client, cluster_id, context_id, f'import sys\nsys.argv = {argv}')
        dbx_echo("Starting entrypoint file execution")
        execute_command(v1_client, cluster_id, context_id, pathlib.Path(entrypoint_file).read_text())
        dbx_echo("Command execution finished")
