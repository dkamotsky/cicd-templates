#! /bin/bash

python -u /{{cookiecutter.project_slug}}/run_now --cluster ${DB_CLUSTER} $@

