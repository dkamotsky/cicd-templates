FROM openjdk:8 AS java

FROM python:3.7 AS base

COPY --from=java /usr/local/openjdk-8 /usr/local/openjdk-8

MAINTAINER Denis Kamotsky "kamotskyd@corning.com"

ENV PIP_DEFAULT_TIMEOUT 300

RUN apt-get update -y
RUN apt-get install -y vim less jq

RUN pip install --upgrade pip

# /{{cookiecutter.project_slug}} is not mounted yet!
COPY ./requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# /{{cookiecutter.project_slug}} is not mounted yet!
COPY ./tools /tools
RUN pip install --no-cache-dir /tools/dbx-0.7.0-py3-none-any.whl

ENV PYTHONPATH /{{cookiecutter.project_slug}}:./
ENV JAVA_HOME /usr/local/openjdk-8
ENV PATH="${JAVA_HOME}/bin:${PATH}"

#Tensorboard and Jupyter
EXPOSE 6006 8888

WORKDIR /{{cookiecutter.project_slug}}

CMD ["sh", "-c", "echo 'Starting wait with locally mounted filesystem...' && sleep infinity"]

FROM base AS connect

RUN pip uninstall -y pyspark
RUN pip install databricks-connect==7.3.3b0
