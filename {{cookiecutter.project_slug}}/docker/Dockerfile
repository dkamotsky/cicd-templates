FROM openjdk:8 AS java

FROM python:3.7 AS base

COPY --from=java /usr/local/openjdk-8 /usr/local/openjdk-8

MAINTAINER Denis Kamotsky "kamotskyd@corning.com"

ENV PIP_DEFAULT_TIMEOUT 300

RUN apt-get update -y
RUN apt-get install -y vim less

RUN pip install --upgrade pip

# /{{cookiecutter.project_slug}} is not mounted yet!
COPY ./tools /tools
RUN pip install --no-cache-dir /tools/dbx-0.7.0-py3-none-any.whl

# /{{cookiecutter.project_slug}} is not mounted yet!
COPY ./requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

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

ARG CLUSTER
RUN : "${CLUSTER:?CLUSTER build argument needs to be set and non-empty.}"
ARG TOKEN
RUN : "${TOKEN:?TOKEN build argument needs to be set and non-empty.}"
ENV DB_CLUSTER=${CLUSTER}

RUN echo \
"[DEFAULT]\n\
host = https://corning.cloud.databricks.com\n\
token = ${TOKEN}" > /root/.databrickscfg

RUN echo \
"{\n\
  \"host\": \"https://corning.cloud.databricks.com\",\n\
  \"token\": \"${TOKEN}\",\n\
  \"cluster_id\": \"${CLUSTER}\",\n\
  \"org_id\": \"0\",\n\
  \"port\": \"15001\"\n\
}" > /root/.databricks-connect
