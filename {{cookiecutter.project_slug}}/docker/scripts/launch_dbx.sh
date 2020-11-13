#! /bin/bash

#set -x

basedir=`dirname $0`

if [ -f /home/host/.databrickscfg ]; then
  cp /home/host/.databrickscfg /root
else
  echo "File .databrickscfg must be mounted in volume /home/host!"
  exit 1
fi

if [[ $@ == run* ]] || [[ $@ == execute* ]]; then
  if [ -f /home/host/.databricks-connect ]; then
    cp /home/host/.databricks-connect /root
    CLUSTER_OPT="--cluster-id=`jq -r '.cluster_id' /root/.databricks-connect`"
  else
    echo "WARNING! .databricks-connect not found in volume /home/host, please specify --cluster-id!"
  fi
fi

if [[ $@ != launch* ]]; then
  DEPLOY_OPT="--deployment-file=/{{cookiecutter.project_slug}}/.dbx/deployment.json"
  REQ_OPT="--requirements-file=/{{cookiecutter.project_slug}}/.dbx/requirements.txt"
fi

python -u /{{cookiecutter.project_slug}}/tools/dbx_ext $@ ${CLUSTER_OPT} ${DEPLOY_OPT} ${REQ_OPT}
