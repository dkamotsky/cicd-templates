#! /bin/bash

if [ -f /home/host/.databricks-connect ]; then
  cp /home/host/.databricks-connect /root
else
  echo "File .databricks-connect must be mounted in volume /home/host!"
  echo "Example format: \
  { \
    \"host\": \"https://corning.cloud.databricks.com\", \
    \"token\": \"dapide0d6d19a85df37f51cac172ebe893ed\", \
    \"cluster_id\": \"0801-196502-gear443\", \
    \"org_id\": \"0\", \
    \"port\": \"15001\" \
  }"
  exit 1
fi

echo "Starting Tensorboard..."
tensorboard --logdir=/{{cookiecutter.project_slug}}/logs --port=6006 --bind_all &

echo "Starting Jupyter from notebooks..."
cd notebooks
jupyter notebook --ip=0.0.0.0 --allow-root --no-browser --NotebookApp.token='' --NotebookApp.password='' &
cd ..

#Kill all background children when this script is killed
trap "trap - SIGTERM && kill -- -$$" SIGINT SIGTERM EXIT

#echo "Tailing Application Log File..."
#tail -F ${LOG_FILE}

wait
