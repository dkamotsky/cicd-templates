#! /bin/bash

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
