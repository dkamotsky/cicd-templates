#!/bin/bash

tests_dir=`dirname ${0}`
cd ${tests_dir}

echo "Running static Python type checks..."
echo
echo
mypy --config-file mypy.ini ../{{cookiecutter.project_slug}} && \
mypy --config-file mypy.ini integration
mypy --config-file mypy.ini unit

if [ $? -eq 0 ];then
    echo
    echo
    echo "Static Type Checks SUCCESS"
else
    echo
    echo
    echo "Static Type Checks FAILED: Aborting!"
    exit 1
fi

echo "Running static PyLint ERROR-ONLY checks..."
echo
echo
pylint --rcfile=pylint.rc -E ../{{cookiecutter.project_slug}} integration unit

if [ $? -eq 0 ];then
    echo
    echo
    echo "Static PyLint ERROR-ONLY Checks SUCCESS"
else
    echo
    echo
    echo "Static PyLint ERROR-ONLY Checks FAILED: Aborting!"
    exit 1
fi

echo "Running Unit Tests..."
echo
echo
cd unit
export PYTHONPATH="$PYTHONPATH:../.."
echo "Modified for unit tests: PYTHONPATH = $PYTHONPATH"
python -m pytest

if [ $? -eq 0 -o $? -eq 5 ];then
    echo
    echo
    echo "Unit Tests SUCCESS"
else
    echo
    echo
    echo "Unit Tests FAILED: Aborting!"
    exit 1
fi

exit 0
