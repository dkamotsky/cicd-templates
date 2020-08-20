#!/bin/bash

echo "Running static Python type checks..."
echo
echo
mypy --config-file ./tests/mypy.ini {{cookiecutter.project_slug}} && \
mypy --config-file ./tests/mypy.ini tests && \
for dir in `find dev-tests -maxdepth 1 -mindepth 1 -type d`; do mypy --config-file ./tests/mypy.ini $dir; done && \
for dir in `find integration-tests -maxdepth 1 -mindepth 1 -type d`; do mypy --config-file ./tests/mypy.ini $dir; done && \
for dir in `find pipelines -maxdepth 1 -mindepth 1 -type d`; do mypy --config-file ./tests/mypy.ini $dir; done

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
pylint --rcfile=./tests/pylint.rc -E {{cookiecutter.project_slug}} tests dev-tests integration-tests pipelines

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
cd tests
export PYTHONPATH="$PYTHONPATH:.."
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
