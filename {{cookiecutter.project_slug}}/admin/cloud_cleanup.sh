#!/bin/bash

cat .exctxv2.yaml | shyaml keys cicdtemplconfv2.exctxs | xargs -I {} databricks clusters permanent-delete --cluster-id '{}'
