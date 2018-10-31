#!/bin/bash

aws lambda create-function \
--region region \
--function-name HelloPython \
--zip-file fileb://deployment-package.zip \
--role arn:aws:iam::account-id:role/lambda_basic_execution  \
--handler hello_python.my_handler \
--runtime python3.6 \
--timeout 15 \
--memory-size 512
