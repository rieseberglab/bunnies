
# Creating lambdas

Info: https://aws.amazon.com/blogs/compute/better-together-amazon-ecs-and-aws-lambda/

0. set up IAM role for reprod lambdas

  aws iam create-role \
      --role-name reprod-lambda-role \
      --path / \
      --description "role for reprod lambdas" \
      --assume-role-policy-document file://reprod-lambda-trust-relationship.json

  aws iam put-role-policy \
      --role-name reprod-lambda-role \
      --policy-name reprod_lambda_permissions \
      --policy-document file://reprod-lambda-policy.json

1. set up S3 bucket:

  aws s3 mb s3://my-bucket

2. Create the lambda function:

      zip the stuff.

      aws lambda create-function \
           --region ca-central-1 \
           --function-name wake-up-ecs \
           --zip-file fileb://foo.zip \
           --runtime python3.6 \
           --handler main.lambda_handler \
           --timeout 15 \
           --memory-size 512

                      