export AWS_REGION="eu-west-1"
export AWS_GLUE_ROLE="ldp-integration-glue-crawler-test"
export AWS_GLUE_DATABASE="ldp-integration-test"
export ENV="dev"
export AWS_PROFILE="non-prod"

python3 main.py "ldp-integration-glue-test-3"