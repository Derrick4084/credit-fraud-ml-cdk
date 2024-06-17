#!/usr/bin/env python3
import aws_cdk as cdk
from card_fraud_ml.statemachine_stack import CardFraudStateMachineStack
from card_fraud_ml.lambda_stack import CardFraudLambdaStack

raw_bucket_name = f"card-fraud-raw-data-{cdk.Aws.ACCOUNT_ID}"
dynamodb_bucket_info = "card-fraud-raw-bucket-info"
dynamodb_fraud_table = "card-fraud-response"
kinesis_stream_name = "card-fraud-stream"
state_machine_name = "card-fraud-state-machine"
sagemaker_endpoint_name = "card-fraud-endpoint"

param_dict = {
  "raw_bucket_name": raw_bucket_name,
  "dynamodb_bucket_info": dynamodb_bucket_info,
  "dynamodb_fraud_table": dynamodb_fraud_table,
  "kinesis_stream_name": kinesis_stream_name,
  "state_machine_name": state_machine_name,
  "sagemaker_endpoint_name": sagemaker_endpoint_name,
}

app = cdk.App()

CardFraudLambdaStack(app, "CardFraudLambdaStack", stack_params=param_dict)
CardFraudStateMachineStack(app, "CardFraudStateMachineStack", stack_params=param_dict)


app.synth()
