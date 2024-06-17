import boto3
import json
import sagemaker
import os
import urllib.parse

sfn_client = boto3.client('stepfunctions')
dynamodb_client = boto3.resource('dynamodb')

sagemaker_bucket = sagemaker.Session().default_bucket()
prefix = "sagemaker/creditcard_fraud_linear_learner" 
s3_train_key = "{}/train-data/recordio-pb-train-data".format(prefix)
s3_train_path = os.path.join("s3://", sagemaker_bucket, s3_train_key)
s3_test_key = "{}/test-data/recordio-pb-test-data".format(prefix)
s3_test_path = os.path.join("s3://", sagemaker_bucket, s3_test_key)
s3_model_test = "{}/testresults/".format(prefix)
s3_model_test_path = os.path.join("s3://", sagemaker_bucket, s3_model_test)


STATE_MACHINE_ARN = os.environ['STATE_MACHINE_ARN']
DYNAMODB_TABLENAME = os.environ['DYNAMODB_TABLENAME']

def put_item_dynamodb(rawbucket, rawkey):
    dynamodb_client = boto3.client('dynamodb')
    response = dynamodb_client.put_item(
        TableName=DYNAMODB_TABLENAME,
        Item={
            'bucketname': {'S': rawbucket},
            'filename': {'S': rawkey}
        }
    )
    return response


def lambda_handler(event, context):

    rawbucket = event["Records"][0]["s3"]["bucket"]["name"]
    rawkey = urllib.parse.unquote_plus(event["Records"][0]["s3"]["object"]["key"], encoding='utf-8')

    put_item_dynamodb(rawbucket, rawkey)
    
    statemachine_payload = {
        "smBucket": sagemaker_bucket,             
        "smInstanceType": "ml.m5.xlarge",
        "smTrainPath": s3_train_path,          
        "smTestPath": s3_test_path,
        "smModelOutput": f"s3://{sagemaker_bucket}/{prefix}/svm_balanced/output",
        "Model": {"imageName": "382416733822.dkr.ecr.us-east-1.amazonaws.com/linear-learner:1"},
        "rawcsvbucket": rawbucket,
        "rawcsvfilename": rawkey,
        "smTrainKey": s3_train_key,
        "smTestKey": s3_test_key
    }
    
    response = sfn_client.start_execution(
        stateMachineArn=STATE_MACHINE_ARN,
        input= json.dumps(statemachine_payload)
    )
    print(response)

    return {
        'statusCode': 200,
        'body': json.dumps('State machine has been started')
    }