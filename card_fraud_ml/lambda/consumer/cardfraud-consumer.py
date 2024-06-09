import json
import os
import boto3
import base64
from datetime import datetime
import uuid

# grab environment variables
SM_ENDPOINT_NAME = os.environ['SM_ENDPOINT_NAME']
SM_RESULTS_TABLE_NAME = os.environ['SM_RESULTS_TABLE_NAME']
BUCKET_TABLE_NAME = os.environ['BUCKET_TABLE_NAME']
RAW_BUCKET_NAME = os.environ['RAW_BUCKET_NAME']

sagemaker_client = boto3.client('runtime.sagemaker')
dynamodb_client = boto3.client('dynamodb')

def get_item_dynamodb(bucketname):
    try:
        table = dynamodb_client.Table(BUCKET_TABLE_NAME)
        response = table.get_item(
            Key={
                "bucketname": bucketname
            }
        )
        return response
    except Exception as msg:
        print(f"Oops, could not get: {msg}")
        return msg


def put_item(score, payload):
    dynamodb_client.put_item(
        TableName=SM_RESULTS_TABLE_NAME,
        Item={
            "id": {
                "S": str(uuid.uuid1())
            },
            "datetime": {
                "S": datetime.now().strftime("%d/%m/%Y %H:%M:%S")
            },
            "score": {
                "N": str(score)
            },
            "raw_transaction": {
                "S": str(payload)
            }
        }
    )

def delete_item_dynamodb(bucketname):
    
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(BUCKET_TABLE_NAME)
    try:
        response = table.delete_item(
            Key={
                'bucketname': bucketname
            }
        )
        return(response)
    except Exception as msg:
       print(f"Oops, could not update: {msg}")
       print(msg)
       return msg


def lambda_handler(event, context):
    print('Received event: ' + json.dumps(event, indent=2))
    
    for record in event['Records']:
        # Kinesis data is base64 encoded so decode here
        payload = base64.b64decode(record['kinesis']['data']).decode('utf-8')

        response = sagemaker_client.invoke_endpoint(EndpointName=SM_ENDPOINT_NAME,
                                                    ContentType='text/csv',
                                                    Body=payload)

        result = json.loads(response['Body'].read().decode())
        score = result['predictions'][0]['score']
        predicted_label = result['predictions'][0]['predicted_label']

        if predicted_label == 1:
            put_item(score, payload)
            print('This looks like a fraud...' + ' Score: ' + str(score))

    delete_item_dynamodb(RAW_BUCKET_NAME)

    print('Successfully processed {} transactions.'.format(len(event['Records'])))

    