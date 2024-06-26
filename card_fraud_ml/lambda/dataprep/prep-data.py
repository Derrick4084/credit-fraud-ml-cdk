import os
import boto3
import io
import numpy as np
import pandas as pd
import sagemaker.amazon.common as smac
import json

s3_client = boto3.client("s3")
sfn_client = boto3.client('stepfunctions')
dynamodb_client = boto3.resource('dynamodb')

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


def save_data_to_s3(features, bucket, key, labels=None):
    vectors = np.array([t.tolist() for t in features]).astype("float32")
    buf = io.BytesIO()
    if labels is None:
        smac.write_numpy_to_dense_tensor(buf, vectors)
    else:
        labels_list = np.array([t.tolist() for t in labels]).astype("float32")
        smac.write_numpy_to_dense_tensor(buf, vectors, labels_list)   
    buf.seek(0)
    boto3.resource("s3").Bucket(bucket).Object(key).upload_fileobj(buf)
    print("Successfully saved data to s3")



def lambda_handler(event, context):

    rawbucket = event["rawcsvbucket"]
    rawkey = event["rawcsvfilename"]
    sagemaker_bucket = event["smBucket"]
    s3_train_key = event["smTrainKey"]
    s3_test_key = event["smTestKey"]

    put_item_dynamodb(rawbucket, rawkey)
    
    response = s3_client.get_object(Bucket=rawbucket, Key=rawkey)

    if response["ResponseMetadata"]["HTTPStatusCode"] == 200:
        print("Successful S3 get_object response")
        raw_data = pd.read_csv(response.get("Body")).values
        np.random.seed(0)
        np.random.shuffle(raw_data)
        train_size = int(raw_data.shape[0] * 0.7)
        train_features = raw_data[:train_size, :-1]
        train_labels = np.array([x.strip("'") for x in raw_data[:train_size, -1]]).astype(int)
        test_features = raw_data[train_size:, :-1]
        test_labels = np.array([x.strip("'") for x in raw_data[train_size:, -1]]).astype(int)

        # Convert the processed training data to protobuf and write to S3 for linear learner
        save_data_to_s3(train_features, sagemaker_bucket, s3_train_key, train_labels)
              
        # Convert the processed testing data to protobuf and write to S3 for linear learner       
        save_data_to_s3(test_features, sagemaker_bucket, s3_test_key, test_labels)

        print("Successfully processed and saved data to s3")   

    return {
        'statusCode': 200,
        'body': json.dumps('Started State Machine')
    }


