import boto3
import csv
import os


s3_client = boto3.client('s3')
kinesis_client = boto3.client('kinesis')
dynamodb_client = boto3.resource('dynamodb')

STREAM_NAME = os.environ['STREAM_NAME']
RAW_BUCKET_NAME = os.environ['RAW_BUCKET_NAME']
BUCKET_TABLE_NAME = os.environ['BUCKET_TABLE_NAME']

def get_item_dynamodb(bucketname):
    dynamodb_client = boto3.resource('dynamodb')
    try:
        table = dynamodb_client.Table(BUCKET_TABLE_NAME)
        response = table.get_item(
            Key={
                "bucketname": bucketname
            }
        )
        return response['Item']['filename']
    except Exception as msg:
        print(f"Oops, could not get: {msg}")
        return msg
    



def lambda_handler(event, context):

    #get the item from dynamodb
    key = get_item_dynamodb(RAW_BUCKET_NAME)

    obj = s3_client.get_object(Bucket=RAW_BUCKET_NAME, Key=key)
    data = obj['Body'].read().decode('utf-8').splitlines()
    records = csv.reader(data)
    
    cont = 0

    #iterate over the csv lines
    for row in records:
        #remove last column
        row.pop()
        #ignores header - row 0
        if cont > 0:
            #replace unwanted characters
            str_row = str(row).replace("'", "").replace("[", "").replace("]", "").replace(" ", "")
            #print(str_row)
            print('sending transaction #' + str(cont))
            print(str_row)
            
            #send the record to Kinesis Streams in csv format...
            kinesis_client.put_record(
                StreamName=STREAM_NAME,
                Data=str_row,
                PartitionKey="partitionkey")
        
        #stop after sending 10 transactions
        cont = cont + 1
        if cont > 1000:
            break
    
    