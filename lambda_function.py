import boto3
import json
import pandas as pd
from io import StringIO

s3_client = boto3.client('s3')
sns_client = boto3.client('sns')
sns_arn = 'arn:aws:sns:ap-south-1:891376987570:s3-arrival-notification'

def lambda_handler(event, context):
    # TODO implement
    print(event)
    try:
        bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
        s3_file_key = event["Records"][0]["s3"]["object"]["key"]
        print(bucket_name)
        print(s3_file_key)
        resp = s3_client.get_object(Bucket=bucket_name, Key=s3_file_key)
        print(resp['Body'])
        df_s3_data = pd.read_json(resp['Body'], sep=",")
        print(df_s3_data.head(10))

        #filter the dataframe with status as 'Delivered'
        result_df = df_s3_data[df_s3_data["status"] == "delivered"]
        result_df.head(10)

        target_bucket = "sathiya-doordash-target-zn"
        target_key = "2024-03-12-processed_output" + ".json"

        #writing dataframe to JSON file in S3 Target Bucket
        output_buffer = StringIO()
        result_df.to_json(output_buffer)
        output_content = output_buffer.getvalue()
        s3_client.put_object(Bucket = target_bucket, Key = target_key, Body = output_content)

        #publish a message through email using SNS Service
        message = "Input S3 File {} has been processed succesfuly !!".format("s3://"+bucket_name+"/"+s3_file_key)
        respone = sns_client.publish(Subject="SUCCESS - Daily Data Processing",TargetArn=sns_arn, Message=message, MessageStructure='text')
    except Exception as err:
        print(err)
        message = "Input S3 File {} processing is Failed !!".format("s3://"+bucket_name+"/"+s3_file_key)
        respone = sns_client.publish(Subject="FAILED - Daily Data Processing", TargetArn=sns_arn, Message=message, MessageStructure='text')