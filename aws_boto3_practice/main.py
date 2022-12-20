import boto3
import pandas 
import time
import csv
import athena_from_S3



# AWS_ACCESS_KEY_ID = 'AKIAV6BBTZZGOKM3LQOK'
# AWS_SECRET_ACCESS_KEY = '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'

# client = boto3.client(
#     's3',
#     aws_access_key_id = AWS_ACCESS_KEY_ID,
#     aws_secret_access_key = AWS_SECRET_ACCESS_KEY
# )

# response = client.list_buckets() 


params = {
    'region' : 'ap-northeast-2',
    'database' : 'test',
    'bucket' : 'sydev-lake',
    'path' : 'data/',
    'query' : 'SELECT * FROM "test"."tripdata1" limit 10;'   
}


session = boto3.Session()

location, data = athena_from_S3.query_results(session, params)
print("Locations: ",location)
print("Result Data: ")
print(data)