# Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#     http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#author Julia Hu
from __future__ import print_function
import io
import os
import sys
import time
import csv
import datetime
import boto3
import logging
import pandas as pd
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    logger.info('Received event: %s', event)
    asset_name = event['asset_name']
    client = boto3.client('athena')
    s3_url = 's3://' + event['athena_output_bucket'] + '/athenaquery/'
    ################First to see if the view exists or not, if not run the named query to create the view**************************
    response = client.list_query_executions(MaxResults=1,WorkGroup=event['work_group'])
    logger.info('List Query Executions: %s', response['QueryExecutionIds'])

    get_query_response = client.get_named_query(NamedQueryId=event['view_query_id'])
    database = event['database']
    if len(response['QueryExecutionIds']) == 0:
       querystr_4_view = get_query_response['NamedQuery']['QueryString']
       executionResponse = client.start_query_execution(QueryString=querystr_4_view,QueryExecutionContext = {'Database': database, 'Catalog': 'AwsDataCatalog'},ResultConfiguration = {'OutputLocation': s3_url})
       logger.info('Create View: %s', executionResponse)
    else:
       pass
    ########################run query to gather data#####################################
    query_name = get_query_response["NamedQuery"]["Name"]
    logger.info('Named Query: %s', query_name)
    query = f'SELECT * FROM {database}.{query_name}'
    logger.info('Query: %s', query)
    queryStart = client.start_query_execution(QueryString=query,QueryExecutionContext = {'Database': database, 'Catalog': 'AwsDataCatalog'},ResultConfiguration = {'OutputLocation': s3_url})
    logger.info('Query Start: %s', queryStart)
    
    status = int(queryStart["ResponseMetadata"]["HTTPStatusCode"])
    if status == 200:
        print("Query Success")
    else:
        print("Query Failed")
        
    #executes query and waits 3 seconds
    queryId = queryStart["QueryExecutionId"]
    
    time.sleep(10)
    #copies newly generated csv file with appropriate name
    
    c_timestamp = (datetime.datetime.now()- datetime.timedelta(minutes=6)).strftime("%Y%m%d%H%M" + "00")   
    print(c_timestamp)
    
    response = boto3.client('s3').get_object(Bucket=event['athena_output_bucket'], Key='athenaquery/'+ queryId+'.csv')
    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    if status == 200:
        inference_df = pd.read_csv(response.get("Body"))
        inference_df['Timestamp'] = pd.to_datetime(inference_df['Timestamp'])
        inference_df = inference_df.sort_values(by='Timestamp')
        ##First fill in empty values from previous cell, then aggregate to minute, sort by descending order, then select top five
        inference_df.ffill(axis = 0, inplace=True)
        inference_df = inference_df.resample("T", on='Timestamp').mean()
        inference_df = inference_df.sort_values(by='Timestamp', ascending=False).head(5).reset_index()
        print(inference_df)
    else:
        print(f"Unsuccessful S3 get_object response. Status - {status}")
    ########################################write csv to inference scheduler**************************  
    with io.StringIO() as csv_buffer:
        inference_df.to_csv(csv_buffer, index=False, date_format='%Y-%m-%dT%H:%M:%S.%f')                            
        response = boto3.client('s3').put_object(Bucket=event['l4e_bucket'], Key=event['assetId']+'/inference-data/input/'+asset_name+'_'+ c_timestamp+'.csv', Body=csv_buffer.getvalue())
        status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if status == 200:
            print(f"Successful S3 put_object response. Status - {status}")
        else:
            print(f"Unsuccessful S3 put_object response. Status - {status}")
    
    response = boto3.client('s3').delete_object(Bucket=event['athena_output_bucket'],Key='athenaquery/'+ queryId+'.csv')
    print(response)  
    
    response_meta = boto3.client('s3').delete_object(Bucket=event['athena_output_bucket'], Key='athenaquery/'+ queryId+'.csv.metadata')
    print(response_meta)

if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO)
    event = {
        "athena_output_bucket": "etlpipeline-asset-data-bucket",
        "view_query_id": "ff7774d0-ef18-445d-81b1-3ea24afafcd6",
        "l4e_bucket": "etlpipeline-l4e-bucket0",
        "assetId": "14136e4a-08fe-4a14-871d-d8aeb17e3dc2",
        "work_group": "etlpipeline_l4esitewisequery",
        "database": "etlpipeline_firehose_glue_database",
        "asset_name": "engine"
    }
    lambda_handler(event, None)