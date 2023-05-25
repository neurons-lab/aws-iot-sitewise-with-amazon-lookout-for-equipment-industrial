from __future__ import print_function
import json
import urllib.parse
import boto3
import logging
import os
import time
from random import randint
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
from botocore.exceptions import ClientError

p = '%Y-%m-%dT%H:%M:%S.%f'
n = 8   #entry id length
MAX_BATCH_PUT_SIZE = 10

s3 = boto3.client('s3')
iotsitewise = boto3.client('iotsitewise')

def process_s3_triggering_object(object_bucket, object_key, number_of_lines = 25, input_serialization=None, output_serialization=None):
    """
    Queries the s3 object triggering the lambda run and converts the inference output into a list of dictionaries
    """
    if output_serialization is None:
        output_serialization = {'JSON': {'RecordDelimiter': '\n'}}
    if input_serialization is None:
        input_serialization = {'JSON': {"Type": "Lines"}}

    query = f'SELECT * FROM s3object s LIMIT {str(number_of_lines)}'

    s3_object_query_response = s3.select_object_content(Bucket=object_bucket, Key=object_key, ExpressionType='SQL', Expression=query, InputSerialization=input_serialization, OutputSerialization=output_serialization)
    s3_object_query_response_list = [event for event in s3_object_query_response['Payload']]
    inferencer_output_list = [json.loads(thing) for thing in s3_object_query_response_list[0]['Records']['Payload'].splitlines()]
    
    return inferencer_output_list
################################These are functions to write asset L4E scores####################################
def send_batch_put_asset_property_value(entries):
    return iotsitewise.batch_put_asset_property_value(entries=entries)    
  
def get_batch(asset_id, property_values, asset_L4E_property_name, desc_asset_response):
    entry_id = ''.join(["{}".format(randint(0, 9)) for num in range(0, n)])
    for i in range (len(desc_asset_response['assetProperties'])):
      if (desc_asset_response['assetProperties'][i]['name'] == asset_L4E_property_name):
        asset_property_id = desc_asset_response['assetProperties'][i]['id']
    
    batch = []
    entry = {'entryId': entry_id, 'assetId': asset_id, 'propertyId':asset_property_id, 'propertyValues':property_values}
    batch.append(entry)
    return batch      
        
def get_property_values(inference_output_list,p):
    property_values = []
    for i in range(len(inference_output_list)):
        timestamp_str = inference_output_list[i]['timestamp']
        timestamp = int(time.mktime(time.strptime(timestamp_str, p)))
        
        value = float(inference_output_list[i]['prediction'])
        property_value = {'value': {'doubleValue': value}, 'timestamp':{'timeInSeconds':timestamp,'offsetInNanos':0},'quality':'GOOD'}
        property_values.append(property_value)
    return property_values

#################################These are functions to write sensor_l4e_scores##############################

def sensor_property_values(inference_output_list, i,j,p):
    property_values = []   
    timestamp_str = inference_output_list[i]['timestamp']
    timestamp = int(time.mktime(time.strptime(timestamp_str, p)))
    sensor_value = inference_output_list[i]['diagnostics'][j]['value']
    property_value = {'value': {'doubleValue': sensor_value}, 'timestamp':{'timeInSeconds':timestamp,'offsetInNanos':0},'quality':'GOOD'}
    property_values.append(property_value)
    return property_values      
    
def sensor_entries(i, j, inference_output_list, asset_id, desc_asset_response, sensor_property_value, entries):
    entry_id = ''.join(["{}".format(randint(0, 9)) for num in range(0, n)])
    sensor_name = inference_output_list[i]['diagnostics'][j]['name'].split('\\')[1]
    for m in range(len(desc_asset_response['assetProperties'])):
        if (desc_asset_response['assetProperties'][m]['name'] == sensor_name+' L4EScore'):
            asset_property_id = desc_asset_response['assetProperties'][m]['id']
            break
    entries.append({'entryId': entry_id, 'assetId': asset_id, 'propertyId':asset_property_id, 'propertyValues':sensor_property_value})
    
def lambda_handler(event, context):
    asset_L4E_property_name = os.environ['Asset_L4E_Score']

    # Get the object from the event and show its content type
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    #############################fetch asset_id from S3 bucket############################# 
    asset_id = key.split('/')[0]
    
    try: 
        inference_output_list = process_s3_triggering_object(object_bucket = bucket, object_key=key)
    except Exception as e:
        logger.error(e)
    
    #######################3write_data_to_sitewise with batch put message###############
    property_values = get_property_values(inference_output_list, p)
    desc_asset_response = iotsitewise.describe_asset(assetId=asset_id)
    
    entry = get_batch(asset_id, property_values, asset_L4E_property_name, desc_asset_response)
    print(entry)
    try: 
        send_batch_put_asset_property_value(entry)
    except Exception as e:
        logger.error(e)
    
    ################Batch put Sensor L4E score########################################
    for i in range(len(inference_output_list)):
        if (float(inference_output_list[i]['prediction']) ==1.0):
            entries = []
            for j in range(len(inference_output_list[i]['diagnostics'])):
                sensor_property_value = sensor_property_values(inference_output_list, i,j,p)
                sensor_entries(i, j, inference_output_list, asset_id, desc_asset_response, sensor_property_value, entries)
                    
            ##############################devide entries by 10 then send to SiteWise#####################
            for k in range(len(entries)//MAX_BATCH_PUT_SIZE):
                try:
                    response = send_batch_put_asset_property_value(entries=entries[k*MAX_BATCH_PUT_SIZE:(k+1)*MAX_BATCH_PUT_SIZE])
                    print(response)
                except Exception as e:
                    logger.error(e)
                            
        else:
            pass