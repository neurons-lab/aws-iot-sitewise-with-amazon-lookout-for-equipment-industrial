"""
Metadata on assets from AWS IoT SiteWise.
"""
import json
import logging
import os
import sys
import time

import boto3
from botocore.exceptions import ClientError
from ratelimiter import RateLimiter
from retrying import retry

logger = logging.getLogger()
logger.setLevel(logging.INFO)
stream_hanlder = logging.StreamHandler(stream=sys.stdout)
formatter = logging.Formatter(
    "%(asctime)s - %(name)s - %(levelname)s - %(message)s")
stream_hanlder.setFormatter(formatter)
logger.addHandler(stream_hanlder)

s3 = boto3.client("s3")
sitewise = boto3.client("iotsitewise")
MAX_REQUESTS_PER_PERIOD = 20
MAX_REQUESTS_PER_PERIOD_MODEL = 8
PERIOD_LENGTH_IN_SECONDS = 1

STANDARD_RETRY_MAX_ATTEMPT_COUNT = 10


def lambda_handler(event, context):
    bucket_name = event['bucket_name']
    key_name_prefix = event['key_name_prefix']

    for asset in list_asset_generator():
        asset_summary = describe_asset_from_sitewise(asset["id"])
        asset_property_list = extract_asset_property_details(
            asset_summary, asset)
        result = [json.dumps(record) for record in asset_property_list]

        key = key_name_prefix + "/" + "asset-id-" + asset["id"] + ".ndjson"
        body = "\n".join(result)
        # Write one file per asset
        put_object_to_s3(bucket_name, key, body)

    return {
        "statusCode": 200,
        "body": json.dumps("Lambda is successfully executed")
    }

def is_retryable_error(exception):
    if isinstance(exception, ClientError):
        error_code = exception.response['Error']['Code']
        if error_code == 'ThrottlingException' or error_code == 'InternalFailureException':
            return True
    return False

@RateLimiter(max_calls=MAX_REQUESTS_PER_PERIOD,
             period=PERIOD_LENGTH_IN_SECONDS)
@retry(retry_on_exception=is_retryable_error,
       stop_max_attempt_number=STANDARD_RETRY_MAX_ATTEMPT_COUNT)
def describe_asset_from_sitewise(asset_id):
    asset_summary = sitewise.describe_asset(assetId=asset_id)
    return asset_summary

def list_asset_model_generator():
    token = None
    first_execution = True
    while first_execution or token is not None:
        first_execution = False
        asset_model_list_result = list_asset_models_from_sitewise(
            next_token=token)
        token = asset_model_list_result.get("nextToken")
        for asset_model in asset_model_list_result["assetModelSummaries"]:
            yield asset_model

@RateLimiter(max_calls=MAX_REQUESTS_PER_PERIOD_MODEL,
             period=PERIOD_LENGTH_IN_SECONDS)
@retry(retry_on_exception=is_retryable_error,
       stop_max_attempt_number=STANDARD_RETRY_MAX_ATTEMPT_COUNT)
def list_asset_models_from_sitewise(next_token=None):
    if next_token is None:
        asset_model_lists_summary = sitewise.list_asset_models(maxResults=250)
    else:
        asset_model_lists_summary = sitewise.list_asset_models(nextToken=next_token, maxResults=250)
    return asset_model_lists_summary

def list_asset_generator():
    for asset_model in list_asset_model_generator():
        token = None
        first_execution = True
        while first_execution or token is not None:
            first_execution = False
            asset_list_result = list_assets_from_sitewise(asset_model["id"], next_token=token)
            token = asset_list_result.get("nextToken")
            for asset in asset_list_result["assetSummaries"]:
                yield asset

@RateLimiter(max_calls=MAX_REQUESTS_PER_PERIOD,
             period=PERIOD_LENGTH_IN_SECONDS)
@retry(retry_on_exception=is_retryable_error,
       stop_max_attempt_number=STANDARD_RETRY_MAX_ATTEMPT_COUNT)
def list_assets_from_sitewise(asset_model_id, next_token=None):
    if next_token is None:
        asset_lists_summary = sitewise.list_assets(
            assetModelId=asset_model_id, maxResults=250)
    else:
        asset_lists_summary = sitewise.list_assets(assetModelId=asset_model_id, nextToken=next_token, maxResults=250)
    return asset_lists_summary

@RateLimiter(max_calls=MAX_REQUESTS_PER_PERIOD,
             period=PERIOD_LENGTH_IN_SECONDS)
@retry(retry_on_exception=is_retryable_error,
       stop_max_attempt_number=STANDARD_RETRY_MAX_ATTEMPT_COUNT)
def put_object_to_s3(bucket, key, body):
    s3.put_object(Bucket=bucket, Key=key, Body=body)

def extract_asset_property_details(asset_summary, asset):
    asset_property_list = []
    for asset_property in asset_summary["assetProperties"]:
        assetpropertyinfo = {}
        assetpropertyinfo["asset_id"] = asset["id"]
        assetpropertyinfo["asset_name"] = asset["name"]
        assetpropertyinfo["asset_model_id"] = asset["assetModelId"]
        assetpropertyinfo["asset_property_id"] = asset_property["id"]
        assetpropertyinfo["asset_property_name"] = asset_property["name"]
        assetpropertyinfo["asset_property_data_type"] = asset_property["dataType"]
        assetpropertyinfo["asset_property_unit"] = asset_property.get("unit")
        assetpropertyinfo["asset_property_alias"] = asset_property.get("alias")
        asset_property_list.append(assetpropertyinfo)
    return asset_property_list
