import boto3
import logging
import json
import time
import random
from datetime import datetime, timedelta, timezone
from ratelimiter import RateLimiter

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
iotsitewise = boto3.client('iotsitewise')
s3 = boto3.client('s3')

MAX_ENTRIES_IN_BATCH = 1
NUM_VALUES_PER_ENTRY = 10

SECONDS_PER_MINUTE = 60

STANDARD_INVOCATION_DURATION_TO_UPLOAD_DATA_FOR = timedelta(minutes=1)

MAX_REQUESTS_PER_PERIOD = 1
PERIOD_LENGTH_IN_SECONDS = 1

def put_all_data(asset_id, asset_data_bucket, asset_data_file_path, start_time, base_offset, num_entries_needed, property_to_put_data):
    desc_asset_response = iotsitewise.describe_asset(assetId=asset_id)
    obj = s3.get_object(Bucket=asset_data_bucket, Key=asset_data_file_path)
    body = obj['Body']
    asset_data = json.load(body)
    for asset_property in desc_asset_response['assetProperties']:
        if asset_property['name'] == property_to_put_data:
            asset_property_values = asset_data[asset_property['name']]
            for batch in get_batches(asset_id, asset_property, asset_property_values, start_time, base_offset, num_entries_needed):
                send_batch_put_asset_property_value(batch)
    return True

def get_batches(asset_id, asset_property, asset_property_values, start_time, base_offset, num_entries_needed):
    num_batches = int(num_entries_needed // MAX_ENTRIES_IN_BATCH)
    if num_entries_needed % MAX_ENTRIES_IN_BATCH != 0:
        num_batches += 1
    for batch_number in range(num_batches):
        yield get_batch(asset_id, asset_property, asset_property_values, start_time, base_offset, num_entries_needed, batch_number)

def get_batch(asset_id, asset_property, asset_property_values, start_time, base_offset, num_entries_needed, batch_number):
    batch = []
    for entry_id in range(MAX_ENTRIES_IN_BATCH):
        seconds_since_start_for_current_entry = (batch_number*MAX_ENTRIES_IN_BATCH*NUM_VALUES_PER_ENTRY)+(entry_id*NUM_VALUES_PER_ENTRY)
        property_value_range = get_property_value_range(asset_property_values, base_offset, seconds_since_start_for_current_entry)
        property_values = get_property_values(property_value_range, start_time, seconds_since_start_for_current_entry)
        entry = {'entryId': str(entry_id), 'assetId': asset_id, 'propertyId':asset_property['id'], 'propertyValues':property_values}
        batch.append(entry)
        num_entries_created = (batch_number*MAX_ENTRIES_IN_BATCH) + len(batch)
        if num_entries_created == num_entries_needed:
            break
    return batch

def get_property_value_range(asset_property_values, base_offset, seconds_since_start_for_current_entry):
    minutes_since_start = seconds_since_start_for_current_entry // SECONDS_PER_MINUTE
    range_minimum_index = (base_offset + minutes_since_start) % len(asset_property_values)
    range_maximum_index = (range_minimum_index + 1) % len(asset_property_values)
    return [asset_property_values[range_minimum_index],asset_property_values[range_maximum_index]]

def get_property_values(property_value_range, start_time, seconds_since_start_for_current_entry):
    property_values = []
    start_time_timestamp = int(start_time.timestamp())
    for value_second in range(NUM_VALUES_PER_ENTRY):
        current_timestamp = start_time_timestamp + seconds_since_start_for_current_entry + value_second
        value = get_value_in_range(property_value_range)
        property_value = {'value': {'doubleValue': value}, 'timestamp':{'timeInSeconds':current_timestamp,'offsetInNanos':0},'quality':'GOOD'}
        property_values.append(property_value)
    return property_values

def get_value_in_range(property_value_range):
    return random.uniform(property_value_range[0], property_value_range[1])

@RateLimiter(max_calls=MAX_REQUESTS_PER_PERIOD, period=PERIOD_LENGTH_IN_SECONDS)
def send_batch_put_asset_property_value(entries):
    return iotsitewise.batch_put_asset_property_value(entries=entries)


def handler(event, context):
    logger.info('Received event: %s', event)
    now = datetime.now(timezone.utc)

    asset_id = event['asset_id']
    asset_data_bucket = event['asset_data_bucket']
    asset_data_path = event['asset_data_path']
    property_to_put_data = event['property_to_put_data']

    logger.info('property_to_put_data: %s', property_to_put_data)
    duration_to_upload_data_for = STANDARD_INVOCATION_DURATION_TO_UPLOAD_DATA_FOR
    start_time = now - duration_to_upload_data_for
    base_offset = 0
    num_entries_needed = int(duration_to_upload_data_for.total_seconds() // NUM_VALUES_PER_ENTRY)
    put_all_data(asset_id, asset_data_bucket, asset_data_path, start_time, base_offset, num_entries_needed, property_to_put_data)