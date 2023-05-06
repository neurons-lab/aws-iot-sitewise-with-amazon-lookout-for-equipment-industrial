"""
Flattens data produced by AWS IoT SiteWise to make it queryable.
"""
import base64
import json
import logging
import sys

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)
stream_handler = logging.StreamHandler(stream=sys.stdout)
formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)

def handler(event, context):
    logger.info("event: {}".format(event))
    output = []

    # Extract the list of records from event, where each
    # record is a json that contains asset data and recordId.
    for record in event['records']:
        # data contains asset property values.
        data = json.loads(base64.b64decode(record['data']))
        flat_data = flatten_data(data)
        output_record = convert_to_out_format(flat_data, record['recordId'])
        output.append(output_record)

    logger.info('Successfully processed {} records.'.format(
        len(event['records'])))
    return {'records': output}

def convert_to_out_format(flat_data, record_id):
    # Convert the elements in flat data into a list of jsons.
    data_list = [json.dumps(row) for row in flat_data]

    # Convert the list of jsons to new line seprated string
    data = '\n'.join(data_list)
    output_record = {
        'recordId': record_id,
        'result': 'Ok',
        'data': base64.b64encode(data.encode('utf-8')).decode('utf-8')
    }
    return output_record

def flatten_data(data):
    payload_type = data['type']
    asset_id = data['payload']['assetId']
    property_id = data['payload']['propertyId']
    flat_data = []

    try:
        for asset_value in data['payload']['values']:
            raw_value = {"type": payload_type,
                        "asset_id": asset_id, "asset_property_id": property_id}

            time_in_seconds = asset_value['timestamp']['timeInSeconds']
            offset_in_nanos = asset_value['timestamp']['offsetInNanos']

            value, valuetype = extract_value_and_type(asset_value)

            quality = asset_value['quality']

            raw_value['time_in_seconds'] = time_in_seconds
            raw_value['offset_in_nanos'] = offset_in_nanos
            raw_value['asset_property_quality'] = quality
            raw_value['asset_property_value'] = str(value)
            raw_value['asset_property_data_type'] = valuetype

            flat_data.append(raw_value)

    except Exception as e:
        logger.error(e)
    return flat_data

def extract_value_and_type(asset_value):
    if 'doubleValue' in asset_value['value']:
        value = asset_value['value']['doubleValue']
        valuetype = "double"
    if 'integerValue' in asset_value['value']:
        value = asset_value['value']['integerValue']
        valuetype = "integer"
    if 'booleanValue' in asset_value['value']:
        value = asset_value['value']['booleanValue']
        valuetype = "boolean"
    if 'stringValue' in asset_value['value']:
        value = asset_value['value']['stringValue']
        valuetype = "string"
    return value, valuetype