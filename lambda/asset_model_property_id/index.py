import boto3
import sys
import logging
import json

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
iotsitewise = boto3.client('iotsitewise')


# method to get asset model property id from asset_model_id and property_name
def get_asset_model_property_id(asset_model_id, property_name):
    """Returns the asset model property id for the given asset model id and property name
    """
    asset_model = iotsitewise.describe_asset_model(assetModelId=asset_model_id)
    logger.info('Asset Model: %s', json.dumps(asset_model, default=str))
    for asset_model_property in asset_model['assetModelProperties']:
        if asset_model_property['name'] == property_name:
            return asset_model_property['id']
    for asset_composite_model in asset_model['assetModelCompositeModels']:
        for asset_model_property in asset_composite_model['properties']:
            if asset_model_property['name'] == property_name:
                return asset_model_property['id']
    return None

def handler(event, context):
    """Lambda handler
    """
    logger.info('Received event: %s', event)
    asset_model_id = event['ResourceProperties']['assetModelId']
    property_name = event['ResourceProperties']['propertyName']
    asset_model_property_id = get_asset_model_property_id(asset_model_id, property_name)

    cfn_response = {
        "Data": {
            "assetModelId": asset_model_id,
            "propertyName": property_name,
            "propertyId": asset_model_property_id
        }
    }
    logger.info('Returning response: %s', cfn_response)

    return cfn_response


if __name__ == '__main__':
    # test the lambda locally
    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
    print(get_asset_model_property_id('ebb0b33f-dcf6-4262-86eb-b78641dd8cd4', 'AVG L4E Score'))
    print(get_asset_model_property_id('ebb0b33f-dcf6-4262-86eb-b78641dd8cd4', 'AWS/ALARM_STATE'))
