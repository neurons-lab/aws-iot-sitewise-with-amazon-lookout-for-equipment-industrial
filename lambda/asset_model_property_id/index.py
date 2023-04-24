import boto3
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
iotsitewise = boto3.client('iotsitewise')

# method to get asset model property id from asset_model_id and property_name
def get_asset_model_property_id(asset_model_id, property_name):
    """Returns the asset model property id for the given asset model id and property name
    """
    asset_model_properties = iotsitewise.list_asset_model_properties(assetModelId=asset_model_id)['assetModelPropertySummaries']
    for asset_model_property in asset_model_properties:
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
