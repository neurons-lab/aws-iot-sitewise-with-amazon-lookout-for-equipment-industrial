from aws_cdk import (
    aws_iotsitewise as iotsitewise,
    aws_iotevents as iotevents,
    aws_iam as iam,
    custom_resources as cr,
    CfnOutput,
    Duration,
    aws_lambda as _lambda,
    aws_iam as iam,
    aws_logs as logs,
    CustomResource
)
from constructs import Construct

class TwoEngineVesselIoTSiteWiseAsset(Construct):

    def __init__(self, scope: Construct, id: str, vessel_asset_model: object, engine_asset_model: object, property_list: list, *, prefix=None):
        super().__init__(scope, id)
        
        self.engine_model_id = engine_asset_model.ref
        self.vessel_model_id = vessel_asset_model.ref

        # Create Asset Properties for Engine
        asset_properties=[]
        for property in property_list:
            asset_properties.append(iotsitewise.CfnAsset.AssetPropertyProperty(
                logical_id=property,
                notification_state="ENABLED"
            ))
        additional_property = [
            "AVGL4EScore",
            "AssetL4EScore",
            "AWS/ALARM_STATE"
        ]
        for property in additional_property:
            asset_properties.append(iotsitewise.CfnAsset.AssetPropertyProperty(
                logical_id=property,
                notification_state="ENABLED"
            ))
        
        self.engine_asset0_name = f'{id}EngineAsset0'
        self.engine_asset1_name = f'{id}EngineAsset1'
        self.vessel_asset_name = f'{id}VesselAsset'

        # Create Assets
        self.engine_asset0 = iotsitewise.CfnAsset(self, f'{id}EngineAsset0',
            asset_model_id=self.engine_model_id,
            asset_name=self.engine_asset0_name,
            asset_properties=asset_properties
        )
        self.engine_asset1 = iotsitewise.CfnAsset(self, f'{id}EngineAsset1',
            asset_model_id=self.engine_model_id,
            asset_name=self.engine_asset1_name,
            asset_properties=asset_properties
        )
        self.vessel_asset = iotsitewise.CfnAsset(self, f'{id}VesselAsset',
            asset_model_id=self.vessel_model_id,
            asset_name=self.vessel_asset_name,
            asset_hierarchies=[
                iotsitewise.CfnAsset.AssetHierarchyProperty(
                    logical_id="EngineHierarchy",
                    child_asset_id=self.engine_asset0.ref
                ),
                iotsitewise.CfnAsset.AssetHierarchyProperty(
                    logical_id="EngineHierarchy",
                    child_asset_id=self.engine_asset1.ref
                )
            ],
            asset_properties=[
                iotsitewise.CfnAsset.AssetPropertyProperty(
                    logical_id="TotalL4EScore",
                    notification_state="ENABLED"
                )
            ]
        )




            