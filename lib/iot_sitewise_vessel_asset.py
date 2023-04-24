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

PROPERTY_ID_CF_LAMBDA_PATH = "lambda/asset_model_property_id"

class TwoEngineVesselIoTSiteWiseAsset(Construct):

    def __init__(self, scope: Construct, id: str, vessel_asset_model: object, engine_asset_model: object, *, prefix=None):
        super().__init__(scope, id)
        
        # Create Assets
        self.engine_asset0 = iotsitewise.CfnAsset(self, f'{id}EngineAsset0',
            asset_model_id=engine_asset_model.ref,
            asset_name=f'{id}EngineAsset0',
        )
        self.engine_asset1 = iotsitewise.CfnAsset(self, f'{id}EngineAsset1',
            asset_model_id=engine_asset_model.ref,
            asset_name=f'{id}EngineAsset1',
        )
        self.vessel_asset = iotsitewise.CfnAsset(self, f'{id}VesselAsset',
            asset_model_id=vessel_asset_model.ref,
            asset_name=f'{id}VesselAsset',
            asset_hierarchies=[
                iotsitewise.CfnAsset.AssetHierarchyProperty(
                    logical_id="EngineHierarchy",
                    child_asset_id=self.engine_asset0.ref
                ),
                iotsitewise.CfnAsset.AssetHierarchyProperty(
                    logical_id="EngineHierarchy",
                    child_asset_id=self.engine_asset1.ref
                )
            ]
        )

        # IAM Role for IoT Events to use for Alarm Actions for SiteWise
        self.alarm_role = iam.Role(self, f'{id}AlarmRole',
            assumed_by=iam.ServicePrincipal('iotevents.amazonaws.com'),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name('AWSIoTEventsFullAccess'),
                iam.ManagedPolicy.from_aws_managed_policy_name('AWSIoTSiteWiseFullAccess')
            ]
        )

        # Get Asset Model ID for Engine Asset Model
        self.engine_model_asset_id = engine_asset_model.attr_asset_model_id


        property_id = "b04f1da1-7da1-43e2-bde5-968283ddc81a"

        # Create IoT Event Alarm Model for AVG L4E Score Alarm for Engine #0
        self.avg_l4e_score_alarm = iotevents.CfnAlarmModel(self, f'{id}AvgL4EScoreAlarm',
            alarm_model_name="AvgL4EScoreAlarm",
            role_arn=self.alarm_role.role_arn,
            alarm_rule=iotevents.CfnAlarmModel.AlarmRuleProperty(
                simple_rule=iotevents.CfnAlarmModel.SimpleRuleProperty(
                    input_property=f"$sitewise.assetModel.`{self.engine_model_asset_id}`.`b04f1da1-7da1-43e2-bde5-968283ddc81a`.propertyValue.value",
                    comparison_operator="GREATER_OR_EQUAL",
                    threshold=f"$sitewise.assetModel.`{self.engine_model_asset_id}`.`c0970bd1-0157-4891-abb6-ad4dab2068e2`.propertyValue.value"
                )
            ),
            alarm_capabilities=iotevents.CfnAlarmModel.AlarmCapabilitiesProperty(
                acknowledge_flow=iotevents.CfnAlarmModel.AcknowledgeFlowProperty(
                    enabled=False
                ),
                initialization_configuration=iotevents.CfnAlarmModel.InitializationConfigurationProperty(
                    disabled_on_initialization=False
                )
            ),
            alarm_event_actions=
                iotevents.CfnAlarmModel.AlarmEventActionsProperty(
                    alarm_actions=[iotevents.CfnAlarmModel.AlarmActionProperty(
                        iot_site_wise=iotevents.CfnAlarmModel.IotSiteWiseProperty(
                            asset_id=f"$sitewise.assetModel.`{self.engine_model_asset_id}`.`b04f1da1-7da1-43e2-bde5-968283ddc81a`.assetId",
                            property_id="'bd11027e-d0b0-46c5-bf2e-48cfada20eda'"
                        )
                    )
                ]
            )
        )
        
        # iam role for lambda to get the asset model property id for the Asset Model
        self.get_asset_model_property_id_role = iam.Role(self, f'{id}GetAssetModelPropertyIdRole',
            assumed_by=iam.ServicePrincipal('lambda.amazonaws.com'),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name('service-role/AWSLambdaBasicExecutionRole'),
                iam.ManagedPolicy.from_aws_managed_policy_name('AWSIoTSiteWiseFullAccess')
            ]
        )

        # lambda for custom resource the get the asset model property id for the Asset Model
        self.get_asset_model_property_id_lambda = _lambda.Function(self, f'{id}GetAssetModelPropertyIdLambda',
            runtime=_lambda.Runtime.PYTHON_3_8,
            handler='index.handler',
            code=_lambda.Code.from_asset(PROPERTY_ID_CF_LAMBDA_PATH),
            timeout=Duration.seconds(30),
            memory_size=128,
            role=self.get_asset_model_property_id_role
        )

        # custom provider for the lambda to get the asset model property id for the Asset Model
        self.get_asset_model_property_id_provider = cr.Provider(self, f'{id}GetAssetModelPropertyIdProvider',
            on_event_handler=self.get_asset_model_property_id_lambda,
            log_retention=logs.RetentionDays.ONE_DAY,
        )

        avg_l4e_score_property_id = CustomResource(self, f'{id}GetAssetModelPropertyIdCustomResource',
            service_token=self.get_asset_model_property_id_provider.service_token,
            properties={
                "assetModelId": self.engine_model_asset_id,
                "propertyName": "AVG L4E Score"
            }
        ).get_att_string("propertyId")

        CfnOutput(self, f'{id}AvgL4EScoreAlarmPropertyId',
            value=avg_l4e_score_property_id,
            description="Property ID for AVG L4E Score Alarm"
        )
