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


class AlertModel(Construct):
    def __init__(self, scope: Construct, id: str, *, prefix=None):
        super().__init__(scope, id)

        # IAM Role for IoT Events to use for Alarm Actions for SiteWise
        self.alarm_role = iam.Role(self, f'{id}AlarmRole',
            assumed_by=iam.ServicePrincipal('iotevents.amazonaws.com'),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name('AWSIoTEventsFullAccess'),
                iam.ManagedPolicy.from_aws_managed_policy_name('AWSIoTSiteWiseFullAccess')
            ]
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

    def get_property_id(self, asset_model_id: str, property_name: str):
        """Returns the asset model property id for the given asset model id and property name"""
        # custom resource to get the asset model property id for the Asset Model
        asset_model_property_id = CustomResource(self, f'{id}GetAssetModelPropertyIdCustomResource',
            service_token=self.get_asset_model_property_id_provider.service_token,
            properties={
                'assetModelId': asset_model_id,
                'propertyName': property_name
            }
        )
        return asset_model_property_id.get_att_string('propertyId')

    def create_alert_model(self, id: str, asset_model_id: str, property_name: str, threshold_property_name: str):
        """Creates an alert model for the given asset model id and property name"""
        self.asset_model_id = asset_model_id
        self.property_id = self.get_property_id(asset_model_id, property_name)
        self.threshold_property_id = self.get_property_id(asset_model_id, threshold_property_name)

       # Create IoT Event Alarm Model
        self.avg_l4e_score_alarm = iotevents.CfnAlarmModel(self, f'{id}Alarm',
            alarm_model_name=f'{id}Alarm',
            role_arn=self.alarm_role.role_arn,
            alarm_rule=iotevents.CfnAlarmModel.AlarmRuleProperty(
                simple_rule=iotevents.CfnAlarmModel.SimpleRuleProperty(
                    input_property=f"$sitewise.assetModel.`{self.asset_model_id}`.`{self.property_id}`.propertyValue.value",
                    comparison_operator="GREATER_OR_EQUAL",
                    threshold=f"$sitewise.assetModel.`{self.asset_model_id}`.`{self.threshold_property_id}`.propertyValue.value"
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
                            asset_id=f"$sitewise.assetModel.`{self.asset_model_id}`.`{self.property_id}`.assetId",
                            property_id="'bd11027e-d0b0-46c5-bf2e-48cfada20eda'"
                        )
                    )
                ]
            )
        )

