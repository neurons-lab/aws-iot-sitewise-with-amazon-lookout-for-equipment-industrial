from aws_cdk import (
    Duration,
    aws_iotsitewise as iotsitewise,
    aws_s3_assets as s3_assets,
    aws_lambda as _lambda,
    aws_iam as iam,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as sfn_tasks,
    aws_events as events,
    aws_events_targets as targets,
)
from constructs import Construct

INGEST_DATA_LAMBDA_PATH = "lambda/ingest_data"

class DataIngestToIoTSiteWiseAsset(Construct):
    """
    This construct creates a state machine to ingest data to IoT SiteWise asset
    Parameters:
        scope (Construct): The scope of the construct
        id (str): The id of the construct
        property_list (list): List of properties to ingest data to
            ["Sensor0", "Sensor1", "Sensor2", "Sensor3", "Sensor4", "Sensor5", "Sensor6", "Sensor7", "Sensor8", "Sensor9"]
        asset_data (dict): Dictionary of asset data
            [
                {
                    "asset_id": "asset0_id",
                    "data": "data/EngineAsset0.txt"
                },
                {
                    "asset_id": "asset1_id",
                    "data": "data/EngineAsset1.txt"
                }
            ]
    """

    def __init__(self, scope: Construct, id: str, property_list: list, asset_data: dict, *, prefix=None):
        super().__init__(scope, id)
        self.asset_data = asset_data
        self.property_list = property_list

        self.ingest_data_lambda = self._create_ingest_data_lambda()
        self.s3_asset_data = self._store_asset_data_in_s3()
        self.property_list_and_s3_asset_data = self._merge_property_list_and_s3_asset_data()
        self.ingest_data_state_machine = self._create_ingest_data_step_function()

    # function to Store asset data in S3 with read permissions to ingest data lambda
    def _store_asset_data_in_s3(self):
        asset_data = []
        for asset in self.asset_data:
            asset_obj = s3_assets.Asset(self, f'AssetData{asset["asset_id"]}',
                path=asset["data_path"]
            )
            asset_data.append(
                {
                    "asset_id": asset["asset_id"],
                    "asset_data_path": asset_obj.s3_object_key,
                    "asset_data_bucket": asset_obj.s3_bucket_name
                }
            )
            asset_obj.grant_read(self.ingest_data_lambda)
        return asset_data
    
    # function to ingest data to IoT SiteWise
    def _create_ingest_data_lambda(self):
        ingest_data_lambda  = _lambda.Function(self, "IngestDataLambda",
            code=_lambda.Code.from_asset(INGEST_DATA_LAMBDA_PATH),
            handler="index.handler",
            runtime=_lambda.Runtime.PYTHON_3_8,
            timeout=Duration.seconds(90),
            memory_size=512,
        )
        # Lambda function permissions to ingest data to IoT SiteWise
        ingest_data_lambda.add_to_role_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    "iotsitewise:*",
                ],
                resources=["*"]
            )
        )
        return ingest_data_lambda
    
    # function to merge property list and s3 asset data
    def _merge_property_list_and_s3_asset_data(self):
        assets_properties = []
        for asset in self.s3_asset_data:
            for propoerty_name in self.property_list:
                assets_properties.append({
                    "property_to_put_data": propoerty_name,
                    "asset_id": asset["asset_id"],
                    "asset_data_bucket": asset["asset_data_bucket"],
                    "asset_data_path": asset["asset_data_path"],
                })
        return assets_properties
    
    # Step function to ingest data to IoT SiteWise
    def _create_ingest_data_step_function(self):
        first_step = sfn.Pass(self, "Start",
            result=sfn.Result.from_object({
                "assets": self.property_list_and_s3_asset_data
            })
        )
        process_asset_map = sfn.Map(self, "ProcessAssetMap",
            items_path=sfn.JsonPath.string_at("$.assets"),
            max_concurrency=10
        )
        process_asset_map.iterator(sfn_tasks.LambdaInvoke(self, "IngestData",
                lambda_function=self.ingest_data_lambda
        ))
        cfn_chain = sfn.Chain.start(first_step).next(process_asset_map)
        ingest_data_state_machine = sfn.StateMachine(self, "IngestDataStateMachine",
            definition=cfn_chain
        )
        events.Rule(self, "IngestDataStateMachineRule",
            description="Ingest data to IoT SiteWise",
            schedule=events.Schedule.expression("cron(* * * * ? *)"),
            targets=[targets.SfnStateMachine(ingest_data_state_machine)]
        )
        return ingest_data_state_machine