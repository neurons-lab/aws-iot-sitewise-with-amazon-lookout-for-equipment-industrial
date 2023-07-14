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

L4E_DATASET_LAMBDA_PATH = "lambda/l4e_dataset"



class L4ESetup(Construct):
    def __init__(self, scope: Construct, id: str, property_list: list, asset_data: dict, *, prefix=None):
        super().__init__(scope, id)
        self.asset_data = asset_data
        self.property_list = property_list

        self.state_machine_input={
            "Assets": [
                {
                    "Input": {
                        "Name": "Engine0",
                        "WaitTime": 60,
                        "TrainingData": {
                            "Bucket": "etlpipeline-l4e-bucket0",
                            "KeyPattern": "{prefix}/{component_name}/*",
                            "Prefix": self.asset_data[0]["asset_id"] + "/training-data/"
                        },
                        "LableData": {
                            "Bucket": "etlpipeline-l4e-bucket0",
                            "Prefix": self.asset_data[0]["asset_id"] + "/label-data/"
                        },
                        "InferenceData": {
                            "InputBucket": "etlpipeline-l4e-bucket0",
                            "InputPrefix": self.asset_data[0]["asset_id"] + "/inference-data/input/",
                            "OutputBucket": "etlpipeline-l4e-bucket0",
                            "OutputPrefix": self.asset_data[0]["asset_id"] + "/inference-data/output/"
                        },
                        "InlineDataSchema": "{\"Components\": [{\"ComponentName\": \"engine\", \"Columns\": [{\"Name\": \"Timestamp\", \"Type\": \"DATETIME\"}, {\"Name\": \"Sensor0\", \"Type\": \"DOUBLE\"}, {\"Name\": \"Sensor1\", \"Type\": \"DOUBLE\"}, {\"Name\": \"Sensor2\", \"Type\": \"DOUBLE\"}, {\"Name\": \"Sensor3\", \"Type\": \"DOUBLE\"}, {\"Name\": \"Sensor4\", \"Type\": \"DOUBLE\"}, {\"Name\": \"Sensor5\", \"Type\": \"DOUBLE\"}, {\"Name\": \"Sensor6\", \"Type\": \"DOUBLE\"}, {\"Name\": \"Sensor7\", \"Type\": \"DOUBLE\"}, {\"Name\": \"Sensor8\", \"Type\": \"DOUBLE\"}, {\"Name\": \"Sensor9\", \"Type\": \"DOUBLE\"}, {\"Name\": \"Sensor10\", \"Type\": \"DOUBLE\"}, {\"Name\": \"Sensor11\", \"Type\": \"DOUBLE\"}, {\"Name\": \"Sensor24\", \"Type\": \"DOUBLE\"}, {\"Name\": \"Sensor25\", \"Type\": \"DOUBLE\"}, {\"Name\": \"Sensor26\", \"Type\": \"DOUBLE\"}, {\"Name\": \"Sensor27\", \"Type\": \"DOUBLE\"}, {\"Name\": \"Sensor28\", \"Type\": \"DOUBLE\"}, {\"Name\": \"Sensor29\", \"Type\": \"DOUBLE\"}, {\"Name\": \"Sensor12\", \"Type\": \"DOUBLE\"}, {\"Name\": \"Sensor13\", \"Type\": \"DOUBLE\"}, {\"Name\": \"Sensor14\", \"Type\": \"DOUBLE\"}, {\"Name\": \"Sensor15\", \"Type\": \"DOUBLE\"}, {\"Name\": \"Sensor16\", \"Type\": \"DOUBLE\"}, {\"Name\": \"Sensor17\", \"Type\": \"DOUBLE\"}, {\"Name\": \"Sensor18\", \"Type\": \"DOUBLE\"}, {\"Name\": \"Sensor19\", \"Type\": \"DOUBLE\"}, {\"Name\": \"Sensor20\", \"Type\": \"DOUBLE\"}, {\"Name\": \"Sensor21\", \"Type\": \"DOUBLE\"}, {\"Name\": \"Sensor22\", \"Type\": \"DOUBLE\"}, {\"Name\": \"Sensor23\", \"Type\": \"DOUBLE\"}]}]}",
                        "ModelTraining": {
                            "TrainingStart": "2019-01-01T00:00:00Z",
                            "TrainingEnd": "2019-07-31T00:00:00Z",
                            "EvaluationStart": "2019-08-01T00:00:00Z",
                            "EvaluationEnd": "2019-10-27T00:00:00Z"
                        }
                    }
                }
            ]
        }
        self.sfn_iam_role_arn = iam.Role(self, "L4EStateMachineRole",
            assumed_by=iam.ServicePrincipal("lookoutequipment.amazonaws.com"),
        )
        self.sfn_iam_role_arn.add_to_policy(iam.PolicyStatement(
            actions=["lookoutequipment:*"],
            resources=["*"]
        ))
        self.sfn_iam_role_arn.add_to_policy(iam.PolicyStatement(
            actions=["s3:*"],
            resources=["*"]
        ))
        self.sfn_iam_role_arn.add_to_policy(iam.PolicyStatement(
            actions=["logs:*"],
            resources=["*"]
        ))

        self._create_step_function()


    # Step function to ingest data to IoT SiteWise
    def _create_step_function(self):
        input_step = sfn.Pass(self, "Start",
            result=sfn.Result.from_object(self.state_machine_input)
        )
        job_failed = sfn.Fail(self, "Job Failed",
            cause="Job Failed",
            error="Job Failed"
        )
        job_success = sfn.Succeed(self, "Job Succeeded")
        # Steps to create dataset and ingest data
        # Create dataset
        create_dataset_step = sfn_tasks.CallAwsService(self, "CreateL4EDataset",
            service="lookoutequipment",
            action="createDataset",
            iam_resources=["*"],
            iam_action="lookoutequipment:CreateDataset",
            parameters={
                "ClientToken": sfn.JsonPath.string_at("$.Input.Name"),
                "DatasetName": sfn.JsonPath.string_at("$.Input.Name"),
                "DatasetSchema": {
                    "InlineDataSchema": sfn.JsonPath.string_at("$.Input.InlineDataSchema")
                }
            },
            result_path="$.CreateDatasetResult"
        )
        # Start data ingestion
        start_data_ingestion_step = sfn_tasks.CallAwsService(self, "StartDataIngestionJob",
            service="lookoutequipment",
            action="startDataIngestionJob",
            iam_resources=["*"],
            iam_action="lookoutequipment:StartDataIngestionJob",
            parameters={
                "ClientToken": sfn.JsonPath.string_at("$.Input.Name"),
                "DatasetName": sfn.JsonPath.string_at("$.Input.Name"),
                "RoleArn": self.sfn_iam_role_arn.role_arn,
                "IngestionInputConfiguration": {
                    "S3InputConfiguration": {
                        "Bucket": sfn.JsonPath.string_at("$.Input.TrainingData.Bucket"),
                        "Prefix": sfn.JsonPath.string_at("$.Input.TrainingData.Prefix"),
                        "KeyPattern": sfn.JsonPath.string_at("$.Input.TrainingData.KeyPattern")
                    }
                }
            },
            result_path="$.StartDataIngestionJobResult"
        )
        # Describe data ingestion job
        describe_data_ingestion_job_step = sfn_tasks.CallAwsService(self, "DescribeDataIngestionJob",
            service="lookoutequipment",
            action="describeDataIngestionJob",
            iam_resources=["*"],
            iam_action="lookoutequipment:DescribeDataIngestionJob",
            parameters={
                "JobId": sfn.JsonPath.string_at("$.StartDataIngestionJobResult.JobId")
            },
            result_path="$.DescribeDataIngestionJobResult"
        )
        # Wait X seconds for dataset to be created and ingest data
        wait_x_for_creaete_dataset_step = sfn.Wait(self, "Wait X Seconds for Create Dataset",
            time=sfn.WaitTime.seconds_path("$.Input.WaitTime")
        )

        # Model training steps
        # Create model
        create_model_step = sfn_tasks.CallAwsService(self, "CreateL4EModel",
            service="lookoutequipment",
            action="createModel",
            iam_resources=["*"],
            iam_action="lookoutequipment:CreateModel",
            parameters={
                "ClientToken": sfn.JsonPath.string_at("$.Input.Name"),
                "ModelName": sfn.JsonPath.string_at("$.Input.Name"),
                "DatasetName": sfn.JsonPath.string_at("$.Input.Name"),
                "DataPreProcessingConfiguration": {
                                "TargetSamplingRate": "PT5M"
                },
                "DatasetSchema": {
                    "InlineDataSchema": sfn.JsonPath.string_at("$.Input.InlineDataSchema")
                },
                "LabelsInputConfiguration": {
                    "S3InputConfiguration": {
                        "Bucket": sfn.JsonPath.string_at("$.Input.LableData.Bucket"),
                        "Prefix": sfn.JsonPath.string_at("$.Input.LableData.Prefix")
                    }
                },
                "RoleArn": self.sfn_iam_role_arn.role_arn,
                "TrainingDataStartTime": sfn.JsonPath.string_at("$.Input.ModelTraining.TrainingStart"),
                "TrainingDataEndTime": sfn.JsonPath.string_at("$.Input.ModelTraining.TrainingEnd"),
                "EvaluationDataStartTime": sfn.JsonPath.string_at("$.Input.ModelTraining.EvaluationStart"),
                "EvaluationDataEndTime": sfn.JsonPath.string_at("$.Input.ModelTraining.EvaluationEnd")
            },
            result_path="$.CreateModelResult"
        )
        # Wait X seconds for model to be created
        wait_x_for_create_model_step = sfn.Wait(self, "Wait X Seconds for Create Model",
            time=sfn.WaitTime.seconds_path("$.Input.WaitTime")
        )
        # Describe model
        describe_model_step = sfn_tasks.CallAwsService(self, "DescribeModel",
            service="lookoutequipment",
            action="describeModel",
            iam_resources=["*"],
            iam_action="lookoutequipment:DescribeModel",
            parameters={
                "ModelName": sfn.JsonPath.string_at("$.Input.Name")
            },
            result_path="$.DescribeModelResult",
            result_selector={
                "Status": sfn.JsonPath.string_at("$.Status")
            }
        )

        # Inference steps
        # Start inference scheduler
        start_inference_scheduler_step = sfn_tasks.CallAwsService(self, "StartInferenceScheduler",
            service="lookoutequipment",
            action="createInferenceScheduler",
            iam_resources=["*"],
            iam_action="lookoutequipment:CreateInferenceScheduler",
            parameters={
                "ModelName": sfn.JsonPath.string_at("$.Input.Name"),
                "ClientToken": sfn.JsonPath.string_at("$.Input.Name"),
                "InferenceSchedulerName": sfn.JsonPath.string_at("$.Input.Name"),
                "DataDelayOffsetInMinutes": 2,
                "DataUploadFrequency": "PT5M",
                
                "RoleArn": self.sfn_iam_role_arn.role_arn,
                "DataInputConfiguration": {
                    "InputTimeZoneOffset": "+00:00",
                    "InferenceInputNameConfiguration": {
                        "ComponentTimestampDelimiter": "_",
                        "TimestampFormat": "yyyyMMddHHmmss"
                    },
                    "S3InputConfiguration": {
                        "Bucket": sfn.JsonPath.string_at("$.Input.InferenceData.InputBucket"),
                        "Prefix": sfn.JsonPath.string_at("$.Input.InferenceData.InputPrefix")
                    }
                },
                "DataOutputConfiguration": {
                    "S3OutputConfiguration": {
                        "Bucket": sfn.JsonPath.string_at("$.Input.InferenceData.OutputBucket"),
                        "Prefix": sfn.JsonPath.string_at("$.Input.InferenceData.OutputPrefix")
                    }
                }
            },
            result_path="$.StartInferenceSchedulerResult"
        )
        # Describe inference scheduler
        describe_inference_scheduler_step = sfn_tasks.CallAwsService(self, "DescribeInferenceScheduler",
            service="lookoutequipment",
            action="describeInferenceScheduler",
            iam_resources=["*"],
            iam_action="lookoutequipment:DescribeInferenceScheduler",
            parameters={
                "InferenceSchedulerName": sfn.JsonPath.string_at("$.Input.Name")
            },
            result_path="$.DescribeInferenceSchedulerResult"
        )
        # Wait X seconds for inference scheduler to be created
        wait_x_for_create_inference_scheduler_step = sfn.Wait(self, "Wait X Seconds for Create Inference Scheduler",
            time=sfn.WaitTime.seconds_path("$.Input.WaitTime")
        )

        # Block
        block = create_dataset_step.next(
            start_data_ingestion_step
        ).next(
            wait_x_for_creaete_dataset_step
        ).next(
            describe_data_ingestion_job_step
        ).next(
            sfn.Choice(self, "Data Ingestion Job Complete?").when(
                sfn.Condition.string_equals("$.DescribeDataIngestionJobResult.Status", "FAILED"),
                job_failed
            ).when(
                sfn.Condition.string_equals("$.DescribeDataIngestionJobResult.Status", "SUCCESS"),
                create_model_step.next(
                    wait_x_for_create_model_step
                ).next(
                    describe_model_step
                ).next(
                    sfn.Choice(self, "Create Model Created?").when(
                        sfn.Condition.string_equals("$.DescribeModelResult.Status", "FAILED"),
                        job_failed
                    ).when(
                        sfn.Condition.string_equals("$.DescribeModelResult.Status", "SUCCESS"),
                        start_inference_scheduler_step.next(
                            wait_x_for_create_inference_scheduler_step
                        ).next(
                            describe_inference_scheduler_step
                        ).next(
                            sfn.Choice(self, "Create Inference Scheduler Created?").when(
                                sfn.Condition.string_equals("$.DescribeInferenceSchedulerResult.Status", "FAILED"),
                                job_failed
                            ).when(
                                sfn.Condition.string_equals("$.DescribeInferenceSchedulerResult.Status", "RUNNING"),
                                job_success
                            ).otherwise(
                                wait_x_for_create_inference_scheduler_step
                            )
                        )
                    ).otherwise(
                        wait_x_for_create_model_step
                    )
                )
            ).otherwise(
                wait_x_for_creaete_dataset_step
            )
        )

        map_cfn_chain = sfn.Chain.start(block)

        process_asset_map = sfn.Map(self, "ProcessAssetMap",
            items_path=sfn.JsonPath.string_at("$.Assets"),
            max_concurrency=2
        )
        process_asset_map.iterator(
            map_cfn_chain
        )
        cfn_chain = sfn.Chain.start(input_step).next(process_asset_map)
        state_machine = sfn.StateMachine(self, "IngestDataStateMachine",
            definition=cfn_chain
        )
        state_machine.add_to_role_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    "iam:PassRole",
                ],
                conditions={
                    "StringEquals": {
                        "iam:PassedToService": "lookoutequipment.amazonaws.com"
                    }
                },
                resources=[
                    self.sfn_iam_role_arn.role_arn
                ]
            )
        )
        return state_machine
