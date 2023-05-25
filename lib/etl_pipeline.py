import json
from aws_cdk import (
    aws_kinesisfirehose as firehose,
    aws_iam as iam,
    Duration,
    aws_lambda as _lambda,
    aws_iam as iam,
    aws_logs as logs,
    aws_s3 as s3,
    aws_glue as glue,
    RemovalPolicy,
    Aws,
    aws_iot as iot,
    aws_athena as athena,
    aws_events as events,
    aws_events_targets as targets,
    aws_s3_assets as s3_assets,
)
from constructs import Construct

TRANSFORM_LAMBDA_PATH = "lambda/transform_json_data/"
METADATA_LAMBDA_PATH = "lambda/asset_metadata/"
INFERENCE_SCHEDULE_LAMBDA_PATH = "lambda/inference_schedule/"
L4E_TO_SITEWISE_LAMBDA_PATH = "lambda/l4e_to_sitewise/"

class EtlPipeline(Construct):
    
        def __init__(self, scope: Construct, id: str, *, assets: object, prefix=None):
            super().__init__(scope, id)
            
            # create S3 bucket for IoT SiteWise data
            self.data_bucket = s3.Bucket(self, f'{id}DataBucket',
                bucket_name=f'{prefix}-asset-data-bucket',
                removal_policy=RemovalPolicy.DESTROY,
                auto_delete_objects=True
            )

            # create Firehose Glue Database
            self.glue_database = glue.CfnDatabase(self, f'{id}GlueDatabase',
                catalog_id=Aws.ACCOUNT_ID,
                database_input=glue.CfnDatabase.DatabaseInputProperty(
                    name=f'{prefix}_firehose_glue_database',
                    description='IoT SiteWise data'
                )
            )

            # create Firehose Glue Table, parquet format
            self.glue_table = glue.CfnTable(self, f'{id}GlueTable',
                                            catalog_id=Aws.ACCOUNT_ID,
                                            database_name=self.glue_database.ref,
                                            table_input=glue.CfnTable.TableInputProperty(
                                                description='IoT SiteWise data',
                                                name=f'{prefix}_firehose_glue_table',
                                                table_type='EXTERNAL_TABLE',
                                                parameters={
                                                    'classification': 'parquet',
                                                    'typeOfData': 'file',
                                                    'parquet.compression': 'SNAPPY'
                                                },
                                                storage_descriptor=glue.CfnTable.StorageDescriptorProperty(
                                                    columns=[
                                                        glue.CfnTable.ColumnProperty(
                                                            name='type',
                                                            type='string'
                                                        ),
                                                        glue.CfnTable.ColumnProperty(
                                                            name='asset_id',
                                                            type='string'
                                                        ),
                                                        glue.CfnTable.ColumnProperty(
                                                            name='asset_property_id',
                                                            type='string'
                                                        ),
                                                        glue.CfnTable.ColumnProperty(
                                                            name='time_in_seconds',
                                                            type='int'
                                                        ),
                                                        glue.CfnTable.ColumnProperty(
                                                            name='offset_in_nanos',
                                                            type='int'
                                                        ),
                                                        glue.CfnTable.ColumnProperty(
                                                            name='asset_property_quality',
                                                            type='string'
                                                        ),
                                                        glue.CfnTable.ColumnProperty(
                                                            name='asset_property_value',
                                                            type='string'
                                                        ),
                                                        glue.CfnTable.ColumnProperty(
                                                            name='asset_property_data_type',
                                                            type='string'
                                                        )
                                                    ],
                                                    input_format='org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
                                                    output_format='org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
                                                    location=f's3://{self.data_bucket.bucket_name}/asset-property-updates/',
                                                    serde_info=glue.CfnTable.SerdeInfoProperty(
                                                        serialization_library='org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe',
                                                        parameters={
                                                            'serialization.format': '1'
                                                        }
                                                    )
                                                )
                                            )
                                        )

            # create Firehose Metadata Glue Table
            self.glue_metadata_table = glue.CfnTable(self, f'{id}GlueMetadataTable',
                                            catalog_id=Aws.ACCOUNT_ID,
                                            database_name=self.glue_database.ref,
                                            table_input=glue.CfnTable.TableInputProperty(
                                                description='IoT SiteWise Metadata',
                                                name=f'{prefix}_firehose_metadata_glue_table',
                                                table_type='EXTERNAL_TABLE',
                                                parameters={
                                                    'classification': 'json'
                                                },
                                                storage_descriptor=glue.CfnTable.StorageDescriptorProperty(
                                                    columns=[
                                                        glue.CfnTable.ColumnProperty(
                                                            name='asset_id',
                                                            type='string'
                                                        ),
                                                        glue.CfnTable.ColumnProperty(
                                                            name='asset_name',
                                                            type='string'
                                                        ),
                                                        glue.CfnTable.ColumnProperty(
                                                            name='asset_model_id',
                                                            type='string'
                                                        ),
                                                        glue.CfnTable.ColumnProperty(
                                                            name='asset_property_id',
                                                            type='string'
                                                        ),
                                                        glue.CfnTable.ColumnProperty(
                                                            name='asset_property_name',
                                                            type='string'
                                                        ),
                                                        glue.CfnTable.ColumnProperty(
                                                            name='asset_property_data_type',
                                                            type='string'
                                                        ),
                                                        glue.CfnTable.ColumnProperty(
                                                            name='asset_property_unit',
                                                            type='string'
                                                        ),
                                                        glue.CfnTable.ColumnProperty(
                                                            name='asset_property_alias',
                                                            type='string'
                                                        )
                                                    ],
                                                    input_format='org.apache.hadoop.mapred.TextInputFormat',
                                                    output_format='org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
                                                    location=f's3://{self.data_bucket.bucket_name}/asset-metadata/',
                                                    serde_info=glue.CfnTable.SerdeInfoProperty(
                                                        serialization_library='org.openx.data.jsonserde.JsonSerDe',
                                                        parameters={
                                                            'serialization.format': '1',
                                                            'paths': 'asset_id,asset_name,asset_model_id,asset_properties.asset_property_id,asset_properties.asset_property_name,asset_properties.asset_property_data_type,asset_properties.asset_property_unit,asset_properties.asset_property_alias'
                                                        }
                                                    )
                                                )
                                            )
                                        )
            
            # IoT SiteWise Export To S3 Metadata Function Role
            self.metadata_function_role = iam.Role(self, f'{id}MetadataFunctionRole',
                assumed_by=iam.ServicePrincipal('lambda.amazonaws.com'),
                role_name=f'{prefix}_sitewise_metadata_function_role'
            )
            self.metadata_function_role.add_to_policy(iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    'logs:CreateLogGroup',
                    'logs:CreateLogStream',
                    'logs:PutLogEvents'
                ],
                resources=[
                    f'arn:aws:logs:{Aws.REGION}:{Aws.ACCOUNT_ID}:*'
                ]
            ))
            self.metadata_function_role.add_to_policy(iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    's3:PutObject',
                    's3:GetBucketLocation',
                    's3:GetObject',
                    's3:ListBucket'

                ],
                resources=[
                    f'arn:aws:s3:::{self.data_bucket.bucket_name}/*',
                    f'arn:aws:s3:::{self.data_bucket.bucket_name}'
                ]
            ))
            self.metadata_function_role.add_to_policy(iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    'iotsitewise:Describe*',
                    'iotsitewise:List*',
                    'iotsitewise:Get*'
                ],
                resources=[
                    '*'
                ]
            ))
            



            # IoT SiteWise Export To S3 Metadata Function
            self.metadata_function = _lambda.Function(self, f'{id}MetadataFunction',
                function_name=f'{prefix}_sitewise_metadata_function',
                runtime=_lambda.Runtime.PYTHON_3_8,
                handler='asset_metadata_lambda.lambda_handler',
                code=_lambda.Code.from_asset(METADATA_LAMBDA_PATH),
                timeout=Duration.seconds(900),
                memory_size=128,
                role=self.metadata_function_role
            )
            
            # IoT SiteWise Export To S3 Metadata Scheduled Rule
            self.metadata_rule = events.Rule(self, f'{id}MetadataRule',
                rule_name=f'{prefix}_sitewise_metadata_rule',
                schedule=events.Schedule.rate(Duration.minutes(1)),
                targets=[
                    targets.LambdaFunction(self.metadata_function,
                        event=events.RuleTargetInput.from_object({
                            'bucket_name': self.data_bucket.bucket_name,
                            'key_name_prefix': 'asset-metadata'
                        })
                    )
                ],
                enabled=True,

            )
            # IoT SiteWise Export To S3 Metadata Event Lambda Permission
            self.metadata_function.add_permission(f'{id}MetadataFunctionPermission',
                principal=iam.ServicePrincipal('events.amazonaws.com'),
                action='lambda:InvokeFunction',
                source_arn=self.metadata_rule.rule_arn
            )



            # IoT SiteWise Export To S3 Firehose LogGroup
            self.firehose_log_group = logs.LogGroup(self, f'{id}FirehoseLogGroup',
                log_group_name=f'/aws/kinesisfirehose/{prefix}_firehose_delivery_stream',
                removal_policy=RemovalPolicy.DESTROY,
                retention=logs.RetentionDays.ONE_WEEK
            )

            # Log Stream for IoT SiteWise Export To S3 Firehose
            self.firehose_log_stream = logs.LogStream(self, f'{id}FirehoseLogStream',
                log_group=self.firehose_log_group,
                log_stream_name=f'{prefix}_firehose_log_stream'
            )

            # IAM Role for Transform Lambda
            self.firehose_transform_lambda_role = iam.Role(self, f'{id}FirehoseTransformLambdaRole',
                assumed_by=iam.ServicePrincipal('lambda.amazonaws.com'),
                role_name=f'{prefix}_firehose_transform_lambda_role'
            )
            self.firehose_transform_lambda_role.add_to_policy(iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    'logs:CreateLogGroup',
                    'logs:CreateLogStream',
                    'logs:PutLogEvents'
                ],
                resources=[
                    '*'
                ]
            ))

            
            # IoT SiteWise Export To S3 Transform Lambda Function
            self.firehose_transform_lambda = _lambda.Function(self, f'{id}FirehoseTransformLambda',
                function_name=f'{prefix}_firehose_transform_lambda',
                runtime=_lambda.Runtime.PYTHON_3_8,
                code=_lambda.Code.from_asset(TRANSFORM_LAMBDA_PATH),
                handler='index.handler',
                timeout=Duration.seconds(900),
                memory_size=128,
                role=self.firehose_transform_lambda_role,
                log_retention=logs.RetentionDays.ONE_WEEK
            )

            # IoT SiteWise Export To S3 Firehose IAM Role
            self.firehose_delivery_role = iam.CfnRole(self, f'{id}FirehoseRole',
                role_name=f'{prefix}_firehose_role',
                assume_role_policy_document={
                    'Version': '2012-10-17',
                    'Statement': [
                        {
                            'Effect': 'Allow',
                            'Principal': {
                                'Service': 'firehose.amazonaws.com'
                            },
                            'Action': 'sts:AssumeRole'
                        }
                    ]
                },
                policies=[iam.CfnRole.PolicyProperty(
                    policy_document={
                        'Version': '2012-10-17',
                        'Statement': [
                            {
                                'Effect': 'Allow',
                                'Action': [
                                    'glue:GetTable',
                                    'glue:GetTables',
                                    'glue:GetTableVersions'
                                ],
                                'Resource': [
                                    f'arn:aws:glue:{Aws.REGION}:{Aws.ACCOUNT_ID}:catalog',
                                    f'arn:aws:glue:{Aws.REGION}:{Aws.ACCOUNT_ID}:database/{self.glue_database.ref}',
                                    f'arn:aws:glue:{Aws.REGION}:{Aws.ACCOUNT_ID}:table/{self.glue_database.ref}/{self.glue_table.ref}'
                                ]
                            },
                            {
                                'Effect': 'Allow',
                                'Action': [
                                    's3:AbortMultipartUpload',
                                    's3:GetBucketLocation',
                                    's3:GetObject',
                                    's3:ListBucket',
                                    's3:ListBucketMultipartUploads',
                                    's3:PutObject'
                                ],
                                'Resource': [
                                    f'arn:aws:s3:::{self.data_bucket.bucket_name}/*',
                                    f'arn:aws:s3:::{self.data_bucket.bucket_name}'
                                ]
                            },
                            {
                                'Effect': 'Allow',
                                'Action': [
                                    'lambda:InvokeFunction',
                                    'lambda:GetFunctionConfiguration'
                                ],
                                'Resource': [
                                    self.firehose_transform_lambda.function_arn
                                ]
                            },
                            {
                                'Effect': 'Allow',
                                'Action': [
                                    'logs:PutLogEvents'
                                ],
                                'Resource': [
                                    f'arn:aws:logs:{Aws.REGION}:{Aws.ACCOUNT_ID}:*'
                                ]
                            }
                        ]
                    },
                    policy_name='firehose_delivery_policy'
                )]
            )

            # kinesis firehose delivery stream
            self.firehose_stream = firehose.CfnDeliveryStream(self, f'{id}FirehoseStream',
                delivery_stream_name=f'{prefix}_firehose_stream',
                delivery_stream_type='DirectPut',
                extended_s3_destination_configuration=firehose.CfnDeliveryStream.ExtendedS3DestinationConfigurationProperty(
                    role_arn=self.firehose_delivery_role.attr_arn,
                    bucket_arn=self.data_bucket.bucket_arn,
                    prefix='asset-property-updates/year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}/hour=!{timestamp:HH}/',
                    error_output_prefix='asset-property-errors/',
                    buffering_hints=firehose.CfnDeliveryStream.BufferingHintsProperty(
                        interval_in_seconds=60,
                        size_in_m_bs=64
                    ),
                    compression_format='UNCOMPRESSED',
                    cloud_watch_logging_options=firehose.CfnDeliveryStream.CloudWatchLoggingOptionsProperty(
                        enabled=True,
                        log_group_name=self.firehose_log_group.log_group_name,
                        log_stream_name=self.firehose_log_stream.log_stream_name
                    ),
                    processing_configuration=firehose.CfnDeliveryStream.ProcessingConfigurationProperty(
                        enabled=True,
                        processors=[
                            firehose.CfnDeliveryStream.ProcessorProperty(
                                type='Lambda',
                                parameters=[
                                    firehose.CfnDeliveryStream.ProcessorParameterProperty(
                                        parameter_name='LambdaArn',
                                        parameter_value=self.firehose_transform_lambda.function_arn
                                    ),
                                    firehose.CfnDeliveryStream.ProcessorParameterProperty(
                                        parameter_name='BufferSizeInMBs',
                                        parameter_value='3'
                                    ),
                                    firehose.CfnDeliveryStream.ProcessorParameterProperty(
                                        parameter_name='BufferIntervalInSeconds',
                                        parameter_value='60'
                                    )
                                ]
                            )
                        ]
                    ),
                    data_format_conversion_configuration=firehose.CfnDeliveryStream.DataFormatConversionConfigurationProperty(
                        enabled=True,
                        input_format_configuration=firehose.CfnDeliveryStream.InputFormatConfigurationProperty(
                            deserializer=firehose.CfnDeliveryStream.DeserializerProperty(
                                open_x_json_ser_de=firehose.CfnDeliveryStream.OpenXJsonSerDeProperty()
                            )
                        ),
                        output_format_configuration=firehose.CfnDeliveryStream.OutputFormatConfigurationProperty(
                            serializer=firehose.CfnDeliveryStream.SerializerProperty(
                                parquet_ser_de=firehose.CfnDeliveryStream.ParquetSerDeProperty()
                            )
                        ),
                        schema_configuration=firehose.CfnDeliveryStream.SchemaConfigurationProperty(
                            catalog_id=Aws.ACCOUNT_ID,
                            role_arn=self.firehose_delivery_role.attr_arn,
                            database_name=self.glue_database.ref,
                            region=Aws.REGION,
                            table_name=self.glue_table.ref,
                            version_id='LATEST'
                        )
                    )
                )
            )

            # IoT SiteWise Export To S3 Core Access To Firehose Role with self.firehose_put_record_policy attached
            self.core_topic_rule_role = iam.Role(self, f'{id}CoreFirehoseRole',
                assumed_by=iam.ServicePrincipal('iot.amazonaws.com'),
                role_name=f'{prefix}_core_topic_rule_role'
            )
            self.core_topic_rule_role.add_to_policy(iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=['firehose:PutRecord'],
                resources=[self.firehose_stream.attr_arn]
            ))
            # permission to CloudWatch Logs
            self.core_topic_rule_role.add_to_policy(iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    'logs:PutLogEvents',
                    'logs:CreateLogStream',
                    'logs:CreateLogGroup'
                ],
                resources=['*']
            ))

            # Topic Rule Error Action CloudWatch Logs
            self.core_log_group = logs.LogGroup(self, f'{id}CoreLogGroup',
                log_group_name=f'/aws/iot/topicrule/{prefix}_core_log_group',
                removal_policy=RemovalPolicy.DESTROY,
                retention=logs.RetentionDays.ONE_WEEK
            )
            # IoT SiteWise Export To S3 Core Rule
            self.topic_core_rule = iot.CfnTopicRule(self, f'{id}CoreRule',
                rule_name=f'{prefix}core_rule',
                topic_rule_payload=iot.CfnTopicRule.TopicRulePayloadProperty(
                    actions=[
                        iot.CfnTopicRule.ActionProperty(
                            firehose=iot.CfnTopicRule.FirehoseActionProperty(
                                delivery_stream_name=self.firehose_stream.delivery_stream_name,
                                role_arn=self.core_topic_rule_role.role_arn,
                                separator='\n'
                            )
                        )
                    ],
                    error_action=iot.CfnTopicRule.ActionProperty(
                        cloudwatch_logs=iot.CfnTopicRule.CloudwatchLogsActionProperty(
                            log_group_name=self.core_log_group.log_group_name,
                            role_arn=self.core_topic_rule_role.role_arn
                        )
                    ),
                    sql="SELECT * FROM '$aws/sitewise/asset-models/+/assets/+/properties/+' WHERE type = 'PropertyValueUpdate'"
                )
            )

            # Athena WorkGroup

            self.athena_workgroup = athena.CfnWorkGroup(self, f'{id}AthenaBucket',
                name=f'{prefix}_l4esitewisequery',
                recursive_delete_option=True,
                state='ENABLED',
                work_group_configuration=athena.CfnWorkGroup.WorkGroupConfigurationProperty(
                    publish_cloud_watch_metrics_enabled=True,
                    result_configuration=athena.CfnWorkGroup.ResultConfigurationProperty(
                        output_location=f's3://{self.data_bucket.bucket_name}/athenaquery/'
                    )
                )
            )

            # Athena Query Engine0
            self.athena_query_engine0 = athena.CfnNamedQuery(self, f'{id}AthenaQueryEngine0',
                database=self.glue_database.ref,
                description='IoT SiteWise Query Engine',
                name=f'{prefix}_l4esitewisequery_engine0',
                query_string=f'''CREATE OR REPLACE VIEW {prefix}_l4esitewisequery_engine0 AS 
                    SELECT "date_format"("date_trunc"('minute', "timestamp"), '%Y-%m-%dT%H:%i:%S.%f') "Timestamp"
                    , CAST("max"((CASE WHEN ("asset_property_name" = 'Sensor0') THEN "asset_property_value" ELSE null END)) AS double) "Sensor0"
                    , CAST("max"((CASE WHEN ("asset_property_name" = 'Sensor1') THEN "asset_property_value" ELSE null END)) AS double) "Sensor1"
                    , CAST("max"((CASE WHEN ("asset_property_name" = 'Sensor2') THEN "asset_property_value" ELSE null END)) AS double) "Sensor2"
                    , CAST("max"((CASE WHEN ("asset_property_name" = 'Sensor3') THEN "asset_property_value" ELSE null END)) AS double) "Sensor3"
                    , CAST("max"((CASE WHEN ("asset_property_name" = 'Sensor4') THEN "asset_property_value" ELSE null END)) AS double) "Sensor4"
                    , CAST("max"((CASE WHEN ("asset_property_name" = 'Sensor5') THEN "asset_property_value" ELSE null END)) AS double) "Sensor5"
                    , CAST("max"((CASE WHEN ("asset_property_name" = 'Sensor6') THEN "asset_property_value" ELSE null END)) AS double) "Sensor6"
                    , CAST("max"((CASE WHEN ("asset_property_name" = 'Sensor7') THEN "asset_property_value" ELSE null END)) AS double) "Sensor7"
                    , CAST("max"((CASE WHEN ("asset_property_name" = 'Sensor8') THEN "asset_property_value" ELSE null END)) AS double) "Sensor8"
                    , CAST("max"((CASE WHEN ("asset_property_name" = 'Sensor9') THEN "asset_property_value" ELSE null END)) AS double) "Sensor9"
                    , CAST("max"((CASE WHEN ("asset_property_name" = 'Sensor10') THEN "asset_property_value" ELSE null END)) AS double) "Sensor10"
                    , CAST("max"((CASE WHEN ("asset_property_name" = 'Sensor11') THEN "asset_property_value" ELSE null END)) AS double) "Sensor11"
                    , CAST("max"((CASE WHEN ("asset_property_name" = 'Sensor12') THEN "asset_property_value" ELSE null END)) AS double) "Sensor12"
                    , CAST("max"((CASE WHEN ("asset_property_name" = 'Sensor13') THEN "asset_property_value" ELSE null END)) AS double) "Sensor13"
                    , CAST("max"((CASE WHEN ("asset_property_name" = 'Sensor14') THEN "asset_property_value" ELSE null END)) AS double) "Sensor14"
                    , CAST("max"((CASE WHEN ("asset_property_name" = 'Sensor15') THEN "asset_property_value" ELSE null END)) AS double) "Sensor15"
                    , CAST("max"((CASE WHEN ("asset_property_name" = 'Sensor16') THEN "asset_property_value" ELSE null END)) AS double) "Sensor16"
                    , CAST("max"((CASE WHEN ("asset_property_name" = 'Sensor17') THEN "asset_property_value" ELSE null END)) AS double) "Sensor17"
                    , CAST("max"((CASE WHEN ("asset_property_name" = 'Sensor18') THEN "asset_property_value" ELSE null END)) AS double) "Sensor18"
                    , CAST("max"((CASE WHEN ("asset_property_name" = 'Sensor19') THEN "asset_property_value" ELSE null END)) AS double) "Sensor19"
                    , CAST("max"((CASE WHEN ("asset_property_name" = 'Sensor20') THEN "asset_property_value" ELSE null END)) AS double) "Sensor20"
                    , CAST("max"((CASE WHEN ("asset_property_name" = 'Sensor21') THEN "asset_property_value" ELSE null END)) AS double) "Sensor21"
                    , CAST("max"((CASE WHEN ("asset_property_name" = 'Sensor22') THEN "asset_property_value" ELSE null END)) AS double) "Sensor22"
                    , CAST("max"((CASE WHEN ("asset_property_name" = 'Sensor23') THEN "asset_property_value" ELSE null END)) AS double) "Sensor23"
                    , CAST("max"((CASE WHEN ("asset_property_name" = 'Sensor24') THEN "asset_property_value" ELSE null END)) AS double) "Sensor24"
                    , CAST("max"((CASE WHEN ("asset_property_name" = 'Sensor25') THEN "asset_property_value" ELSE null END)) AS double) "Sensor25"
                    , CAST("max"((CASE WHEN ("asset_property_name" = 'Sensor26') THEN "asset_property_value" ELSE null END)) AS double) "Sensor26"
                    , CAST("max"((CASE WHEN ("asset_property_name" = 'Sensor27') THEN "asset_property_value" ELSE null END)) AS double) "Sensor27"
                    , CAST("max"((CASE WHEN ("asset_property_name" = 'Sensor28') THEN "asset_property_value" ELSE null END)) AS double) "Sensor28"
                    , CAST("max"((CASE WHEN ("asset_property_name" = 'Sensor29') THEN "asset_property_value" ELSE null END)) AS double) "Sensor29"
                    FROM( 
                    SELECT "from_unixtime"(("time_in_seconds" + ("offset_in_nanos" / 1000000000))) "timestamp"
                    , "metadata"."asset_name", "metadata"."asset_property_name", "data"."asset_property_value"
                    , "metadata"."asset_property_unit", "metadata"."asset_property_alias"
                    FROM ({self.glue_database.ref}.{self.glue_table.ref} data
                    INNER JOIN {self.glue_database.ref}.{self.glue_metadata_table.ref} metadata ON (("data"."asset_id" = "metadata"."asset_id") AND ("data"."asset_property_id" = "metadata"."asset_property_id")))) 
                    WHERE (("timestamp" > ("date_trunc"('minute', current_timestamp) - INTERVAL  '6' MINUTE)) AND ("asset_name" = '{assets.engine_asset0_name}')) GROUP BY "timestamp"

                ''',
                work_group=self.athena_workgroup.name
            )

            # Athena Query Engine1
            self.athena_query_engine1 = athena.CfnNamedQuery(self, f'{id}AthenaQueryEngine1',
                database=self.glue_database.ref,
                description='IoT SiteWise Query Engine',
                name=f'{prefix}_l4esitewisequery_engine1',
                query_string=f'''CREATE OR REPLACE VIEW {prefix}_l4esitewisequery_engine1 AS 
                    SELECT "date_format"("date_trunc"('minute', "timestamp"), '%Y-%m-%dT%H:%i:%S.%f') "Timestamp"
                    , CAST("max"((CASE WHEN ("asset_property_name" = 'Sensor0') THEN "asset_property_value" ELSE null END)) AS double) "Sensor0"
                    , CAST("max"((CASE WHEN ("asset_property_name" = 'Sensor1') THEN "asset_property_value" ELSE null END)) AS double) "Sensor1"
                    , CAST("max"((CASE WHEN ("asset_property_name" = 'Sensor2') THEN "asset_property_value" ELSE null END)) AS double) "Sensor2"
                    , CAST("max"((CASE WHEN ("asset_property_name" = 'Sensor3') THEN "asset_property_value" ELSE null END)) AS double) "Sensor3"
                    , CAST("max"((CASE WHEN ("asset_property_name" = 'Sensor4') THEN "asset_property_value" ELSE null END)) AS double) "Sensor4"
                    , CAST("max"((CASE WHEN ("asset_property_name" = 'Sensor5') THEN "asset_property_value" ELSE null END)) AS double) "Sensor5"
                    , CAST("max"((CASE WHEN ("asset_property_name" = 'Sensor6') THEN "asset_property_value" ELSE null END)) AS double) "Sensor6"
                    , CAST("max"((CASE WHEN ("asset_property_name" = 'Sensor7') THEN "asset_property_value" ELSE null END)) AS double) "Sensor7"
                    , CAST("max"((CASE WHEN ("asset_property_name" = 'Sensor8') THEN "asset_property_value" ELSE null END)) AS double) "Sensor8"
                    , CAST("max"((CASE WHEN ("asset_property_name" = 'Sensor9') THEN "asset_property_value" ELSE null END)) AS double) "Sensor9"
                    , CAST("max"((CASE WHEN ("asset_property_name" = 'Sensor10') THEN "asset_property_value" ELSE null END)) AS double) "Sensor10"
                    , CAST("max"((CASE WHEN ("asset_property_name" = 'Sensor11') THEN "asset_property_value" ELSE null END)) AS double) "Sensor11"
                    , CAST("max"((CASE WHEN ("asset_property_name" = 'Sensor12') THEN "asset_property_value" ELSE null END)) AS double) "Sensor12"
                    , CAST("max"((CASE WHEN ("asset_property_name" = 'Sensor13') THEN "asset_property_value" ELSE null END)) AS double) "Sensor13"
                    , CAST("max"((CASE WHEN ("asset_property_name" = 'Sensor14') THEN "asset_property_value" ELSE null END)) AS double) "Sensor14"
                    , CAST("max"((CASE WHEN ("asset_property_name" = 'Sensor15') THEN "asset_property_value" ELSE null END)) AS double) "Sensor15"
                    , CAST("max"((CASE WHEN ("asset_property_name" = 'Sensor16') THEN "asset_property_value" ELSE null END)) AS double) "Sensor16"
                    , CAST("max"((CASE WHEN ("asset_property_name" = 'Sensor17') THEN "asset_property_value" ELSE null END)) AS double) "Sensor17"
                    , CAST("max"((CASE WHEN ("asset_property_name" = 'Sensor18') THEN "asset_property_value" ELSE null END)) AS double) "Sensor18"
                    , CAST("max"((CASE WHEN ("asset_property_name" = 'Sensor19') THEN "asset_property_value" ELSE null END)) AS double) "Sensor19"
                    , CAST("max"((CASE WHEN ("asset_property_name" = 'Sensor20') THEN "asset_property_value" ELSE null END)) AS double) "Sensor20"
                    , CAST("max"((CASE WHEN ("asset_property_name" = 'Sensor21') THEN "asset_property_value" ELSE null END)) AS double) "Sensor21"
                    , CAST("max"((CASE WHEN ("asset_property_name" = 'Sensor22') THEN "asset_property_value" ELSE null END)) AS double) "Sensor22"
                    , CAST("max"((CASE WHEN ("asset_property_name" = 'Sensor23') THEN "asset_property_value" ELSE null END)) AS double) "Sensor23"
                    , CAST("max"((CASE WHEN ("asset_property_name" = 'Sensor24') THEN "asset_property_value" ELSE null END)) AS double) "Sensor24"
                    , CAST("max"((CASE WHEN ("asset_property_name" = 'Sensor25') THEN "asset_property_value" ELSE null END)) AS double) "Sensor25"
                    , CAST("max"((CASE WHEN ("asset_property_name" = 'Sensor26') THEN "asset_property_value" ELSE null END)) AS double) "Sensor26"
                    , CAST("max"((CASE WHEN ("asset_property_name" = 'Sensor27') THEN "asset_property_value" ELSE null END)) AS double) "Sensor27"
                    , CAST("max"((CASE WHEN ("asset_property_name" = 'Sensor28') THEN "asset_property_value" ELSE null END)) AS double) "Sensor28"
                    , CAST("max"((CASE WHEN ("asset_property_name" = 'Sensor29') THEN "asset_property_value" ELSE null END)) AS double) "Sensor29"
                    FROM( 
                    SELECT "from_unixtime"(("time_in_seconds" + ("offset_in_nanos" / 1000000000))) "timestamp"
                    , "metadata"."asset_name", "metadata"."asset_property_name", "data"."asset_property_value"
                    , "metadata"."asset_property_unit", "metadata"."asset_property_alias"
                    FROM ({self.glue_database.ref}.{self.glue_table.ref} data
                    INNER JOIN {self.glue_database.ref}.{self.glue_metadata_table.ref} metadata ON (("data"."asset_id" = "metadata"."asset_id") AND ("data"."asset_property_id" = "metadata"."asset_property_id")))) 
                    WHERE (("timestamp" > ("date_trunc"('minute', current_timestamp) - INTERVAL  '6' MINUTE)) AND ("asset_name" = '{assets.engine_asset1_name}')) GROUP BY "timestamp"

                ''',
                work_group=self.athena_workgroup.name
            )

            # Inference Schedule Lambda Execution Role
            self.inference_schedule_lambda_execution_role = iam.Role(
                self,
                f'{id}InferenceScheduleLambdaExecutionRole',
                assumed_by=iam.ServicePrincipal('lambda.amazonaws.com'),
                managed_policies=[
                    iam.ManagedPolicy.from_aws_managed_policy_name('AmazonAthenaFullAccess'),
                    iam.ManagedPolicy.from_aws_managed_policy_name('AmazonS3FullAccess'),
                ]
            )
            self.inference_schedule_lambda_execution_role.add_to_policy(
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        'glue:*',
                        'logs:GetLogEvents',
                        'logs:PutLogEvents',
                        'logs:CreateLogGroup',
                        'logs:CreateLogStream',
                    ],
                    resources=['*']
                )
            )

            # Panda Layer from S3 bucket
            self.panda_layer_bucket='aws-data-wrangler-public-artifacts'
            self.panda_layer_key='releases/3.0.0/awswrangler-layer-3.0.0-py3.8.zip'
            self.panda_layer = _lambda.CfnLayerVersion(
                self,
                f'{id}PandaLayer',
                content=_lambda.CfnLayerVersion.ContentProperty(
                    s3_bucket=self.panda_layer_bucket,
                    s3_key=self.panda_layer_key
                ),
                layer_name=f'{id}PandaLayer',
                license_info='Apache-2.0'
            )

            # l4e inference schedule lambda
            self.inference_schedule_function_code = s3_assets.Asset(
                self,
                f'{id}InferenceScheduleFunctionCode',
                path=INFERENCE_SCHEDULE_LAMBDA_PATH
            )
            self.inference_schedule_lambda = _lambda.CfnFunction(
                self,
                f'{id}InferenceScheduleLambdaFunction',
                function_name=f'{prefix}_inference_schedule_lambda_function',
                runtime='python3.8',
                handler='lambda_function.lambda_handler',
                code=_lambda.CfnFunction.CodeProperty(
                    s3_bucket=self.inference_schedule_function_code.s3_bucket_name,
                    s3_key=self.inference_schedule_function_code.s3_object_key
                ),
                timeout=180,
                memory_size=128,
                role=self.inference_schedule_lambda_execution_role.role_arn,
                layers=[self.panda_layer.ref],
            )

            self.inference_schedule_lambda_permission0 = _lambda.CfnPermission(self,
                f'{id}InferenceScheduleLambdaPermission0',
                function_name=self.inference_schedule_lambda.function_name,
                action='lambda:InvokeFunction',
                principal='s3.amazonaws.com',
                source_account=Aws.ACCOUNT_ID,
                source_arn=f'arn:aws:s3:::{prefix}-l4e-bucket0'
            )
            self.inference_schedule_lambda_permission1 = _lambda.CfnPermission(self,
                f'{id}InferenceScheduleLambdaPermission1',
                function_name=self.inference_schedule_lambda.function_name,
                action='lambda:InvokeFunction',
                principal='s3.amazonaws.com',
                source_account=Aws.ACCOUNT_ID,
                source_arn=f'arn:aws:s3:::{prefix}-l4e-bucket1'
            )
            self.inference_schedule_lambda_permission0.add_dependency(self.inference_schedule_lambda)
            self.inference_schedule_lambda_permission1.add_dependency(self.inference_schedule_lambda)

            # Inference Schedule Lambda Execution Role
            self.l4e_to_sitewise_lambda_permission_role = iam.Role(
                self,
                f'{id}L4EToSitewiseLambdaPermissionRole',
                assumed_by=iam.ServicePrincipal('lambda.amazonaws.com'),
                managed_policies=[
                    iam.ManagedPolicy.from_aws_managed_policy_name('AmazonS3FullAccess'),
                    iam.ManagedPolicy.from_aws_managed_policy_name('AWSIoTSiteWiseFullAccess'),
                ]
            )
            self.l4e_to_sitewise_lambda_permission_role.add_to_policy(
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        'logs:GetLogEvents',
                        'logs:PutLogEvents',
                        'logs:CreateLogGroup',
                        'logs:CreateLogStream',
                    ],
                    resources=['*']
                )
            )

            # Lambda L4E to Sitewise
            self.l4e_to_sitewise_lambda = _lambda.Function(
                self,
                f'{id}L4EToSitewiseLambda',
                function_name=f'{prefix}_l4e_to_sitewise_lambda',
                runtime=_lambda.Runtime.PYTHON_3_8,
                handler="lambda_function.lambda_handler",
                code=_lambda.Code.from_asset(L4E_TO_SITEWISE_LAMBDA_PATH),
                timeout=Duration.seconds(180),
                memory_size=128,
                description="Lambda function to ingest data to IoT SiteWise asset",
                environment={
                    "Asset_L4E_Score": "AssetL4EScore",
                },
                role=self.l4e_to_sitewise_lambda_permission_role,
            )

            self.l4e_to_sitewise_lambda_permission0 = _lambda.CfnPermission(self,
                f'{id}L4EToSitewiseLambdaPermission0',
                function_name=self.l4e_to_sitewise_lambda.function_name,
                action='lambda:InvokeFunction',
                principal='s3.amazonaws.com',
                source_account=Aws.ACCOUNT_ID,
                source_arn=f'arn:aws:s3:::{prefix}-l4e-bucket0'
            )
            self.l4e_to_sitewise_lambda_permission1 = _lambda.CfnPermission(self,
                f'{id}L4EToSitewiseLambdaPermission1',
                function_name=self.l4e_to_sitewise_lambda.function_name,
                action='lambda:InvokeFunction',
                principal='s3.amazonaws.com',
                source_account=Aws.ACCOUNT_ID,
                source_arn=f'arn:aws:s3:::{prefix}-l4e-bucket1'
            )

            # L4e Bucket Engine0 with event notification
            self.l4e_bucket0 = s3.CfnBucket(
                self,
                f'{id}L4eBucket0',
                bucket_name=f'{prefix}-l4e-bucket0',
                public_access_block_configuration=s3.CfnBucket.PublicAccessBlockConfigurationProperty(
                    block_public_acls=True,
                    block_public_policy=True,
                    ignore_public_acls=True,
                    restrict_public_buckets=True
                ),
                notification_configuration=s3.CfnBucket.NotificationConfigurationProperty(
                    lambda_configurations=[
                        s3.CfnBucket.LambdaConfigurationProperty(
                            event='s3:ObjectCreated:Put',
                            filter=s3.CfnBucket.NotificationFilterProperty(
                                s3_key=s3.CfnBucket.S3KeyFilterProperty(
                                    rules=[
                                        s3.CfnBucket.FilterRuleProperty(
                                            name='suffix',
                                            value='.jsonl'
                                        ),
                                        s3.CfnBucket.FilterRuleProperty(
                                            name='prefix',
                                            value=f'{assets.engine_asset0.ref}/inference-data/output/'
                                        )
                                    ]
                                )
                            ),
                            function=self.l4e_to_sitewise_lambda.function_arn
                        ),
                                          
                    ]
                )
            )

            # L4e Bucket Engine1 with event notification
            self.l4e_bucket1 = s3.CfnBucket(
                self,
                f'{id}L4eBucket1',
                bucket_name=f'{prefix}-l4e-bucket1',
                public_access_block_configuration=s3.CfnBucket.PublicAccessBlockConfigurationProperty(
                    block_public_acls=True,
                    block_public_policy=True,
                    ignore_public_acls=True,
                    restrict_public_buckets=True
                ),
                notification_configuration=s3.CfnBucket.NotificationConfigurationProperty(
                    lambda_configurations=[
                        s3.CfnBucket.LambdaConfigurationProperty(
                            event='s3:ObjectCreated:Put',
                            filter=s3.CfnBucket.NotificationFilterProperty(
                                s3_key=s3.CfnBucket.S3KeyFilterProperty(
                                    rules=[
                                        s3.CfnBucket.FilterRuleProperty(
                                            name='suffix',
                                            value='.jsonl'
                                        ),
                                        s3.CfnBucket.FilterRuleProperty(
                                            name='prefix',
                                            value=f'{assets.engine_asset1.ref}/inference-data/output/'
                                        )
                                    ]
                                )
                            ),
                            function=self.l4e_to_sitewise_lambda.function_arn
                        )
                    ]
                )
            )


            self.l4e_bucket0.add_dependency(self.l4e_to_sitewise_lambda_permission0)
            self.l4e_bucket1.add_dependency(self.l4e_to_sitewise_lambda_permission1)

            # Inference Schedule Lambda Event Rule Engine0
            self.inference_schedule_lambda_event_rule0 = events.CfnRule(
                self,
                f'{id}InferenceScheduleLambdaEventRule0',
                name=f'{prefix}_inference_schedule_lambda_event_rule0',
                schedule_expression='rate(5 minutes)',
                state='ENABLED',
                targets=[
                    events.CfnRule.TargetProperty(
                        arn=self.inference_schedule_lambda.attr_arn,
                        id='Target0',
                        input=json.dumps({
                            'athena_output_bucket': self.data_bucket.bucket_name,
                            'view_query_id': self.athena_query_engine0.ref,
                            'l4e_bucket': self.l4e_bucket0.bucket_name,
                            'assetId': assets.engine_asset0.ref,
                            'work_group': self.athena_workgroup.name,
                            'database': self.glue_database.ref,
                            'asset_name': 'engine',
                        })
                    )
                ]
            )
            # Lambda Permission for Inference Schedule Lambda Event Rule Engine0
            self.inference_schedule_lambda_event_rule0_permission = _lambda.CfnPermission(
                self,
                f'{id}InferenceScheduleLambdaEventRule0Permission',
                action='lambda:InvokeFunction',
                function_name=self.inference_schedule_lambda.ref,
                principal='events.amazonaws.com',
                source_arn=self.inference_schedule_lambda_event_rule0.attr_arn
            )

            # Inference Schedule Lambda Event Rule Engine1
            self.inference_schedule_lambda_event_rule1 = events.CfnRule(
                self,
                f'{id}InferenceScheduleLambdaEventRule1',
                name=f'{prefix}_inference_schedule_lambda_event_rule1',
                schedule_expression='rate(5 minutes)',
                state='ENABLED',
                targets=[
                    events.CfnRule.TargetProperty(
                        arn=self.inference_schedule_lambda.attr_arn,
                        id='Target1',
                        input=json.dumps({
                            'athena_output_bucket': self.data_bucket.bucket_name,
                            'view_query_id': self.athena_query_engine1.ref,
                            'l4e_bucket': self.l4e_bucket1.bucket_name,
                            'assetId': assets.engine_asset1.ref,
                            'work_group': self.athena_workgroup.name,
                            'database': self.glue_database.ref,
                            'asset_name': 'engine',
                        })
                    )
                ]
            )

            # Lambda Permission for Inference Schedule Lambda Event Rule Engine1
            self.inference_schedule_lambda_event_rule1_permission = _lambda.CfnPermission(
                self,
                f'{id}InferenceScheduleLambdaEventRule1Permission',
                action='lambda:InvokeFunction',
                function_name=self.inference_schedule_lambda.ref,
                principal='events.amazonaws.com',
                source_arn=self.inference_schedule_lambda_event_rule1.attr_arn
            )
