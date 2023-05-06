import json
from aws_cdk import (
    RemovalPolicy,
    Aws,
    aws_iam as iam,
    aws_sagemaker as sagemaker,
)
from constructs import Construct

class SiteWiseNotebook(Construct):
        """Creates a SageMaker Notebook Instance with the following:"""
        def __init__(self, scope: Construct, id: str, *, prefix=None):
            super().__init__(scope, id)

            cfn_iam_role = iam.CfnRole(
                self, f'{id}SiteWiseNotebookRole',
                role_name=f'{prefix}-SiteWiseNotebookRole',
                assume_role_policy_document={
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Principal": {
                                "Service": "sagemaker.amazonaws.com"
                            },
                            "Action": "sts:AssumeRole"
                        }
                    ]
                },
                managed_policy_arns=[
                    "arn:aws:iam::aws:policy/AmazonSageMakerFullAccess",
                    "arn:aws:iam::aws:policy/AmazonS3FullAccess",
                    "arn:aws:iam::aws:policy/IAMFullAccess",
                    "arn:aws:iam::aws:policy/AmazonSSMFullAccess",
                    "arn:aws:iam::aws:policy/AmazonLookoutEquipmentFullAccess"
                ]
            )


            cfn_notebook_instance = sagemaker.CfnNotebookInstance(
                self, f'{id}SiteWiseNotebook',
                instance_type="ml.m5.xlarge",
                role_arn=cfn_iam_role.attr_arn,
                notebook_instance_name="SiteWiseNotebook",
                root_access="Enabled",
                volume_size_in_gb=10,
                default_code_repository="https://github.com/aws-samples/aws-iot-sitewise-with-amazon-lookout-for-equipment"
            )
                  



