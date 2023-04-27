from aws_cdk import (
    aws_iotsitewise as iotsitewise,
)
from constructs import Construct

from lib.alert_model import AlertModel

class VesselIoTSiteWiseAssetModel(Construct):

    def __init__(self,
                scope: Construct,
                id: str,
                measurement_property_list: list,
                *,
                prefix=None):
        super().__init__(scope, id)

        self.engine_attribute_property_list = [
            {
                "name": "Make",
                "default_value": "VesselMaker"
            },
            {
                "name": "Model",
                "default_value": "VesselModel"
            },
            {
                "name": "SerialNumber",
                "default_value": "0123456789"
            },
            {
                "name": "L4EAlarmThreshold",
                "default_value": "0.9"
            }
        ]

        self.asset_model_properties = []
        for property in self.engine_attribute_property_list:
            self.asset_model_properties.append(
                iotsitewise.CfnAssetModel.AssetModelPropertyProperty(
                    name=property["name"],
                    logical_id=property["name"],
                    data_type="STRING",
                    type=iotsitewise.CfnAssetModel.PropertyTypeProperty(
                        type_name="Attribute",
                        attribute=iotsitewise.CfnAssetModel.AttributeProperty(
                            default_value=property["default_value"]
                        )
                    )
                )
            )
        for property in measurement_property_list:
            self.asset_model_properties.append(
                iotsitewise.CfnAssetModel.AssetModelPropertyProperty(
                    name=property,
                    logical_id=property,
                    data_type="DOUBLE",
                    type=iotsitewise.CfnAssetModel.PropertyTypeProperty(
                        type_name="Measurement"
                    ),
                    unit="None"
                )
            )

        self.asset_model_properties.append(iotsitewise.CfnAssetModel.AssetModelPropertyProperty(
            name="AssetL4EScore",
            logical_id="AssetL4EScore",
            data_type="DOUBLE",
            type=iotsitewise.CfnAssetModel.PropertyTypeProperty(
                type_name="Measurement"
            ),
            unit="None"
        ))
        self.asset_model_properties.append(iotsitewise.CfnAssetModel.AssetModelPropertyProperty(
            name="AVGL4EScore",
            logical_id="AVGL4EScore",
            data_type="DOUBLE",
            type=iotsitewise.CfnAssetModel.PropertyTypeProperty(
                type_name="Metric",
                metric=iotsitewise.CfnAssetModel.MetricProperty(
                    expression="AVG(score)",
                    # variable L4E Score with value of asset property Asset L4E Score
                    variables=[iotsitewise.CfnAssetModel.ExpressionVariableProperty(
                        name="score",
                        value=iotsitewise.CfnAssetModel.VariableValueProperty(
                            property_logical_id="AssetL4EScore"
                        )
                    )],
                    window=iotsitewise.CfnAssetModel.MetricWindowProperty(
                        tumbling=iotsitewise.CfnAssetModel.TumblingWindowProperty(
                            interval="5m"
                        )
                    )
                )
            )
        ))

        self.engine_asset_model = iotsitewise.CfnAssetModel(
            self, f'{id}EngineAssetModel',
            asset_model_name=f'{id}EngineAssetModel',
            asset_model_description="Engine Asset Model",
            asset_model_properties=self.asset_model_properties,
            asset_model_composite_models=[iotsitewise.CfnAssetModel.AssetModelCompositeModelProperty(
                    name="l4eAlarm",
                    type="AWS/ALARM",
                    composite_model_properties=[
                        iotsitewise.CfnAssetModel.AssetModelPropertyProperty(
                            name="AWS/ALARM_TYPE",
                            logical_id="AWS/ALARM_TYPE",
                            data_type="STRING",
                            type=iotsitewise.CfnAssetModel.PropertyTypeProperty(
                                type_name="Attribute",
                                attribute=iotsitewise.CfnAssetModel.AttributeProperty(
                                    default_value="IOT_EVENTS"
                                )
                            )
                        ),
                        iotsitewise.CfnAssetModel.AssetModelPropertyProperty(
                            name="AWS/ALARM_STATE",
                            logical_id="AWS/ALARM_STATE",
                            data_type="STRUCT",
                            type=iotsitewise.CfnAssetModel.PropertyTypeProperty(
                                type_name="Measurement"
                            ),
                            data_type_spec="AWS/ALARM_STATE"
                        )
                    ]
                )
            ]
        )
        self.vessel_asset_model = iotsitewise.CfnAssetModel(
            self, f'{id}VesselAssetModel',
            asset_model_name=f'{id}VesselAssetModel',
            asset_model_description="Vessel Asset Model",
            asset_model_hierarchies=[
                iotsitewise.CfnAssetModel.AssetModelHierarchyProperty(
                logical_id="EngineHierarchy",
                name="Engine",
                child_asset_model_id=self.engine_asset_model.ref
            )],
            asset_model_properties=[
                # Attribute properties
                iotsitewise.CfnAssetModel.AssetModelPropertyProperty(
                    name="ReliabilityManager",
                    data_type="STRING",
                    logical_id="ReliabilityManagerAttribute",
                    type=iotsitewise.CfnAssetModel.PropertyTypeProperty(
                        type_name="Attribute",
                        attribute=iotsitewise.CfnAssetModel.AttributeProperty(
                            default_value="John Doe"
                        )
                    )
                ),
                iotsitewise.CfnAssetModel.AssetModelPropertyProperty(
                    name="Code",
                    data_type="STRING",
                    logical_id="CodeAttribute",
                    type=iotsitewise.CfnAssetModel.PropertyTypeProperty(
                        type_name="Attribute",
                        attribute=iotsitewise.CfnAssetModel.AttributeProperty(
                            default_value="1234567890"
                        )
                    )
                ),
                iotsitewise.CfnAssetModel.AssetModelPropertyProperty(
                    name="CountryOfRegistration",
                    data_type="STRING",
                    logical_id="CountryOfRegistrationAttribute",
                    type=iotsitewise.CfnAssetModel.PropertyTypeProperty(
                        type_name="Attribute",
                        attribute=iotsitewise.CfnAssetModel.AttributeProperty(
                            default_value="USA"
                        )
                    )
                ),
                iotsitewise.CfnAssetModel.AssetModelPropertyProperty(
                    name="VesselAbnormalyThreshold",
                    data_type="DOUBLE",
                    logical_id="VesselAbnormalyThresholdAttribute",
                    type=iotsitewise.CfnAssetModel.PropertyTypeProperty(
                        type_name="Attribute",
                        attribute=iotsitewise.CfnAssetModel.AttributeProperty(
                            default_value="1.8"
                        )
                    )
                ),
                # Metric properties
                iotsitewise.CfnAssetModel.AssetModelPropertyProperty(
                    name="TotalL4EScore",
                    logical_id="TotalL4EScore",
                    data_type="DOUBLE",
                    type=iotsitewise.CfnAssetModel.PropertyTypeProperty(
                        type_name="Metric",
                        metric=iotsitewise.CfnAssetModel.MetricProperty(
                            expression="SUM(score)",
                            # variable L4E Score with value of asset property Asset L4E Score
                            variables=[iotsitewise.CfnAssetModel.ExpressionVariableProperty(
                                name="score",
                                # AVG L4E Score for Engine variable
                                value=iotsitewise.CfnAssetModel.VariableValueProperty(
                                    property_logical_id="AVGL4EScore",
                                    hierarchy_logical_id="EngineHierarchy"
                                )
                            )],
                            window=iotsitewise.CfnAssetModel.MetricWindowProperty(
                                tumbling=iotsitewise.CfnAssetModel.TumblingWindowProperty(
                                    interval="5m"
                                )
                            )
                        )
                    )
                )
            ]
        )

        self.alert_model = AlertModel(self, id="EngineAlertModel")
        self.alert_model.create_alert_model("EngineAnomalyAlert", self.engine_asset_model.ref, "AVGL4EScore", "L4EAlarmThreshold", "AWS/ALARM_STATE")







