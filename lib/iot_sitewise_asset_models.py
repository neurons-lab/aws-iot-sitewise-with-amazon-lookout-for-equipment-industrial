from aws_cdk import (
    aws_iotsitewise as iotsitewise,
)
from constructs import Construct

class VesselIoTSiteWiseAssetModel(Construct):

    def __init__(self, scope: Construct, id: str, *, prefix=None):
        super().__init__(scope, id)

        self.engine_asset_model = iotsitewise.CfnAssetModel(
            self, f'{id}EngineAssetModel',
            asset_model_name=f'{id}EngineAssetModel',
            asset_model_description="Engine Asset Model",
            asset_model_properties=[
                # Attribute properties
                iotsitewise.CfnAssetModel.AssetModelPropertyProperty(
                    name="Make",
                    data_type="STRING",
                    logical_id="MakeAttribute",
                    type=iotsitewise.CfnAssetModel.PropertyTypeProperty(
                        type_name="Attribute",
                        attribute=iotsitewise.CfnAssetModel.AttributeProperty(
                            default_value="Cummins"
                        )
                    )
                ),
                iotsitewise.CfnAssetModel.AssetModelPropertyProperty(
                    name="Model",
                    data_type="STRING",
                    logical_id="ModelAttribute",
                    type=iotsitewise.CfnAssetModel.PropertyTypeProperty(
                        type_name="Attribute",
                        attribute=iotsitewise.CfnAssetModel.AttributeProperty(
                            default_value="QSK60"
                        )
                    )
                ),
                iotsitewise.CfnAssetModel.AssetModelPropertyProperty(
                    name="SerialNumber",
                    data_type="STRING",
                    logical_id="SerialNumberAttribute",
                    type=iotsitewise.CfnAssetModel.PropertyTypeProperty(
                        type_name="Attribute",
                        attribute=iotsitewise.CfnAssetModel.AttributeProperty(
                            default_value="1234567890"
                        )
                    )
                ),
                iotsitewise.CfnAssetModel.AssetModelPropertyProperty(
                    name="Location",
                    data_type="STRING",
                    logical_id="LocationAttribute",
                    type=iotsitewise.CfnAssetModel.PropertyTypeProperty(
                        type_name="Attribute",
                        attribute=iotsitewise.CfnAssetModel.AttributeProperty(
                            default_value="Engine Room #1"
                        )
                    )
                ),
                iotsitewise.CfnAssetModel.AssetModelPropertyProperty(
                    name="L4E Alarm Threshold",
                    data_type="DOUBLE",
                    logical_id="L4EAlarmThresholdAttribute",
                    type=iotsitewise.CfnAssetModel.PropertyTypeProperty(
                        type_name="Attribute",
                        attribute=iotsitewise.CfnAssetModel.AttributeProperty(
                            default_value="0.9"
                        )
                    )
                ),
                # Mesurement properties Sensor0 - Sensor9
                iotsitewise.CfnAssetModel.AssetModelPropertyProperty(
                    name="Sensor0",
                    logical_id="Sensor0",
                    data_type="DOUBLE",
                    type=iotsitewise.CfnAssetModel.PropertyTypeProperty(
                        type_name="Measurement"
                    ),
                    unit="None"
                ),
                iotsitewise.CfnAssetModel.AssetModelPropertyProperty(
                    name="Sensor1",
                    logical_id="Sensor1",
                    data_type="DOUBLE",
                    type=iotsitewise.CfnAssetModel.PropertyTypeProperty(
                        type_name="Measurement"
                    ),
                    unit="None"
                ),
                iotsitewise.CfnAssetModel.AssetModelPropertyProperty(
                    name="Sensor2",
                    logical_id="Sensor2",
                    data_type="DOUBLE",
                    type=iotsitewise.CfnAssetModel.PropertyTypeProperty(
                        type_name="Measurement"
                    ),
                    unit="None"
                ),
                iotsitewise.CfnAssetModel.AssetModelPropertyProperty(
                    name="Sensor3",
                    logical_id="Sensor3",
                    data_type="DOUBLE",
                    type=iotsitewise.CfnAssetModel.PropertyTypeProperty(
                        type_name="Measurement"
                    ),
                    unit="None"
                ),
                iotsitewise.CfnAssetModel.AssetModelPropertyProperty(
                    name="Sensor4",
                    logical_id="Sensor4",
                    data_type="DOUBLE",
                    type=iotsitewise.CfnAssetModel.PropertyTypeProperty(
                        type_name="Measurement"
                    ),
                    unit="None"
                ),
                iotsitewise.CfnAssetModel.AssetModelPropertyProperty(
                    name="Sensor5",
                    logical_id="Sensor5",
                    data_type="DOUBLE",
                    type=iotsitewise.CfnAssetModel.PropertyTypeProperty(
                        type_name="Measurement"
                    ),
                    unit="None"
                ),
                iotsitewise.CfnAssetModel.AssetModelPropertyProperty(
                    name="Sensor6",
                    logical_id="Sensor6",
                    data_type="DOUBLE",
                    type=iotsitewise.CfnAssetModel.PropertyTypeProperty(
                        type_name="Measurement"
                    ),
                    unit="None"
                ),
                iotsitewise.CfnAssetModel.AssetModelPropertyProperty(
                    name="Sensor7",
                    logical_id="Sensor7",
                    data_type="DOUBLE",
                    type=iotsitewise.CfnAssetModel.PropertyTypeProperty(
                        type_name="Measurement"
                    ),
                    unit="None"
                ),
                iotsitewise.CfnAssetModel.AssetModelPropertyProperty(
                    name="Sensor8",
                    logical_id="Sensor8",
                    data_type="DOUBLE",
                    type=iotsitewise.CfnAssetModel.PropertyTypeProperty(
                        type_name="Measurement"
                    ),
                    unit="None"
                ),
                iotsitewise.CfnAssetModel.AssetModelPropertyProperty(
                    name="Sensor9",
                    logical_id="Sensor9",
                    data_type="DOUBLE",
                    type=iotsitewise.CfnAssetModel.PropertyTypeProperty(
                        type_name="Measurement"
                    ),
                    unit="None"
                ),
                iotsitewise.CfnAssetModel.AssetModelPropertyProperty(
                    name="Sensor10",
                    logical_id="Sensor10",
                    data_type="DOUBLE",
                    type=iotsitewise.CfnAssetModel.PropertyTypeProperty(
                        type_name="Measurement"
                    ),
                    unit="None"
                ),
                iotsitewise.CfnAssetModel.AssetModelPropertyProperty(
                    name="Sensor11",
                    logical_id="Sensor11",
                    data_type="DOUBLE",
                    type=iotsitewise.CfnAssetModel.PropertyTypeProperty(
                        type_name="Measurement"
                    ),
                    unit="None"
                ),
                iotsitewise.CfnAssetModel.AssetModelPropertyProperty(
                    name="Sensor12",
                    logical_id="Sensor12",
                    data_type="DOUBLE",
                    type=iotsitewise.CfnAssetModel.PropertyTypeProperty(
                        type_name="Measurement"
                    ),
                    unit="None",
                ),
                iotsitewise.CfnAssetModel.AssetModelPropertyProperty(
                    name="Sensor13",
                    logical_id="Sensor13",
                    data_type="DOUBLE",
                    type=iotsitewise.CfnAssetModel.PropertyTypeProperty(
                        type_name="Measurement"
                    ),
                    unit="None",
                ),
                iotsitewise.CfnAssetModel.AssetModelPropertyProperty(
                    name="Sensor14",
                    logical_id="Sensor14",
                    data_type="DOUBLE",
                    type=iotsitewise.CfnAssetModel.PropertyTypeProperty(
                        type_name="Measurement"
                    ),
                    unit="None",
                ),
                iotsitewise.CfnAssetModel.AssetModelPropertyProperty(
                    name="Sensor15",
                    logical_id="Sensor15",
                    data_type="DOUBLE",
                    type=iotsitewise.CfnAssetModel.PropertyTypeProperty(
                        type_name="Measurement"
                    ),
                    unit="None",
                ),
                iotsitewise.CfnAssetModel.AssetModelPropertyProperty(
                    name="Sensor16",
                    logical_id="Sensor16",
                    data_type="DOUBLE",
                    type=iotsitewise.CfnAssetModel.PropertyTypeProperty(
                        type_name="Measurement"
                    ),
                    unit="None",
                ),
                iotsitewise.CfnAssetModel.AssetModelPropertyProperty(
                    name="Sensor17",
                    logical_id="Sensor17",
                    data_type="DOUBLE",
                    type=iotsitewise.CfnAssetModel.PropertyTypeProperty(
                        type_name="Measurement"
                    ),
                    unit="None",
                ),
                iotsitewise.CfnAssetModel.AssetModelPropertyProperty(
                    name="Sensor18",
                    logical_id="Sensor18",
                    data_type="DOUBLE",
                    type=iotsitewise.CfnAssetModel.PropertyTypeProperty(
                        type_name="Measurement"
                    ),
                    unit="None",
                ),
                iotsitewise.CfnAssetModel.AssetModelPropertyProperty(    
                    name="Sensor19",
                    logical_id="Sensor19",
                    data_type="DOUBLE",
                    type=iotsitewise.CfnAssetModel.PropertyTypeProperty(
                        type_name="Measurement"
                    ),
                    unit="None",
                ),
                iotsitewise.CfnAssetModel.AssetModelPropertyProperty(
                    name="Sensor20",
                    logical_id="Sensor20",
                    data_type="DOUBLE",
                    type=iotsitewise.CfnAssetModel.PropertyTypeProperty(
                        type_name="Measurement"
                    ),
                    unit="None",
                ),
                iotsitewise.CfnAssetModel.AssetModelPropertyProperty(
                    name="Sensor21",
                    logical_id="Sensor21",
                    data_type="DOUBLE",
                    type=iotsitewise.CfnAssetModel.PropertyTypeProperty(
                        type_name="Measurement"
                    ),
                    unit="None",
                ),
                iotsitewise.CfnAssetModel.AssetModelPropertyProperty(
                    name="Sensor22",
                    logical_id="Sensor22",
                    data_type="DOUBLE",
                    type=iotsitewise.CfnAssetModel.PropertyTypeProperty(
                        type_name="Measurement"
                    ),
                    unit="None",
                ),
                iotsitewise.CfnAssetModel.AssetModelPropertyProperty(
                    name="Sensor23",
                    logical_id="Sensor23",
                    data_type="DOUBLE",
                    type=iotsitewise.CfnAssetModel.PropertyTypeProperty(
                        type_name="Measurement"
                    ),
                    unit="None",
                ),
                iotsitewise.CfnAssetModel.AssetModelPropertyProperty(
                    name="Sensor24",
                    logical_id="Sensor24",
                    data_type="DOUBLE",
                    type=iotsitewise.CfnAssetModel.PropertyTypeProperty(
                        type_name="Measurement"
                    ),
                    unit="None",
                ),
                iotsitewise.CfnAssetModel.AssetModelPropertyProperty(
                    name="Sensor25",
                    logical_id="Sensor25",
                    data_type="DOUBLE",
                    type=iotsitewise.CfnAssetModel.PropertyTypeProperty(
                        type_name="Measurement"
                    ),
                    unit="None",
                ),
                iotsitewise.CfnAssetModel.AssetModelPropertyProperty(
                    name="Sensor26",
                    logical_id="Sensor26",
                    data_type="DOUBLE",
                    type=iotsitewise.CfnAssetModel.PropertyTypeProperty(
                        type_name="Measurement"
                    ),
                    unit="None",
                ),
                iotsitewise.CfnAssetModel.AssetModelPropertyProperty(
                    name="Sensor27",
                    logical_id="Sensor27",
                    data_type="DOUBLE",
                    type=iotsitewise.CfnAssetModel.PropertyTypeProperty(
                        type_name="Measurement"
                    ),
                    unit="None",
                ),
                iotsitewise.CfnAssetModel.AssetModelPropertyProperty(
                    name="Sensor28",
                    logical_id="Sensor28",
                    data_type="DOUBLE",
                    type=iotsitewise.CfnAssetModel.PropertyTypeProperty(
                        type_name="Measurement"
                    ),
                    unit="None",
                ),
                iotsitewise.CfnAssetModel.AssetModelPropertyProperty(
                    name="Sensor29",
                    logical_id="Sensor29",
                    data_type="DOUBLE",
                    type=iotsitewise.CfnAssetModel.PropertyTypeProperty(
                        type_name="Measurement"
                    ),
                    unit="None",
                ),
                iotsitewise.CfnAssetModel.AssetModelPropertyProperty(
                    name="Sensor0 L4EScore",
                    logical_id="Sensor0L4EScore",
                    data_type="DOUBLE",
                    type=iotsitewise.CfnAssetModel.PropertyTypeProperty(
                        type_name="Measurement"
                    ),
                    unit="None",
                ),
                iotsitewise.CfnAssetModel.AssetModelPropertyProperty(
                    name="Sensor1 L4EScore",
                    logical_id="Sensor1L4EScore",
                    data_type="DOUBLE",
                    type=iotsitewise.CfnAssetModel.PropertyTypeProperty(
                        type_name="Measurement"
                    ),
                    unit="None",
                ),
                iotsitewise.CfnAssetModel.AssetModelPropertyProperty(
                    name="Sensor2 L4EScore",
                    logical_id="Sensor2L4EScore",
                    data_type="DOUBLE",
                    type=iotsitewise.CfnAssetModel.PropertyTypeProperty(
                        type_name="Measurement"
                    ),
                    unit="None",
                ),
                iotsitewise.CfnAssetModel.AssetModelPropertyProperty(
                    name="Sensor3 L4EScore",
                    logical_id="Sensor3L4EScore",
                    data_type="DOUBLE",
                    type=iotsitewise.CfnAssetModel.PropertyTypeProperty(
                        type_name="Measurement"
                    ),
                    unit="None",
                ),
                iotsitewise.CfnAssetModel.AssetModelPropertyProperty(
                    name="Sensor4 L4EScore",
                    logical_id="Sensor4L4EScore",
                    data_type="DOUBLE",
                    type=iotsitewise.CfnAssetModel.PropertyTypeProperty(
                        type_name="Measurement"
                    ),
                    unit="None"
                ),
                iotsitewise.CfnAssetModel.AssetModelPropertyProperty(
                    name="Sensor5 L4EScore",
                    logical_id="Sensor5L4EScore",
                    data_type="DOUBLE",
                    type=iotsitewise.CfnAssetModel.PropertyTypeProperty(
                        type_name="Measurement"
                    ),
                    unit="None"
                ),
                iotsitewise.CfnAssetModel.AssetModelPropertyProperty(
                    name="Sensor6 L4EScore",
                    logical_id="Sensor6L4EScore",
                    data_type="DOUBLE",
                    type=iotsitewise.CfnAssetModel.PropertyTypeProperty(
                        type_name="Measurement"
                    ),
                    unit="None"
                ),
                iotsitewise.CfnAssetModel.AssetModelPropertyProperty(
                    name="Sensor7 L4EScore",
                    logical_id="Sensor7L4EScore",
                    data_type="DOUBLE",
                    type=iotsitewise.CfnAssetModel.PropertyTypeProperty(
                        type_name="Measurement"
                    ),
                    unit="None"
                ),
                iotsitewise.CfnAssetModel.AssetModelPropertyProperty(
                    name="Sensor8 L4EScore",
                    logical_id="Sensor8L4EScore",
                    data_type="DOUBLE",
                    type=iotsitewise.CfnAssetModel.PropertyTypeProperty(
                        type_name="Measurement"
                    ),
                    unit="None"
                ),
                iotsitewise.CfnAssetModel.AssetModelPropertyProperty(
                    name="Sensor9 L4EScore",
                    logical_id="Sensor9L4EScore",
                    data_type="DOUBLE",
                    type=iotsitewise.CfnAssetModel.PropertyTypeProperty(
                        type_name="Measurement"
                    ),
                    unit="None"
                ),
                iotsitewise.CfnAssetModel.AssetModelPropertyProperty(
                    name="Sensor10 L4EScore",
                    logical_id="Sensor10L4EScore",
                    data_type="DOUBLE",
                    type=iotsitewise.CfnAssetModel.PropertyTypeProperty(
                        type_name="Measurement"
                    ),
                    unit="None"
                ),
                iotsitewise.CfnAssetModel.AssetModelPropertyProperty(
                    name="Sensor11 L4EScore",
                    logical_id="Sensor11L4EScore",
                    data_type="DOUBLE",
                    type=iotsitewise.CfnAssetModel.PropertyTypeProperty(
                        type_name="Measurement"
                    ),
                    unit="None"
                ),
                iotsitewise.CfnAssetModel.AssetModelPropertyProperty(
                    name="Sensor12 L4EScore",
                    logical_id="Sensor12L4EScore",
                    data_type="DOUBLE",
                    type=iotsitewise.CfnAssetModel.PropertyTypeProperty(
                        type_name="Measurement"
                    ),
                    unit="None"
                ),
                # Sensor13 L4EScore AssetModelProperty
                iotsitewise.CfnAssetModel.AssetModelPropertyProperty(
                    name="Sensor13 L4EScore",
                    logical_id="Sensor13L4EScore",
                    data_type="DOUBLE",
                    type=iotsitewise.CfnAssetModel.PropertyTypeProperty(
                        type_name="Measurement"
                    ),
                    unit="None"
                ),
                # Sensor14 L4EScore AssetModelProperty
                iotsitewise.CfnAssetModel.AssetModelPropertyProperty(
                    name="Sensor14 L4EScore",
                    logical_id="Sensor14L4EScore",
                    data_type="DOUBLE",
                    type=iotsitewise.CfnAssetModel.PropertyTypeProperty(
                        type_name="Measurement"
                    ),
                    unit="None"
                ),
                # Sensor15 L4EScore AssetModelProperty
                iotsitewise.CfnAssetModel.AssetModelPropertyProperty(
                    name="Sensor15 L4EScore",
                    logical_id="Sensor15L4EScore",
                    data_type="DOUBLE",
                    type=iotsitewise.CfnAssetModel.PropertyTypeProperty(
                        type_name="Measurement"
                    ),
                    unit="None"
                ),
                # Sensor16 L4EScore AssetModelProperty
                iotsitewise.CfnAssetModel.AssetModelPropertyProperty(
                    name="Sensor16 L4EScore",
                    logical_id="Sensor16L4EScore",
                    data_type="DOUBLE",
                    type=iotsitewise.CfnAssetModel.PropertyTypeProperty(
                        type_name="Measurement"
                    ),
                    unit="None"
                ),
                # Sensor17 L4EScore AssetModelProperty
                iotsitewise.CfnAssetModel.AssetModelPropertyProperty(
                    name="Sensor17 L4EScore",
                    logical_id="Sensor17L4EScore",
                    data_type="DOUBLE",
                    type=iotsitewise.CfnAssetModel.PropertyTypeProperty(
                        type_name="Measurement"
                    ),
                    unit="None"
                ),
                # Sensor18 L4EScore AssetModelProperty
                iotsitewise.CfnAssetModel.AssetModelPropertyProperty(
                    name="Sensor18 L4EScore",
                    logical_id="Sensor18L4EScore",
                    data_type="DOUBLE",
                    type=iotsitewise.CfnAssetModel.PropertyTypeProperty(
                        type_name="Measurement"
                    ),
                    unit="None"
                ),
                # Sensor19 L4EScore AssetModelProperty
                iotsitewise.CfnAssetModel.AssetModelPropertyProperty(
                    name="Sensor19 L4EScore",
                    logical_id="Sensor19L4EScore",
                    data_type="DOUBLE",
                    type=iotsitewise.CfnAssetModel.PropertyTypeProperty(
                        type_name="Measurement"
                    ),
                    unit="None"
                ),
                # Sensor20 L4EScore AssetModelProperty
                iotsitewise.CfnAssetModel.AssetModelPropertyProperty(
                    name="Sensor20 L4EScore",
                    logical_id="Sensor20L4EScore",
                    data_type="DOUBLE",
                    type=iotsitewise.CfnAssetModel.PropertyTypeProperty(
                        type_name="Measurement"
                    ),
                    unit="None"
                ),
                # Sensor21 L4EScore AssetModelProperty
                iotsitewise.CfnAssetModel.AssetModelPropertyProperty(
                    name="Sensor21 L4EScore",
                    logical_id="Sensor21L4EScore",
                    data_type="DOUBLE",
                    type=iotsitewise.CfnAssetModel.PropertyTypeProperty(
                        type_name="Measurement"
                    ),
                    unit="None"
                ),
                # Sensor22 L4EScore AssetModelProperty
                iotsitewise.CfnAssetModel.AssetModelPropertyProperty(
                    name="Sensor22 L4EScore",
                    logical_id="Sensor22L4EScore",
                    data_type="DOUBLE",
                    type=iotsitewise.CfnAssetModel.PropertyTypeProperty(
                        type_name="Measurement"
                    ),
                    unit="None"
                ),
                # Sensor23 L4EScore AssetModelProperty
                iotsitewise.CfnAssetModel.AssetModelPropertyProperty(
                    name="Sensor23 L4EScore",
                    logical_id="Sensor23L4EScore",
                    data_type="DOUBLE",
                    type=iotsitewise.CfnAssetModel.PropertyTypeProperty(
                        type_name="Measurement"
                    ),
                    unit="None"
                ),
                iotsitewise.CfnAssetModel.AssetModelPropertyProperty(
                    name="Sensor24 L4EScore",
                    logical_id="Sensor24L4EScore",
                    data_type="DOUBLE",
                    type=iotsitewise.CfnAssetModel.PropertyTypeProperty(
                        type_name="Measurement"
                    ),
                    unit="None"
                ),
                iotsitewise.CfnAssetModel.AssetModelPropertyProperty(
                    name="Sensor25 L4EScore",
                    logical_id="Sensor25L4EScore",
                    data_type="DOUBLE",
                    type=iotsitewise.CfnAssetModel.PropertyTypeProperty(
                        type_name="Measurement"
                    ),
                    unit="None"
                ),
                iotsitewise.CfnAssetModel.AssetModelPropertyProperty(
                    name="Sensor26 L4EScore",
                    logical_id="Sensor26L4EScore",
                    data_type="DOUBLE",
                    type=iotsitewise.CfnAssetModel.PropertyTypeProperty(
                        type_name="Measurement"
                    ),
                    unit="None"
                ),
                iotsitewise.CfnAssetModel.AssetModelPropertyProperty(
                    name="Sensor27 L4EScore",
                    logical_id="Sensor27L4EScore",
                    data_type="DOUBLE",
                    type=iotsitewise.CfnAssetModel.PropertyTypeProperty(
                        type_name="Measurement"
                    ),
                    unit="None"
                ),
                iotsitewise.CfnAssetModel.AssetModelPropertyProperty(
                    name="Sensor28 L4EScore",
                    logical_id="Sensor28L4EScore",
                    data_type="DOUBLE",
                    type=iotsitewise.CfnAssetModel.PropertyTypeProperty(
                        type_name="Measurement"
                    ),
                    unit="None"
                ),
                iotsitewise.CfnAssetModel.AssetModelPropertyProperty(
                    name="Sensor29 L4EScore",
                    logical_id="Sensor29L4EScore",
                    data_type="DOUBLE",
                    type=iotsitewise.CfnAssetModel.PropertyTypeProperty(
                        type_name="Measurement"
                    ),
                    unit="None"
                ),
                # Lookout for Equipment Score
                iotsitewise.CfnAssetModel.AssetModelPropertyProperty(
                    name="AssetL4EScore",
                    logical_id="AssetL4EScore",
                    data_type="DOUBLE",
                    type=iotsitewise.CfnAssetModel.PropertyTypeProperty(
                        type_name="Measurement"
                    ),
                    unit="None"
                ),
                # Metric properties
                iotsitewise.CfnAssetModel.AssetModelPropertyProperty(
                    name="AVG L4E Score",
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
                )
            ],
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
                    name="Total L4E Score",
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







