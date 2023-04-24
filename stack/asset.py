
from aws_cdk import (
    Stack,
)
from constructs import Construct

from lib.iot_sitewise_vessel_asset import TwoEngineVesselIoTSiteWiseAsset
from lib.iot_sitewise_asset_models import VesselIoTSiteWiseAssetModel
from lib.iot_sitewise_ingest import DataIngestToIoTSiteWiseAsset

PROPERTY_LIST = ["Sensor0", "Sensor1", "Sensor2", "Sensor3", "Sensor4",
                 "Sensor5", "Sensor6", "Sensor7", "Sensor8", "Sensor9",
                 "Sensor10", "Sensor11", "Sensor12", "Sensor13", "Sensor14",
                 "Sensor15", "Sensor16", "Sensor17", "Sensor18", "Sensor19",
                 "Sensor20", "Sensor21", "Sensor22", "Sensor23", "Sensor24",
                 "Sensor25", "Sensor26", "Sensor27", "Sensor28", "Sensor29"]

class AwsAssetStack(Stack):

    def __init__(self,
                 scope: Construct,
                 construct_id: str,
                 **kwargs
                ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        asset_models = VesselIoTSiteWiseAssetModel(self, "AssetModels")
        vessel_asset_model, engine_asset_model = \
            asset_models.vessel_asset_model, asset_models.engine_asset_model

        vessel_asset = TwoEngineVesselIoTSiteWiseAsset(self, "TwoEngineVesselIoTSiteWiseAsset",
            vessel_asset_model,
            engine_asset_model
        )

        engine_asset_data = [
            {
                "asset_id": vessel_asset.engine_asset0.ref,
                "data_path": "data/EngineAsset0.txt"
            },
            {
                "asset_id": vessel_asset.engine_asset1.ref,
                "data_path": "data/EngineAsset1.txt"
            }
        ]

        ingestion_construct = DataIngestToIoTSiteWiseAsset(self, "DataIngestToIoTSiteWiseAsset",
            PROPERTY_LIST,
            engine_asset_data
        )
