from aws_cdk import (
    Stack,
)
from constructs import Construct

from lib.iot_sitewise_assets import TwoEngineVesselIoTSiteWiseAsset
from lib.iot_sitewise_asset_models import VesselIoTSiteWiseAssetModel
from lib.iot_sitewise_ingest import DataIngestToIoTSiteWiseAsset
from lib.etl_pipeline import EtlPipeline
from lib.iot_sitewise_notebook import SiteWiseNotebook
from lib.l4e_setup import L4ESetup

def get_property_list(template):
    property_list = []
    for i in range(30):
        property_list.append(template.format(i))
    return property_list

class AwsAssetStack(Stack):

    def __init__(self,
                 scope: Construct,
                 construct_id: str,
                 **kwargs
                ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.property_list = get_property_list("Sensor{}")
        self.score_property_list = get_property_list("Sensor{}L4EScore")
        self.total_property_list = self.property_list + self.score_property_list

        asset_models = VesselIoTSiteWiseAssetModel(
            self, 
            "AssetModels",
            self.total_property_list
        )
        vessel_asset_model, engine_asset_model = \
            asset_models.vessel_asset_model, asset_models.engine_asset_model

        vessel_asset = TwoEngineVesselIoTSiteWiseAsset(self, "TwoEngineVesselIoTSiteWiseAsset",
            vessel_asset_model,
            engine_asset_model,
            property_list=self.total_property_list
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
            self.property_list,
            engine_asset_data
        )

        etl_pipeline = EtlPipeline(self, "EtlPipeline", assets=vessel_asset, prefix="etlpipeline",)

        # notebook = SiteWiseNotebook(self, "SiteWiseNotebook", prefix="sitewisenotebook")

        l4e_inference = L4ESetup(self, "L4ETrain", self.property_list, engine_asset_data, prefix="l4etrain")
