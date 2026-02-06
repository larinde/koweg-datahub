from datetime import datetime
from pydantic import BaseModel
from typing import (
    Optional,
    Annotated,
    Dict,
    List
)
from enum import Enum

class Checksum(BaseModel):
    algorithm: str
    hash: str

class MetaData(BaseModel):
    origin: str
    importDate: str
    dataAssetType: str
    dataAssetName: str
    dataAssetSize: str
    dataContext: str
    checksum: Checksum

class Owner(BaseModel):
    name: str
    organisation: str
    location: str

class AssetStatus(str, Enum):
    reviewing = 'UNDER_REVIEW'
    rejected = 'REJECTED'
    approved = 'APPROVED'

class DataAsset(BaseModel):
    id: str
    owner: Owner
    assetStatus: AssetStatus
    metaData: MetaData

class DataImportHistory(BaseModel):
    __root__: List[DataAsset]
