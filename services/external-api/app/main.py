"""
http://127.0.0.1:7070/docs
http://127.0.0.1:7070/api/koweg/datahub/health

testing:
hypercorn ./services/external-api/app/main:app --reload --bind '127.0.0.1:7070'


"""

from fastapi import FastAPI, HTTPException, UploadFile
from fastapi.openapi.utils import get_openapi
from fastapi.responses import (
    UJSONResponse
)
from fastapi.staticfiles import StaticFiles
from datetime import datetime
from pydantic import BaseModel
from typing import (
    Optional,
    Annotated,
    Dict,
    List
)
from enum import Enum
from azure.storage.blob import(
    BlobServiceClient
)
from azure.identity import DefaultAzureCredential
from azure.core.exceptions import(
    ResourceExistsError
)

import os
import logging as log

from .ingestion_service import IngestionService
from .api_models import DataAsset, DataImportHistory, Checksum, MetaData, Owner

API_TITLE = 'Koweg Data API (Public)'
API_VERSION = '1.0.0'
API_DESCRIPTION = 'Koweg External Data Hub API'
API_LOGO = '/static/images/koweg.png'

HTTP_CODE_ACCEPTED = '202'
HTTP_CODE_NOT_FOUND = '404'

def custom_openapi():
    if app.openapi_schema:
        return app.openapi_schema
    openapi_schema = get_openapi(
        title = API_TITLE,
        version = API_VERSION,
        description = API_DESCRIPTION,
        routes=app.routes,
    )
    openapi_schema['info']['x-logo'] = {
        'url': API_LOGO
    }
    app.openapi_schema = openapi_schema
    return app.openapi_schema

app = FastAPI(title=API_TITLE)
app.mount('/static', StaticFiles(directory='static'), name='static')
app.openapi = custom_openapi


IngestionService = IngestionService()

# ------------------------ Mock Data -----------------------
mock_data_store = {
    "1": DataAsset(
        id="1",
        assetStatus="APPROVED",
        owner=Owner(name="Olarinde Ajai", organisation="Koweg", location="London UK"),
        metaData=MetaData(
            origin="Interactive Investor, UK",
            importDate=datetime.today().strftime("%Y-%m-%d"),
            dataAssetType="csv",
            dataAssetName="sipp-snapshot-00-13-05-02-2026-87a8528f-5971-4fdf-80ee-9d995697f13f.csv",
            dataAssetSize="108970",
            dataContext="SIPP Account Snapshot",
            checksum=Checksum(
                algorithm="SHA256",
                hash="2839af5a8e59f605f610e201a5c3c150fed1d0f41c8e7b9a0e5f2c8d9b4e5a",
            ),
        ),
    ),
    "2": DataAsset(
        id="2",
        assetStatus="UNDER_REVIEW",
        owner=Owner(
            name="Max Mustermann",
            organisation="Mustermann AG",
            location="Frankfurt, DE",
        ),
        metaData=MetaData(
            origin="Charles Schwab",
            importDate=datetime.today().strftime("%Y-%m-%d"),
            dataAssetType="csv",
            dataAssetName="trading_portfolio.csv",
            dataAssetSize="108977",
            dataContext="Trade Portfolio Summary",
            checksum=Checksum(
                algorithm="SHA256",
                hash="afa1588df1c975bd3491a5c649fbceeab7e747f2217252420e212efaedfa561e",
            ),
        ),
    ),
}
def mock_data_repository(id: str):
    return mock_data_store[id]

def mock_data_assets():
    return DataImportHistory(__root__=list(mock_data_store.values()))

# --------------- Storage Initialisation ------------------
KOWEG_AZURITE_HOST_IP = os.getenv('KOWEG_AZURITE_HOST_IP', 'azurite')
MOCK_DATALAKE_CONNECTION_STRING = 'DefaultEndpointsProtocol=http;'.__add__ ('AccountName=devstoreaccount1;').__add__('AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;').__add__('BlobEndpoint=http://azurite:10000/devstoreaccount1;').__add__('QueueEndpoint=http://azurite:10001/devstoreaccount1;').__add__('TableEndpoint=http://azurite:10002/devstoreaccount1;')
KOWEG_DATALAKE_CONNECTION_STRING = os.getenv('KOWEG_DATALAKE_CONNECTION_STRING', default = MOCK_DATALAKE_CONNECTION_STRING)
KOWEG_DATALAKE_STAGED_DATA_CONTAINER = os.getenv('KOWEG_DATALAKE_STAGED_DATA_CONTAINER', default = 'staging')
KOWEG_DATALAKE_VALIDATED_DATA_CONTAINER = os.getenv('KOWEG_DATALAKE_VALIDATED_DATA_CONTAINER', default = 'raw')

def retrieve_dataset(id):
    log.warn('TODO :: retrieve dataset from storage')


def storage_health_check():
    log.warn('TODO :: data ingestion ')
    blob_service_client = BlobServiceClient.from_connection_string(KOWEG_DATALAKE_CONNECTION_STRING)
    return blob_service_client.get_account_information()

def list_containers():
    blob_service_client = BlobServiceClient.from_connection_string(KOWEG_DATALAKE_CONNECTION_STRING)
    containers = blob_service_client.list_containers(include_metadata=True)
    for container in containers:
        log.info('container details: %s,  %s ', container['name'], container['metadata'])

# ---------- Endpoints -------------------

@app.get('/api/koweg/datahub/health', include_in_schema=False )
async def health_check():
    return {'status': 'OK', 'time': datetime.now().strftime('%Y-%m-%d, %H:%M:%S'), 'Storage Status': storage_health_check()} 

@app.get('/api/koweg/datahub/assets', response_description='Data asset history', response_model=DataImportHistory)
async def get__data_asset_import_history() -> DataImportHistory:
    """
    Retrieves a list of imported data assets
    """
    return mock_data_assets()

@app.get('/api/koweg/datahub/{id}')
async def get_asset_by_Id(id: str) -> DataAsset:
    result = mock_data_repository(id)
    if not result:
        log.warn('%s is an invalid identifier', id)
        raise HTTPException(status_code=HTTP_CODE_NOT_FOUND, detail= 'invalid identifier' )
    return result

@app.post('/api/koweg/datahub/assets', response_class=UJSONResponse, status_code=202)
async def import_data_asset(asset: UploadFile, assetMetadata: UploadFile, assetCategorisation: str):
    '''
    Uploads data set to data platform.<br/> 
    The platform makes a 'best effort' attempt to fully automate data ingestion processes.<br/> 
    Any failure will trigger a workflow that might require the four-eye principle in resolving data quality issues. 
    '''
    result = IngestionService.persist_dataset(data_name=asset.filename, data_payload=asset.file, metadata_payload=assetMetadata.file, metadata_name=assetMetadata.filename, asset_categorisation=assetCategorisation )
    return {'status' : HTTP_CODE_ACCEPTED, 'message': result }
