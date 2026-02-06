'''
https://learn.microsoft.com/en-us/azure/storage/blobs/storage-blob-container-properties-metadata-python
https://learn.microsoft.com/en-us/azure/storage/common/storage-samples-python?toc=%2Fazure%2Fstorage%2Fblobs%2Ftoc.json#blob-samples


'''

from azure.storage.blob import(
    BlobServiceClient,
    BlobClient,
    ContainerClient
)
from azure.identity import DefaultAzureCredential
from azure.core.exceptions import(
    AzureError,
    ResourceExistsError
)

import os
import logging as log

class IngestionService:
    
    def __init__(self) -> None:
        # --------------- Storage Initialisation ------------------
        self.KOWEG_AZURE_STORAGE_ACCOUNT_KEY = os.getenv('KOWEG_AZURE_STORAGE_ACCOUNT_KEY', default = 'Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==')
        self.MOCK_DATALAKE_CONNECTION_STRING = 'DefaultEndpointsProtocol=http;'.__add__ ('AccountName=devstoreaccount1;').__add__('AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;').__add__('BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;').__add__('QueueEndpoint=http://127.0.0.1:10001/devstoreaccount1;').__add__('TableEndpoint=http://127.0.0.1:10002/devstoreaccount1;')
        self.KOWEG_DATALAKE_CONNECTION_STRING = os.getenv('KOWEG_DATALAKE_CONNECTION_STRING', default = self.MOCK_DATALAKE_CONNECTION_STRING)
        self.KOWEG_DATALAKE_STAGED_DATA_CONTAINER = os.getenv('KOWEG_DATALAKE_STAGED_DATA_CONTAINER', default = 'staging')
        self.KOWEG_DATALAKE_VALIDATED_DATA_CONTAINER = os.getenv('KOWEG_DATALAKE_VALIDATED_DATA_CONTAINER', default = 'raw')
        self.blobServiceClient = BlobServiceClient.from_connection_string(self.KOWEG_DATALAKE_CONNECTION_STRING)

    def persist_dataset(self, data_payload: bytes, data_name: str, metadata_payload: bytes, metadata_name: str, asset_categorisation: str):
        data_security_classification = 'INTERNAL'
        data_approval = 'PENDING'
        log.info('>>>>>>>>>>>>> persisting %s', data_name)
        containerClient = self.blobServiceClient.get_container_client(container = self.KOWEG_DATALAKE_VALIDATED_DATA_CONTAINER)

        if not containerClient.exists():
            containerClient.create_container()
        try:

            containerClient.upload_blob(name = data_name, data = data_payload, overwrite=False)
            blobClient = self.blobServiceClient.get_blob_client(container=self.KOWEG_DATALAKE_VALIDATED_DATA_CONTAINER,blob=data_name)
            blobMetadata = blobClient.get_blob_properties().metadata
            blobMetadata.update({'data_security_classification': data_security_classification, 
                                 'data_category': asset_categorisation, 
                                 'data_dictionary_filename': metadata_name, 
                                 'data_approval': data_approval})
            blobClient.set_blob_metadata(blobMetadata)
            
            containerClient.upload_blob(name = metadata_name, data = metadata_payload, overwrite=False)
            blobClient = self.blobServiceClient.get_blob_client(container=self.KOWEG_DATALAKE_VALIDATED_DATA_CONTAINER,blob=metadata_name)
            blobMetadata = blobClient.get_blob_properties().metadata
            blobMetadata.update({'data_security_classification': data_security_classification, 
                                 'data_category': asset_categorisation, 
                                 'data_approval': data_approval})
            blobClient.set_blob_metadata(blobMetadata)
            
        except ResourceExistsError:
            log.warn('DUPLICATE -> %s has previously been uploaded', data_name)
            return f'{data_name} already exists'
        return f'{data_name} successfully uploaded '
