import os
import logging
from concurrent_log_handler import ConcurrentRotatingFileHandler

logger = logging.getLogger()
logger.setLevel(logging.INFO)
rotateHandler = ConcurrentRotatingFileHandler('app.log', "a", 512*1024, 5)
rotateHandler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(rotateHandler)

logging.info(f'Loading environment variables"')

# Data processing
MAX_CHUNK_SIZE = 28 * 1024 * 1024
DATASET_SIZE = 42000

# Client
TENANT_ID = os.getenv('TENANTID')
SUBSCRIPTION_ID = os.getenv('SUBSCRIPTIONID')

# Azure Storage Account
STORAGE_CONNECTION_STRING = (
    f"DefaultEndpointsProtocol=https;"
    f"AccountName={os.getenv('STORAGEACCOUNTNAME')};"
    f"AccountKey={os.getenv('STORAGEACCOUNTKEY')};"
    f"EndpointSuffix=core.windows.net"
)
FILTERING_PARTITION_KEY = "FilteringPartitionKey"
CURSOR_CHUNKS = int(os.getenv('CURSOR_CHUNKS'))

# Azure Data Explorer
KUSTO_APP_ID = os.getenv('APPLICATIONID')
KUSTO_APP_SECRET = os.getenv('SERVICEPRINCIPALPASSWORD')
KUSTO_CLUSTER = os.getenv('CLUSTERNAME')
KUSTO_DB = os.getenv('DATABASENAME')
FUNCTION_NAME = os.getenv('FUNCTIONNAME')

# Log Analytics
WORKSPACE_ID = os.getenv('WORKSPACEID')
WORKSPACE_KEY = os.getenv('WORKSPACEKEY')
LOG_TYPE = os.getenv('LOGTYPE')