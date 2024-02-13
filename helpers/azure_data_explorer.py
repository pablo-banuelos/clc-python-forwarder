import logging
from config import TENANT_ID, KUSTO_APP_ID, KUSTO_APP_SECRET, KUSTO_CLUSTER, KUSTO_DB, FUNCTION_NAME
from azure.kusto.data import KustoClient, KustoConnectionStringBuilder

class AzureDataExplorer():
    """
    A class to interact with Azure Data Explorer (ADX).
    It uses the KustoClient for querying data.
    """    
    def __init__(self) -> None:
        """
        Initializes the AzureDataExplorer class with authentication details and KustoClient.
        """        
        self.kcsb = KustoConnectionStringBuilder.with_aad_application_key_authentication(
            f'https://{KUSTO_CLUSTER}.kusto.windows.net',
            KUSTO_APP_ID,
            KUSTO_APP_SECRET,
            TENANT_ID
        )
        self.client = KustoClient(self.kcsb)

    def query_adx(self, cursor_pair):
        """
        Queries the ADX with a specific cursor pair and returns the result.
        Logs the function name and cursor pair being queried, and any errors encountered.

        Parameters:
        cursor_pair (list): A list containing two elements representing the start and end of the cursor range.

        Returns:
        KustoResultTable: A table that contains the primary results of the query.
        """        
        logging.info(f'Querying function "{FUNCTION_NAME}" with cursor pair "{cursor_pair}"')
        try:
            response = self.client.execute_query(
                database = KUSTO_DB,
                query = f"set notruncation; {FUNCTION_NAME}('{cursor_pair[0]}', '{cursor_pair[1]}')",
            )
            return response.primary_results[0]
        except Exception as ex:
            logging.error(f'Unable to retrieve "{FUNCTION_NAME}" with cursor pair "{cursor_pair}", error: {ex}')
