import json
import logging
import polars as pl
import concurrent.futures
from config import MAX_CHUNK_SIZE, DATASET_SIZE, LOG_TYPE
from .log_analytics import LogAnalyticsDataCollector

class DataProcess():
    """
    A class to process data from Azure Data Explorer and send it to Azure Log Analytics.
    """    
    def __init__(self) -> None:
        """
        Initializes the DataProcess class with a LogAnalyticsDataCollector object.
        """        
        self.log_analytics = LogAnalyticsDataCollector()

    def send_chunk(self, chunk) -> None:
        """
        Sends a chunk of data to Azure Log Analytics.

        Parameters:
        chunk (dict): The chunk of data to be sent in JSON format.
        """        
        try:
            self.log_analytics.send_data(json.dumps(chunk), len(chunk))
        except Exception as e:
            logging.error(f"Error sending chunk: {e}")     

    def reduce_to_la_chunks(self, data) -> None:
        """
        Reduces the data into chunks of around 30MB and sends each chunk to Azure Log Analytics.

        Parameters:
        data (dict): The data to be reduced into chunks and sent.
        """        
        chunk = []
        chunk_size = 0
        futures = []

        with concurrent.futures.ThreadPoolExecutor() as executor:
            for i, item in enumerate(data):
                item_str = json.dumps(item)
                item_size = len(item_str.encode('utf-8'))

                if i >= 5000 and chunk_size + item_size > MAX_CHUNK_SIZE:
                    futures.append(
                        executor.submit(self.send_chunk, chunk)
                    )
                    chunk = [item]
                    chunk_size = item_size
                else:
                    chunk.append(item)
                    chunk_size += item_size

            if chunk:
                futures.append(
                    executor.submit(self.send_chunk, chunk)
                )

        for future in concurrent.futures.as_completed(futures):
            pass
            
    def dataframe_process(self, args) -> None:
        """
        Processes a dataframe and sends the processed data to be reduced to smaller chunks.

        Parameters:
        args (tuple): A tuple containing a chunk of data and the columns of the data.
        """        
        chunk, columns = args
        pl_df = pl.DataFrame(chunk, infer_schema_length=DATASET_SIZE, schema=columns)
        pl_to_json = pl_df.to_dicts()

        logging.info(f'Sending {len(pl_to_json)} events to table {LOG_TYPE} on LA')
        self.reduce_to_la_chunks(pl_to_json)

    def execute(self, adx_data) -> None:
        """
        Executes the data processing pipeline.

        Parameters:
        adx_data (KustoResultTable): The data from Azure Data Explorer to be processed.
        """        
        if adx_data:
            columns = [col['ColumnName'] for col in adx_data.raw_columns]
            chunks = (
                (adx_data.raw_rows[i:i + DATASET_SIZE], columns)
                for i in range(0, adx_data.rows_count, DATASET_SIZE)
            )

            with concurrent.futures.ProcessPoolExecutor() as executor:
                for _ in executor.map(self.dataframe_process, chunks):
                    pass