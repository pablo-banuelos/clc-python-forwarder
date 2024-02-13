import logging
import concurrent.futures
from config import FUNCTION_NAME
from helpers.azure_storage import AzureStorage
from helpers.azure_data_explorer import AzureDataExplorer
from helpers.data_process import DataProcess


def main():
    """
    Main function to execute the data processing pipeline.
    It initializes AzureStorage, AzureDataExplorer, and DataProcess objects,
    and then continuously processes data in a multi-threaded manner.
    """
    az_storage = AzureStorage()
    az_storage.check_or_create_table()
    adx = AzureDataExplorer()
    data_process = DataProcess()

    logging.info(f'Starting Forwarder "{FUNCTION_NAME}"')

    while True:
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = []
            cursors = az_storage.execute()
            for cursor_pair in cursors:
                futures.append(
                    executor.submit(
                        data_process.execute,
                        adx.query_adx(cursor_pair = cursor_pair)
                    )
                )

            for future in concurrent.futures.as_completed(futures):
                pass

            logging.info(f'Finished batch for "{FUNCTION_NAME}" and cursors "{cursors[0][0]}, {cursors[-1][1]}"')

if __name__ == "__main__":
    main()