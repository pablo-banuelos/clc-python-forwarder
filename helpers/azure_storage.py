import logging
from azure.data.tables import TableServiceClient
from azure.core.exceptions import HttpResponseError
from config import STORAGE_CONNECTION_STRING, FILTERING_PARTITION_KEY, CURSOR_CHUNKS, FUNCTION_NAME

class AzureStorage():
    def __init__(self) -> None:
        self.filtering_table_name = FUNCTION_NAME.lower().replace("_", "")
        self.table_service_client = TableServiceClient.from_connection_string(STORAGE_CONNECTION_STRING)
        self.recovery_pair = None

    def check_or_create_table(self):
        try:
            self.table_service_client.create_table_if_not_exists(self.filtering_table_name)
        except HttpResponseError as ex:
            logging.error(f"Unable to create Table: {ex}")

    def get_latest_row_and_cursor_pair(self):
        try:
            entities = [entity for entity in self.table_service.query_entities(self.filtering_table_name, filter=f"PartitionKey eq '{FILTERING_PARTITION_KEY}'")]
            entities.sort(key=lambda x: int(x.RowKey), reverse=True)

            if not entities:
                filtering_next_row_key = None
            else:
                filtering_latest_row_key = entities[0].LatestRowNumber
                filtering_next_row_key = int(filtering_latest_row_key) + 1

            if filtering_next_row_key is None:
                latest_entity = entities[0]
            else:
                latest_entity = next((entity for entity in entities if entity.RowKey == str(filtering_next_row_key)), None)

            self.recovery_pair = latest_entity.RecoveryPair
            return [latest_entity.Cursor1, latest_entity.Cursor2]

        except Exception as e:
            logging.error('Unable to retrieve latest row or cursor: {e}')
            return None

    def divide_cursors(self, cursor_pair):
        start = cursor_pair[0]
        end = cursor_pair[1]

        step = (start - end) // (CURSOR_CHUNKS - 1)
        result = []
        for i in range(CURSOR_CHUNKS):
            if i == CURSOR_CHUNKS - 1:
                if end not in result[-1]:
                    result.append([start - i * step, end])
            else:
                result.append([start - i * step, start - (i + 1) * step])
        return result

    def execute(self):
        cursors = self.divide_cursors(
            self.get_latest_row_and_cursor_pair()
        )
        logging.info(f'Splited cursor pair into {len(cursors)} pairs: "{cursors}"')
        return cursors