import base64
import hashlib
import hmac
import datetime
import requests
import logging
from config import WORKSPACE_ID, WORKSPACE_KEY, LOG_TYPE


class LogAnalyticsDataCollector():
    """
    A class to to Azure Log Analytics through the Data Collector API.
    Info: https://learn.microsoft.com/en-us/azure/azure-monitor/logs/data-collector-api
    """    
    def __init__(self) -> None:
        """
        Initializes the LogAnalyticsDataCollector class with workspace details and content type.
        """        
        self.workspace_id  = WORKSPACE_ID
        self.workspace_key = WORKSPACE_KEY
        self.resource = "/api/logs"
        self.url = f"https://{self.workspace_id}.ods.opinsights.azure.com{self.resource}?api-version=2016-04-01"
        self.log_type = LOG_TYPE
        self.content_type = "application/json"

    def build_signature(self, date, content_length, content_type) -> str:
        """
        Builds the authorization signature for Azure Log Analytics.

        Parameters:
        date (str): The date in RFC1123 format.
        content_length (int): The length of the content to be sent.
        content_type (str): The type of the content to be sent.

        Returns:
        str: The authorization signature.
        """        
        x_headers = f'x-ms-date:{date}'
        string_to_hash = f"POST\n{str(content_length)}\n{content_type}\n{x_headers}\n{self.resource}"
        bytes_to_hash = bytes(string_to_hash, encoding="utf-8")  
        decoded_key = base64.b64decode(self.workspace_key)
        encoded_hash = base64.b64encode(
            hmac.new(decoded_key, bytes_to_hash, digestmod=hashlib.sha256).digest()
        ).decode()
        authorization = f"SharedKey {self.workspace_id}:{encoded_hash}"
        return authorization

    def send_data(self, data, data_size) -> None:
        """
        Sends data to Azure Log Analytics.

        Parameters:
        data (str): The data to be sent as JSON string.
        data_size (int): The number of records in the JSON object.
        """        
        rfc1123date = datetime.datetime.utcnow().strftime('%a, %d %b %Y %H:%M:%S GMT')
        headers = {
            'content-type': self.content_type,
            'Authorization': self.build_signature(
                date = rfc1123date,
                content_length = len(data),
                content_type = self.content_type,
            ),
            'Log-Type': self.log_type,
            'x-ms-date': rfc1123date
        }

        response = requests.post(self.url, data=data, headers=headers)
        if not (response.status_code >= 200 and response.status_code <= 299):
            logging.error(f'Unable to send {data_size} events, status code: {response.status_code}')