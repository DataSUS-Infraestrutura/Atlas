import base64
import requests
import json 
from ..utils.Enums import HTTPMethod, HTTPStatus
from ..utils.Exception import AtlasServiceException
from .Lineage import LineageClient
from .Process import ProcessClient
from .Entity import EntityClient
import urllib.parse

class ApacheAtlasClient:

    def __init__(self, url: str, username: str, password) -> None:
        self.url = url
        self.username = username
        self.password = password

        self.generate_headers()
        self.generate_base_url()

        self.lineage = LineageClient(self)
        self.process = ProcessClient(self)
        self.entity =  EntityClient(self)

    def generate_headers(self) -> None:
        text = f"{self.username}:{self.password}"
        #encondig_string = base64.b64encode(
        #   text.encode('utf-8')
        #)

        self.headers = {
            "Content-Type": "application/json",
            "Authorization": "Basic YWRtaW46YWRtaW4="
        }

    def generate_base_url(self) -> None:
        self.BASE_URL = f"{self.url}/api/atlas"

    def request(self, url, method_http: HTTPMethod, body=None, params=None):

        if not body:
            body = {}

        full_url = f"{self.BASE_URL}{url}"

        if method_http == HTTPMethod.GET and params:
            encoded_params = urllib.parse.urlencode(params)  
            full_url = f"{full_url}?{encoded_params}"

        response = None

        if method_http == HTTPMethod.GET:
            response = requests.get(full_url, headers=self.headers, data=json.dumps(body)) 
        elif method_http == HTTPMethod.POST:
            response = requests.post(full_url, headers=self.headers, data=json.dumps(body))
        elif method_http == HTTPMethod.DELETE:
            response = requests.delete(full_url, headers=self.headers, data=json.dumps(body))
        else:
            response = requests.put(full_url, headers=self.headers, data=json.dumps(body))

        if response.status_code == HTTPStatus.OK:
            return response.json()

        raise AtlasServiceException(f"{response.status_code} - {response.text}")               

                

        
    
