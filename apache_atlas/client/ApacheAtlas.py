import base64
import requests
import json 
from ..utils.API import HTTPMethod, HTTPStatus, API

class ApacheAtlasClient:

    def __init__(self, url: str, username: str, password) -> None:
        self.url = url
        self.username = username
        self.password = password

        self.generate_headers()
        self.generate_base_url()

        from .Lineage import LineageClient
        from .Process import ProcessClient
        from .Entity import EntityClient
        from .Search import SearchClient

        self.lineage = LineageClient(self)
        self.process = ProcessClient(self)
        self.entity =  EntityClient(self)
        self.search = SearchClient(self)

    def generate_headers(self) -> None:
        credentials = f"{self.username}:{self.password}"

        encode_b64 = base64.b64encode(
           credentials.encode('utf-8')
        )

        decode_string = encode_b64.decode('utf-8')

        self.headers = {
            "Content-Type": "application/json",
            "Authorization": f"Basic {decode_string}"
        }

    def generate_base_url(self) -> None:
        self.BASE_URL = f"{self.url}/api/atlas/v2"

    def request(self, api_instance: API, body_request=None):
        full_url = f"{self.BASE_URL}{api_instance.format_full_url()}"
        method_http = api_instance.method

        body = body_request if body_request else {}
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

        raise Exception(f"{response.status_code} - {response.text}")            

                

        
    
