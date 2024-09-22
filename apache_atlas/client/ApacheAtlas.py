import base64
import requests
import json 
from utils.Enums import HTTPMethod, HTTPStatus
from utils.Exception import AtlasServiceException

class ApacheAtlas:

    def __init__(self, url: str, username: str, password) -> None:
        self.url = url
        self.username = username
        self.password = password

        self.generate_headers()
        self.generate_base_url()

    def generate_headers(self) -> None:
        encondig_string = base64.b64encode(f"{self.username}:{self.password}")

        self.headers = {
            "Content-Type": "application/json",
            "Authorization": f"Basic {encondig_string}"
        }

    def generate_base_url(self) -> None:
        self.BASE_URL = f"{self.url}/api/atlas/v2"

    def request(self, url, body, params, method_http: HTTPMethod):
        full_url = f"{self.BASE_URL}{url}"
        response = None

        if method_http == HTTPMethod.GET:
            response = requests.get(full_url, headers=self.headers, body=json.dumps(body)) 
        elif method_http == HTTPMethod.POST:
            response = requests.post(full_url, headers=self.headers, body=json.dumps(body))
        elif method_http == HTTPMethod.DELETE:
            response = requests.delete(full_url, headers=self.headers, body=json.dumps(body))
        else:
            response = requests.put(full_url, headers=self.headers, body=json.dumps(body))

        if response.status_code == HTTPStatus.OK:
            return response.json()

        raise AtlasServiceException(f"[AtlasServiceError]: {response.status_code} - {response.text}")               

                

        
    
