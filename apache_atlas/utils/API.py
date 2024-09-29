
from functools import reduce
from urllib.parse import urlencode
import enum

class HTTPMethod(enum.Enum):
    GET = "GET"
    PUT = "PUT"
    POST = "POST"
    DELETE = "DELETE"
    
class HTTPStatus:
    OK = 200
    NO_CONTENT = 204
    SERVICE_UNAVAILABLE = 503


class API:
    def __init__(self, path: str, method: HTTPMethod):
        self.path = path
        self.method = method

    def format_path(self, params):
        return API(self.path.format(**params), self.method)
    
    def add_query_params(self):
        if self.params_url:
            query_string = urlencode(self.params_url)

            return API(
               f"{self.path}?{query_string}",
               method=self.method,
            )

        return self
    
