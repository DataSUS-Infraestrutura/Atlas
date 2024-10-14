from ..utils.API import HTTPMethod, API
from ..utils.Exception import AtlasServiceException
from ..utils.Types import FileDO
from apache_atlas.client.ApacheAtlas import ApacheAtlasClient
import json

class TypeClient:

    TYPE_API = "/types/typedefs"

    CREATE_TYPE = API(TYPE_API, HTTPMethod.POST)
    PUT_TYPE = API(TYPE_API, HTTPMethod.PUT)
    GET_TYPE_BY_NAME = API("/types/entitydef/name/{name}", HTTPMethod.GET)
   
    def __init__(self, client: ApacheAtlasClient):
        self.client = client
    

    def get_type_by_name(self, name: str):
        return self.client.request(
            self.GET_TYPE_BY_NAME.format_path({
                'name': name
            })
        )

    def create_type(self, types=[], enumsDefs = [], structDefs=[], classificationDefs=[], relationshipDefs=[]):
        payload = {
            "enumDefs": enumsDefs,
            "structDefs": structDefs,
            "classificationDefs": classificationDefs,
            "relationshipDefs": relationshipDefs,
            "entityDefs": types  
        }

        return self.client.request(
            self.CREATE_TYPE,
            body_request=payload
        )
    
    def put_type(self, types=[], enumsDefs = [], structDefs=[], classificationDefs=[], relationshipDefs=[]):
        payload = {
            "enumDefs": enumsDefs,
            "structDefs": structDefs,
            "classificationDefs": classificationDefs,
            "relationshipDefs": relationshipDefs,
            "entityDefs": types  
        }

        return self.client.request(
            self.PUT_TYPE,
            body_request=payload
        )