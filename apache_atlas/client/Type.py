from ..utils.API import HTTPMethod, API
from ..utils.Exception import AtlasServiceException
from ..utils.Types import FileDO
from apache_atlas.client.ApacheAtlas import ApacheAtlasClient
import json

class TypeClient:

    TYPE_API = "/types/typedefs"
    CREATE_TYPE = API(TYPE_API, HTTPMethod.POST)
   
    def __init__(self, client: ApacheAtlasClient):
        self.client = client
    
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
