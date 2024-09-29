from ..utils.API import HTTPMethod, API
from apache_atlas.client.ApacheAtlas import ApacheAtlasClient

class EntityClient:

    ENTITY_API = "/entity/"

    CREATE_ENTITY = API(ENTITY_API, HTTPMethod.POST)
    GET_ENTITY = API(ENTITY_API + "{guid}", HTTPMethod.GET)

    def __init__(self, client: ApacheAtlasClient):
        self.client = client

    def create_entity(self, entity):
        entity_body = { 
            "entity": entity
        }

        return self.client.request(
            api_request=self.CREATE_ENTITY,
            body=entity_body
        )
        
    def get_entity_by_guid(self, guid_entity):
        return self.client.request(
             api_request=self.GET_ENTITY.format_path({ 'guid': guid_entity })
        )
    
    