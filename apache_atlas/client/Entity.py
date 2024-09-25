from ..utils.Enums import HTTPMethod

class EntityClient:
    ENTITY_POST = "/v2/entity"
    ENTITY_GET = "/v2/entity/guid"
       
    def __init__(self, client):

        self.client = client

    def create_entity(self, entity):
        entity_body = { 
            "entity": entity
        }

        return self.client.request(
            url=self.ENTITY_POST,
            body=entity_body,
            method_http=HTTPMethod.POST
        )
        
    def get_entity(self, guid_entity):
        return self.client.request(
            url=f"{self.ENTITY_GET}/{guid_entity}",
            method_http=HTTPMethod.GET
        )
    
    