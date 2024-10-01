from ..utils.API import HTTPMethod, API
from ..utils.Exception import AtlasServiceException
from ..utils.Types import FileDO
from apache_atlas.client.ApacheAtlas import ApacheAtlasClient

class EntityClient:

    ENTITY_API = "/entity/"

    CREATE_ENTITY = API(ENTITY_API, HTTPMethod.POST)
    GET_ENTITY = API(ENTITY_API + "guid/{guid}", HTTPMethod.GET)

    def __init__(self, client: ApacheAtlasClient):
        self.client = client

    def create_entity(self, entity):
        entity_body = { 
            "entity": entity
        }

        return self.client.request(
            self.CREATE_ENTITY,
            entity_body
        )
    
    def create_entity_file_table(self, data: FileDO, table_acronymus, table_column: str):
        entity_table = self.client.search.search_table_by_acronymus(table_acronymus.upper())

        if not entity_table:
            raise AtlasServiceException("Essa tabela não existe, ou sigla está errada")

        entity_column = self.client.search.search_annual_table(table_column)

        if not entity_column:
            raise AtlasServiceException(f"Esse ano ({data['table_column']}) não possui uma entidade ou nome está inválido")
        
        entity_file = {
            'typeName': 'dt_table_file',
            'attributes': data.update({ 'qualifiedName': f'dt_table_file.DataSUS@{data["name"]}' }),
            'is_file_table': {
                'guid': entity_table['guid']
            },
            'columns_file_table': {
                'guid': entity_column['guid']
            }   
        }

        return self.client.entity.create_entity(entity_file)


    def get_entity_by_guid(self, guid_entity):
        return self.client.request(
             api_instance=self.GET_ENTITY.format_path({ 'guid': guid_entity })
        )
    
    