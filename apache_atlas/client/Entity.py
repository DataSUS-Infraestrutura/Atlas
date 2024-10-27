from ..utils.API import HTTPMethod, API
from ..utils.Exception import AtlasServiceException
from ..utils.Types import *
from apache_atlas.client.ApacheAtlas import ApacheAtlasClient
from ..utils.Constants import TypeNames, EndRelations
import json
import pandas as pd

class EntityClient:

    ENTITY_API = "/entity/"

    BULK_ENTITY = API(ENTITY_API + "bulk", HTTPMethod.POST)
    GET_BULK_ENTITY = API(ENTITY_API + "bulk", HTTPMethod.GET)

    CREATE_ENTITY = API(ENTITY_API, HTTPMethod.POST)
    GET_ENTITY = API(ENTITY_API + "guid/{guid}", HTTPMethod.GET)
    DELETE_ENTITY = API(ENTITY_API + "guid/{guid}", HTTPMethod.DELETE)

    def __init__(self, client: ApacheAtlasClient):
        self.client = client

    def update_entity_attributes_by_guid(self, guid, attributes):
        entity_full = self.get_entity_by_guid(guid)
        entity_reduced = entity_full['entity']

        entity_reduced['attributes'] = {
           **entity_reduced['attributes'],
           **attributes 
        }

        return self.create_entity(entity_reduced)

    def get_entities_by_guid(self, guids):
        return self.client.request(
            self.GET_BULK_ENTITY.add_multivalued_query_params({ "guid": guids })
        )

    def delete_entity_by_guid(self, guid_entity):
        return self.client.request(
             api_instance=self.DELETE_ENTITY.format_path({ 'guid': guid_entity })
        )

    def create_entity(self, entity):
        entity_body = { 
            "entity": entity
        }

        response = self.client.request(
            self.CREATE_ENTITY,
            entity_body
        )

        if 'mutatedEntities' not in response:
            return response

        if 'CREATE' in response['mutatedEntities']:
            return response['mutatedEntities']['CREATE'][0]

        return response
    
    def create_multiple_entities(self, entities):
        body = {
            "entities": entities
        }

        response = self.client.request(
            self.BULK_ENTITY,
            body
        )

        if 'mutatedEntities' not in response:
            return response

        if 'CREATE' in response['mutatedEntities']:
            return response['mutatedEntities']['CREATE']

        return response

    def create_entity_file_table(self, data: FileDO, table_acronymus: str, table_column: str):
        file_exists = self.client.search.search_by_attribute(   
            attributes={
                'typeName': TypeNames.TABLE_FILE,
                'attrName': 'name',
                'attrValuePrefix': data['name'],
                'limit': 1,
                'offset': 0
            }
        )

        if 'entities' in file_exists:
            raise AtlasServiceException("Esse Arquivo já possui metadados")

        entity_table = self.client.search.search_table_by_acronymus(table_acronymus.upper())

        if not entity_table:
            raise AtlasServiceException("Essa tabela não existe, ou sigla está errada")

        entity_column = self.client.search.search_annual_table(table_column)

        if not entity_column:
            raise AtlasServiceException(f"Esse ano ({data['table_column']}) não possui uma entidade ou nome está inválido")
        
        entity_file = {
            'typeName': TypeNames.TABLE_FILE,
            'attributes': {
                **data,
                ** {
                    'qualifiedName': f'{TypeNames.TABLE_FILE}.{table_acronymus}.DataSUS@{data["name"]}',
                    EndRelations.END_TABLE_FILE_COLUMN[1]: {
                        'guid': entity_table['guid']
                    },
                    EndRelations.END_TABLE_FILE_COLUMN[0]: {
                        'guid': entity_column['guid']
                    }, 
                  }
             },
        }

        return self.client.entity.create_entity(entity_file)
        
    def create_entity_dt_table(self, attributes: AttributesTable, database_acronymus: str):
       database = self.client.search.search_unique_entity({
           'typeName': f'{TypeNames.DATABASE}',
           'attrName': 'acronymus',
           'attrValue': database_acronymus
       })

       if not database:
           raise AtlasServiceException("Database nào existe")
       
       attributes['qualifiedName'] = f"{TypeNames.TABLE}.{database_acronymus}.DataSUS@{attributes['acronymus']}"
       attributes['acronymus'] = attributes['acronymus'].upper()
       attributes[EndRelations.END_DATABASE_TO_TABLE[1]] = {
           "guid": database['guid']
       }

       entity_body = {
           "typeName": F"{TypeNames.TABLE}",
           "attributes": attributes
       }

       return self.client.entity.create_entity(entity_body)

    def get_entity_by_guid(self, guid_entity):
        return self.client.request(
             api_instance=self.GET_ENTITY.format_path({ 'guid': guid_entity })
        )
    
    def create_entity_columns(self, path_csv, table_acronymus: str):
        table = self.client.search.search_table_by_acronymus(table_acronymus)

        if not table:
            raise AtlasServiceException("Tabela não existe")
        
        columns = []

        df_columns = pd.read_csv(path_csv)
        df_columns.fillna('', inplace=True)

        for _, row in df_columns.iterrows():
            columns.append({
                "typeName": f"{TypeNames.TABLE_COLUMN}",
                "attributes": {
                    "name": row['name'],
                    "qualifiedName": f"{TypeNames.TABLE_COLUMN}.DataSUS.{table_acronymus}@{row['name']}",
                    "description": row['description'] if 'description' in row else "Não documentado pelo DataSUS...",
                    'primary_key': True if 'primary_key' in row and row['primary_key'] == "SIM" else False,
                    "domain": row['domain'] if 'domain' in row else "",
                    "type": row['type'] if 'type' in row else "",
                    'observation': row['observation'] if 'observation' in row else "",
                    'characteristics':  row['characteristics'] if 'characteristics' in row else "",
                    EndRelations.END_TABLE_TO_COLUMN[1] : {
                        'guid': table['guid']
                    }
                }
            })
        
        return self.create_multiple_entities(columns)
        
    def create_database_entity(self, attributes, data_repository_name):        
        data_repository_entity = self.client.search.search_unique_entity({
           'typeName': f'{TypeNames.DATA_REPOSITORY}',
           'attrName': 'name',
           'attrValue': data_repository_name
        })

        if not data_repository_entity:
            raise AtlasServiceException("Repositorio de dados não encontrado")
        
        entity_database = {
            "typeName": TypeNames.DATABASE,
            "attributes": {
                **attributes,
                ** {
                   'qualifiedName': f"{TypeNames.DATABASE}.DataSUS@{attributes['acronymus']}",
                   EndRelations.END_REPOSITORY_DATA[1]: {
                       'guid': data_repository_entity['guid']
                   }     
                 }
            }
        }

        return self.create_entity(entity_database)