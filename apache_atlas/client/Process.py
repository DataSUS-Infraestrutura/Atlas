import json
from apache_atlas.client.ApacheAtlas import ApacheAtlasClient
from ..utils.Exception import AtlasServiceException
import math
import re 

class ProcessClient:
    
    def __init__(self, client: ApacheAtlasClient):
        self.client = client

    def create_process_validation(self, guid_entity: str, process_entity):
        lineage_entity = self.client.lineage.get_lineage_by_guid(guid_entity)        
        last_entity_guid = self.client.lineage.get_last_guid_entity_of_lineage(
            lineage_entity['relations']
        )

        last_entity_typeName = None
        
        if not last_entity_guid:
            last_entity_guid = guid_entity

            entity = self.client.entity.get_entity_by_guid(last_entity_guid)
            last_entity_typeName = entity['entity']['typeName']
        else:
            last_entity_typeName = lineage_entity['guidEntityMap'][last_entity_guid]['typeName']

        process_entity['attributes']['inputs'] = [
                {
                    "typeName": last_entity_typeName,
                    "guid": last_entity_guid
                }
        ]

        process_entity['attributes']['outputs'] = [
                {   
                    "typeName": last_entity_typeName,
                    "guid": last_entity_guid
                }
            ]

        return self.client.entity.create_entity(
            process_entity
        )

    def create_process_alter_column(self, params_search, attribues_to_change):

        partial_entity_table = self.client.search.search_table_by_acronymus(params_search['table_acronymus'])

        if not partial_entity_table:
           raise AtlasServiceException("Sigla inválida")
             
        full_entity = self.client.entity.get_entity_by_guid(
            guid_entity=partial_entity_table['guid']
        )

        column_to_change_entity = list(
            filter(
                lambda entity: entity['attributes']['name'] == params_search['column_name'],
                full_entity['referredEntities'].values()
            ) 
        )

        if len(column_to_change_entity) == 0:
            raise AtlasServiceException("Coluna não existe ou nome inválido")
        
        column_to_change_entity = column_to_change_entity[0]

        lineage_column = self.client.lineage.get_lineage_by_guid(column_to_change_entity['guid'])
        last_entity_guid = self.client.lineage.get_last_guid_entity_of_lineage(lineage_column['relations'])

        total_absolute_process_lineage = len(lineage_column['relations'])

        last_entity = None

        if last_entity_guid:
            last_entity = self.client.entity.get_entity_by_guid(last_entity_guid)['entity']
        else:
            last_entity = column_to_change_entity

        last_entity_qualifiedName = last_entity['attributes']['qualifiedName']
        
        new_column_data = {
             "typeName": "dt_table_column_process_v1",
             "attributes": { 
                 **last_entity['attributes'],
                 **attribues_to_change,
              }
        }
        
        new_column_data['attributes']['qualifiedName'] = self.client.utils.format_qualifiedName_updated_column(last_entity_qualifiedName)

        new_column_entity = self.client.entity.create_entity(new_column_data)

        process_change = {
            "typeName": "Process",
            "attributes": {
                "name": f"Alteracão de Colunas",
                "description": f"Alterações nos atributos: " + self.client.utils.format_change_atributes_to_description(attribues_to_change),
            }  
        }

        process_change['attributes']['qualifiedName'] = \
            f"Process.AlterTable_DataSUS@{'|'.join(attribues_to_change.keys())}.v{self.client.utils.get_version_lineage(total_absolute_process_lineage)}"
        
        process_change['attributes']['inputs'] = [
             {
                  'typeName': last_entity['typeName'],
                  'guid': last_entity['guid'] 
             }
        ]

        process_change['attributes']['outputs'] = [
            {   
                'typeName': new_column_entity['typeName'],
                'guid': new_column_entity['guid']
            }
        ]

        return self.client.entity.create_entity(process_change)
        
    def create_process_drop_column(self, params, proces):
        pass


        

    
    