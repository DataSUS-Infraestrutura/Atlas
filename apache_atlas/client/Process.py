import json
from apache_atlas.client.ApacheAtlas import ApacheAtlasClient
from ..utils.Exception import AtlasServiceException
import math

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

    def create_process_alter_column(self, params_search, attribues_to_change, process_change):
        
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
        )[0]

        lineage_column = self.client.lineage.get_lineage_by_guid(column_to_change_entity['guid'])
        last_entity_guid = self.client.lineage.get_last_guid_entity_of_lineage(lineage_column['relations'])
        
        if not last_entity_guid:
            last_entity_guid = column_to_change_entity['guid']

        total_process_lineage = math.ceil(len(lineage_column['relations']) / 2)

        attribues_to_change['qualifiedName'] = \
            f"{column_to_change_entity['attributes']['qualifiedName']}.v{total_process_lineage}"
        
        # Todo mudar esse typeName depois
        new_column_data = {
             "typeName": "table_column_15",
             "attributes": { 
                 **column_to_change_entity['attributes'],
                 **attribues_to_change, 
              }
        }
    
        new_column_entity = self.client.entity.create_entity(new_column_data)['mutatedEntities']['CREATE'][0]
        last_entity = self.client.entity.get_entity_by_guid(last_entity_guid) 

        process_change['attributes']['inputs'] = [
             {
                  'typeName': last_entity['entity']['typeName'],
                  'guid': last_entity_guid 
             }
        ]

        process_change['attributes']['outputs'] = [
            {   
                'typeName': new_column_data['typeName'],
                'guid': new_column_entity['guid']
            }
        ]

        return self.client.entity.create_entity(process_change)

    def create_process_drop_column(self, params, proces):
        pass


        

    
    