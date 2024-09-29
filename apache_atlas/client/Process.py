import json
from apache_atlas.client.ApacheAtlas import ApacheAtlasClient

class ProcessClient:
    
    def __init__(self, client: 'ApacheAtlasClient'):
        self.client = client

    def create_process_validation(self, guid_entity: str, process_entity):
        lineage_entity = self.client.lineage.get_lineage_by_guid(guid_entity)        
        last_entity_guid = self.client.lineage.get_last_entity_of_lineage(
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

    def create_process_alter_column(self, database_acronymus):

        database_entity = self.client.search.search_by_attribute(
            attributes={
                'typeName': '',
                'attrName': 'acronymus',
                'attrValuePrefix': database_acronymus,
                'limit': 1,
                'offset': 0
            }
        )

        print(json.dumps(database_entity, indent=2))



        

    
    