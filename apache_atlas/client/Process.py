import json
from apache_atlas.client.ApacheAtlas import ApacheAtlasClient
from ..utils.Exception import AtlasServiceException
from ..utils.Constants import TypeNames, EndRelations
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

        column_to_change_entity = self.client.utils.find(
            lambda entity: entity['attributes']['name'] == params_search['column_name'],
            full_entity['referredEntities'].values()
        )

        if column_to_change_entity is None:
            raise AtlasServiceException("Coluna não existe ou nome inválido")
        
        lineage_column = self.client.lineage.get_lineage_by_guid(column_to_change_entity['guid'])
        last_entity_guid = self.client.lineage.get_last_guid_entity_of_lineage(lineage_column['relations'])

        total_absolute_process_lineage = len(lineage_column['relations'])

        last_entity = None

        if last_entity_guid:
            last_entity = self.client.entity.get_entity_by_guid(last_entity_guid)['entity']
        else:
            last_entity = column_to_change_entity

        last_entity_qualifiedName = last_entity['attributes']['qualifiedName']
        table_group_columns_changed = self.client.search.search_table_by_acronymus("DTC_2")

        if not table_group_columns_changed:
            raise AtlasServiceException("Tabela para agrupar colunas modificas não criada")
        
        # Adaptar Depois isso daq, para criar um table só com as colunas alteradas
        new_column_data = {
             "typeName": f"{TypeNames.TABLE_COLUMN}",
             "attributes": { 
                 **last_entity['attributes'],
                 **attribues_to_change,
                 ** { 
                    EndRelations.END_TABLE_TO_COLUMN[1]: {
                        'guid': table_group_columns_changed['guid']
                    }
                 }
              }
        }
        
        new_column_data['attributes']['qualifiedName'] = self.client.utils.format_qualifiedName_version(last_entity_qualifiedName)
        
        new_column_entity = self.client.entity.create_entity(new_column_data)
        version_process = self.client.utils.get_version_lineage(total_absolute_process_lineage)

        process_change = {
            "typeName": f"{TypeNames.PROCESS}",
            "attributes": {
                "name": f"Alteracão de Colunas - {last_entity['attributes']['name']}",
                "description": f"Alterações nos atributos: " + self.client.utils.format_change_atributes_to_description(attribues_to_change),
            }  
        }

        process_change['attributes']['qualifiedName'] = \
            f"Process.AlterTable_DataSUS@{params_search['table_acronymus']}{params_search['column_name']}.v{version_process}"
        
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
        
    def create_process_drop_column(self, id_process, columns):
        dataset_processing_entity = self.client.search.search_unique_entity({
           'typeName': f'{TypeNames.DATASET_PROCESSING_LINEAGE}',
           'attrName': 'id',
           'attrValue': id_process
        })

        if not dataset_processing_entity:
            raise AtlasServiceException("Processo com esse ID não existe")

        lineage_data = self.client.lineage.get_data_lineage(dataset_processing_entity['guid'])

        full_entity = lineage_data['last_entity']
        entity = full_entity['entity']

        columns_guid = [column['guid'] for column in entity['attributes']['columns']]
        columns_entities = self.client.entity.get_entities_by_guid(columns_guid)

        columns_to_dropped = []

        for column in columns:
            find_column = self.client.utils.find(
                lambda entity: entity['attributes']['name'] == column,
                columns_entities['entities']
            )

            if not find_column:
                raise AtlasServiceException("Coluna com o nome errado.")
            else:
                columns_to_dropped.append(find_column['guid'])

        
        final_columns = set(columns_guid) - set(columns_to_dropped)

        qualifiedName_entity = self.client.utils.format_qualifiedName_version(entity['attributes']['qualifiedName'])

        entity_body = {
            'typeName': TypeNames.DATASET_PROCESSING_LINEAGE_RESULT,
            'attributes': {
                ** entity['attributes'],
                ** {
                    'qualifiedName': qualifiedName_entity,
                    'columns': [ { 'guid': guid }  for guid in list(final_columns)] 
                }
            }
        }

        final_entity = self.client.entity.create_entity(entity_body)

        qualifiedName_process =f"process.{TypeNames.PROCESS_CHANGE_COLUMN}.DROP_COLUMN@{id_process}.v{lineage_data['total_process']}"
        
        process_body = {
                "typeName": f"{TypeNames.PROCESS_CHANGE_COLUMN}",
                "attributes": {
                    "name": f"Drops de Colunas",
                    "description": f"Drops de Colunas",
                    "qualifiedName": qualifiedName_process,
                    'deleted_columns': [ { 'guid': guid }  for guid in columns_to_dropped],
                    "inputs": [
                        {
                            "typeName": entity['typeName'],
                            "guid": entity['guid']  
                        },
                    ],
                    "outputs": [
                        {
                            "typeName": final_entity['typeName'],
                            "guid": final_entity['guid'],  
                        },
                    ]
                }
            }

        return self.client.entity.create_entity(process_body)




        

    
    