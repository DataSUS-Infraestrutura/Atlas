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
    
    # Não funciona se alterar o nome da coluna
    def __create_process_alter_column(self, params_search, attributes_to_change, etl_guid, etl_id):
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
        
        # Obsoleto, já tem uma funcão que faz isso
        lineage_column = self.client.lineage.get_lineage_by_guid(column_to_change_entity['guid'])
        last_entity_guid = self.client.lineage.get_last_guid_entity_of_lineage(lineage_column['relations'])

        total_absolute_process_lineage = len(lineage_column['relations'])

        last_entity = None

        if last_entity_guid:
            last_entity = self.client.entity.get_entity_by_guid(last_entity_guid)['entity']
        else:
            last_entity = column_to_change_entity

        last_entity_qualifiedName = last_entity['attributes']['qualifiedName']

        # Criar essa entidade nA VM
        table_group_columns_changed = self.client.search.search_table_by_acronymus(TypeNames.ACRONYMUS_TABLE_DTC)

        if not table_group_columns_changed:
            raise AtlasServiceException("Tabela para agrupar colunas modificas não criada")
        
        # Adaptar Depois isso daq, para criar um table só com as colunas alteradas
        new_column_data = {
             "typeName": f"{TypeNames.TABLE_COLUMN}",
             "attributes": { 
                 **last_entity['attributes'],
                 **attributes_to_change,
                 ** { 
                    EndRelations.END_TABLE_TO_COLUMN[1]: {
                        'guid': table_group_columns_changed['guid']
                    }
                 }
              }
        }
        
        new_column_data['attributes']['qualifiedName'] = self.client.utils.format_qualifiedName_version(f"{etl_id}-{last_entity_qualifiedName}")
        
        new_column_entity = self.client.entity.create_entity(new_column_data)
        version_process = self.client.utils.get_version_lineage(total_absolute_process_lineage)

        process_change = {
            "typeName": f"{TypeNames.PROCESS_ETL_DATASET_PROCESS}",
            "attributes": {
                "name": f"Alteracão de Colunas - {last_entity['attributes']['name']}",
                "description": f"Alterações nos atributos: " + self.client.utils.format_change_atributes_to_description(attributes_to_change),
                'etl_process': {
                    'guid': etl_guid
                }
            }  
        }

        process_change['attributes']['qualifiedName'] = \
            f"Process.TableChange_DataSUS@{params_search['table_acronymus']}{etl_id}{params_search['column_name']}.v{version_process}"
        
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

        process_entity = self.client.entity.create_entity(process_change)

        return {
            'process_entity': process_entity,
            'entity_column': new_column_entity
        }
        
    def create_process_drop_column_dataset(self, id_process, columns):
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
                raise AtlasServiceException(F"Coluna '{column}' não existe nessa entidade")
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

    def create_process_alter_column_dataset(self, columns, id_process, table_acronymus, process_attributes=None):

        if not process_attributes:
            process_attributes = {
                "name": f"Alterações de Colunas",
                "description": f"ETL alterações de Colunas",
            }

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

        columns_to_updated_guid = []

        for column in columns:
            find_column = self.client.utils.find(
                lambda entity: entity['attributes']['name'] == column['name'],
                columns_entities['entities']
            )

            if not find_column:
                raise AtlasServiceException(f"Coluna '{column}' não existe nessa entidade")
            else:
                columns_to_updated_guid.append(find_column['guid'])

        diff_columns = set(columns_guid) - set(columns_to_updated_guid)

        columns_updated = []
        processes_guids = []
    
        for column in columns:
            process_response = self.__create_process_alter_column(
              params_search= { 'table_acronymus': table_acronymus, 'column_name': column['name'] },
              attributes_to_change=column['attributes_to_change'],
              etl_guid=dataset_processing_entity['guid'],
              etl_id=id_process
            )

            columns_updated.append(process_response['entity_column']['guid'])
            processes_guids.append(process_response['process_entity']['guid'])

        final_columns = list(diff_columns)
        final_columns.extend(columns_updated)

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

        qualifiedName_process =f"process.{TypeNames.PROCESS_CHANGE_COLUMN}.ALTER_COLUMN@{id_process}.v{lineage_data['total_process']}"
        
        process_body = {
                "typeName": f"{TypeNames.PROCESS_CHANGE_COLUMN}",
                "attributes": {
                 ** process_attributes,
                 ** {
                        "qualifiedName": qualifiedName_process,
                        'updated_columns': [ { 'guid': guid }  for guid in columns_to_updated_guid],
                        "processes": [ { 'guid': guid }  for guid in processes_guids ],
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
        }

        return self.client.entity.create_entity(process_body)






        

    
    