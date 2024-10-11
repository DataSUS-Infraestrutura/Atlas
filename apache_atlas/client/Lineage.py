from ..utils.API import HTTPMethod, API
from ..utils.Exception import AtlasServiceException
from ..utils.Types import *
from apache_atlas.client.ApacheAtlas import ApacheAtlasClient
from ..utils.Constants import TypeNames, EndRelations
import json
import pandas as pd

class LineageClient:

    LINEAGE_BY_GUID = API(
        path="/lineage/{guid}",
        method=HTTPMethod.GET
    )

    def __init__(self, client: ApacheAtlasClient):
        self.client = client

    def get_lineage_by_guid(self, guid_entity):
        return self.client.request(
            self.LINEAGE_BY_GUID
                .format_path({ "guid": guid_entity })
                .add_query_params({ "depth": 999_999 })
        )

    def get_last_guid_entity_of_lineage(self, data):
        
        if not data:
            return None
        
        from_entity_ids = { item["fromEntityId"] for item in data }
        to_entity_ids = { item["toEntityId"] for item in data }

        unique_to_entity = to_entity_ids - from_entity_ids

        if not unique_to_entity:
            return None

        return unique_to_entity.pop()
    
    def create_lineage_table(self, data, table_acronymus: str):
        table = self.client.search.search_table_by_acronymus(table_acronymus)

        if not table:
            raise AtlasServiceException("Tabela não existe")

        full_entity_table = self.client.entity.get_entity_by_guid(table['guid'])

        list_guids_columns = [column['guid'] for column in full_entity_table['entity']['relationshipAttributes']['columns_table']]        
        guid_columns = {
            full_entity_table['referredEntities'][guid]['attributes']['name']: guid for guid in list_guids_columns
        }

        table_name = full_entity_table['entity']['attributes']['name']
        entities_lineage = []

        #Só aceitar no formato TTYYMM onde TT é a sigla da tabela, YY o ano e MM o mes
        for lineage, columns in data.items():
            year = lineage[2:4]
            month = lineage[4:]

            columns.sort()
            columns_guid = [{ "guid": guid_columns[column] } for column in columns]

            entities_lineage.append({
                "typeName": f"{TypeNames.MONTLY_TABLE}",
                "qualifiedName": f"{TypeNames.MONTLY_TABLE}.{table_name}@{lineage}",
                "attributes": {
                    'name': f"{lineage}",
                    'description': f'Colunas das Tabelas de {table_acronymus} do ano {year} e mês {month}',
                    "qualifiedName": f"{TypeNames.MONTLY_TABLE}@{lineage}",
                    'year': year,
                    "month": month,
                    EndRelations.END_LINEAGE_TO_COLUMN[0]: columns_guid,
                }
            })

        entities_lineage = self.client.entity.create_multiple_entities(entities_lineage)

        entity_timeline = {
            "typeName": f"{TypeNames.TIMELINE}",
            "attributes": {
                "name": f"Timeline de {table_name}",
                "description": f'Timeline de alteração de colunas do datasus de {table_name}',
                'qualifiedName': f"Process.DataSUS.{TypeNames.TIMELINE}@{table_name}"
            }
        }

        entity_timeline = self.client.entity.create_entity(entity_timeline)    
        lineage = self.client.utils.detect_column_changes(data)

        process_timeline = []

        for entity in lineage:
            lineage_string = entity['interval']
            start, end = lineage_string.split('-')
        
            addedColumns = entity['added']
            deletedColumns = entity['removed']

            entity_start = self.client.utils.find(
                lambda entity: entity['attributes']['name'] == start, 
                entities_lineage
            )

            entity_end = self.client.utils.find(
                lambda entity: entity['attributes']['name'] == end, 
                entities_lineage
            )

            process_timeline.append({
                "typeName": f"{TypeNames.PROCESS_CHANGE_COLUMN}",
                "attributes": {
                    "name": f"Alteracão de Colunas | {start} - {end}",
                    "description": f"Alteração de Colunas na tabela de {start} - {end}",
                    "qualifiedName": f"process.{TypeNames.PROCESS_CHANGE_COLUMN}.DataSUS.{table_acronymus}@{lineage_string}",
                    'added_columns': [{ "guid": guid_columns[column] } for column in addedColumns],
                    'deleted_columns': [{ "guid": guid_columns[column] } for column in deletedColumns],
                    "inputs": [
                        {
                            "typeName": entity_start['typeName'],
                            "guid": entity_start['guid']  
                        },
                    ],
                    "outputs": [
                        {
                            "typeName": entity_end['typeName'],
                            "guid": entity_end['guid'],  
                        },
                    ],
                    "processType": "ETL",
                    EndRelations.END_TIMELINE_TO_TABLE[1] : {
                        'guid': "00dea5b0-062a-4714-9f7b-c1b295c98bdf"
                    }
                }
            })
        
        return self.client.entity.create_multiple_entities(process_timeline)