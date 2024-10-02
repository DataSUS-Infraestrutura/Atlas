from apache_atlas.client.ApacheAtlas import ApacheAtlasClient
import random
import json

atlas_section = ApacheAtlasClient(
    "http://10.100.100.61:21000",
    "admin",
    "admin"
)

random_int = random.randint(0, 100_000_000)

process_entity = {
      "typeName": "Process",
       "attributes": {
            "name": f"Alterar Colunas",
            "description": f"Processo que altera Colunas",
            'qualifiedName': "Process.@Teste"
        }  
}

'''
type_column = {
      "name": "dt_table_column_process",
      "description": "Representa uma coluna após uma transformação",
      "superTypes": ["DataSet"],
      "attributeDefs": [
        {
          "name": "primary_key",
          "typeName": "boolean",
          "isOptional": True,
          "isUnique": False,
          "isIndexable": False
        },
        {
          "name": "domain",
          "typeName": "string",
          "isOptional": True,
          "isUnique": False,
          "isIndexable": False
        },
        {
          "name": "type",
          "typeName": "string",
          "isOptional": True,
          "isUnique": False,
          "isIndexable": False
        },
        {
          "name": "observation",
          "typeName": "string",
          "isOptional": True,
          "isUnique": False,
          "isIndexable": False
        },
        {
          "name": "characteristics",
          "typeName": "string",
          "isOptional": True,
          "isUnique": False,
          "isIndexable": False
        }
      ]
}

response = atlas_section.type.create_type([type_column])
atlas_section.process.create_process_alter_column(
    params_search= { 'table_acronymus': 'AB', 'column_name': 'AB_NUMAIH' },
    process_change=process_entity,
    attribues_to_change={
        "type": "CHAR(5)"
    }
)

print(json.dumps(response, indent=2))

# atlas_section.entity.delete_entity_by_guid("cf8234b8-3db0-46db-8dac-f65dfb54f868")

response_file = atlas_section.entity.create_entity_file_table({
    "name": "DO-Teste",
    "description": "Tabela de DO de Test",
    "extension": ".oarquet",
    "file_size": "16 MB",
    "location": "/teste/teste/teste",
    "state": "SP",
    "total_lines": 0,
    "year": 2020,
}, "SIM", "DO2017")

print(json.dumps(response_file, indent=2))
'''


'''
a = atlas_section.entity.get_entity_by_guid("0ab0a86b-f777-4a2c-a8ac-5f529fe803ba")

atlas_section.process.create_process_alter_column(
    params_search= { 'table_acronymus': 'AB', 'column_name': 'AP_ALTA' },
    process_change=process_entity,
    attribues_to_change={
        'type': 'NUMERIC(1)'
    }
)

response = atlas_section.process.create_process_validation(
    guid_entity="7b1d0a05-8242-41c4-a996-73d9ae2e7882",
    process_entity=process_entity
)

print(json.dumps(response, indent=2))
'''

