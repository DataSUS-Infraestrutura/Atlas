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
            "qualifiedName": f"Process.DataSUS@{random_int}",
        }  
}



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

