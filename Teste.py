from apache_atlas.client.ApacheAtlas import ApacheAtlasClient
import random
import json

atlas_section = ApacheAtlasClient(
    "http://localhost:21000",
    "admin",
    "admin"
)

random_int = random.randint(0, 100_000_000)

process_entity = {
      "typeName": "Process",
       "attributes": {
            "name": f"Validar Dados",
            "description": f"Processo que valida os Dados",
            "qualifiedName": f"Process.DataSUS@{random_int}",
        }  
}
 
'''
response = atlas_section.process.create_process_validation(
    guid_entity="7b1d0a05-8242-41c4-a996-73d9ae2e7882",
    process_entity=process_entity
)

print(json.dumps(response, indent=2))
'''
