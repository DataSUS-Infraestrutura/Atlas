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
            "name": f"Processo",
            "description": f"Processo de Teste para verificão de Arquivos",
            "qualifiedName": f"Process.DataSUS@{random_int}",
        }  
}

response = atlas_section.process.create_process_validation(
    guid_entity="df92db1b-4a8b-40c0-b15b-f4eb61152cfd",
    process_entity=process_entity
)

print(json.dumps(response, indent=2))
