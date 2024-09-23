from apache_atlas.client.ApacheAtlas import ApacheAtlasClient
import json

atlas_section = ApacheAtlasClient(
    "http://10.100.100.61:21000",
    "admin",
    "admin"
)

test = atlas_section.lineage.get_lineage_by_guid("0ab0a86b-f777-4a2c-a8ac-5f529fe803ba")

print(json.dumps(test, indent=2))