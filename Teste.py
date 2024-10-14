from apache_atlas.client.ApacheAtlas import ApacheAtlasClient
import random
import json
from apache_atlas.utils.Constants import EndRelations, TypeNames
from apache_atlas.utils.API import API, HTTPMethod

atlas_section = ApacheAtlasClient(
    "http://localhost:21000",
    "admin",
    "admin"
)


atlas_section.process.create_process_drop_column(
    'BRONZE_PROCESS',
    [
      'AP_MNDIF',
      'AP_CODUNI'
    ]
)


'''
atlas_section.lineage.create_entity_lineage_by_interval_time_monthly({
    'start_year': 10,
    'end_year': 10,
    'start_month': 1,
    'end_month': 12,
}, "AQ", "BRONZE_PROCESS")
'''



random_int = random.randint(0, 100_000_000)


