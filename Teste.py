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


atlas_section.process.create_process_alter_column_dataset(
    [
        { 
            'name': 'AQ_ESQU_P2', 
            'attributes_to_change': {
                'name': 'AQ_ESQUEMA_P2'
            }
        },
        { 
           'name': 'AP_CEPPCN', 
           'attributes_to_change': {
               'name': 'AP_CEP_PACIENTE'
           }
        },
    ],
    '948329084902384',
    "AQ"
)

'''
atlas_section.lineage.create_entity_lineage_by_interval_time_monthly({
    'start_year': 12,
    'end_year': 12,
    'start_month': 1,
    'end_month': 6,
}, "AQ", "948329084902384")

atlas_section.process.create_process_drop_column_dataset(
    '948329084902384',
    [
        'AP_VL_AP',
        'AP_AUTORIZ',
        'AP_CIDSEC',
        'AP_GESTAO',
        'AP_CIDPRI',
        'AP_COIDADE'
    ]
)
'''

random_int = random.randint(0, 100_000_000)


