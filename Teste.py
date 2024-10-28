from apache_atlas.client.ApacheAtlas import ApacheAtlasClient
import random
import json
import os
from apache_atlas.utils.Constants import EndRelations, TypeNames
from apache_atlas.utils.API import API, HTTPMethod

atlas_section = ApacheAtlasClient(
    "http://localhost:21000",
    "admin",
    "admin"
)


'''
changed_columns = [
    {
        "name": "SEXO",
        "attributes_to_change": { 
            'domain': ', '.join(["Ignorado", "Homem", "Mulher"]),
            'type': 'Char (255)'
        }
    },
    {
        "name": "TIPOBITO",
        "attributes_to_change": { 
            'domain': ', '.join(["Fetal", "Não Fetal"]),
            'type': 'Char (255)'
        }
    },
    {
        "name": "RACACOR",
        "attributes_to_change": { 
            'domain': ', '.join(["Branca", "Preta", "Amarela", "Parda", "Indígena"]),
            'type': 'Char (255)'
        }
    },
    {
        "name": "ESTCIV",
        "attributes_to_change": { 
            'domain': ', '.join(["Solteito", "Casado", "Viúvo", "Separado Judicialmente", "Ignorado"]),
            'type': 'Char (255)'
        }
    },
    {
        "name": "ESC",
        "attributes_to_change": { 
            'domain': ', '.join(["Nenhuma", "1 a 3 anos", "4 a 7 anos", "8 a 11 anos", "12 a mais", "Ignorado"]),
            'type': 'Char (255)'
        }
    },
    {
        "name": "LOCOCOR",
        "attributes_to_change": { 
            'domain': ', '.join(["Hospital", "Outro estado saúde", "Domicílio", "Via Pública", "Outros", "Ignorado"]),
            'type': 'Char (255)'
        }
    },
    {
        "name": "GRAVIDEZ",
        "attributes_to_change": { 
            'domain': ', '.join(["Única", "Dupla", "Tripla e mais", "Ignorado"]),
            'type': 'Char (255)'
        }
    },
    {
        "name": "SEMAGESTAC",
        "attributes_to_change": { 
            'domain': ', '.join(["Menos de 22 semanas", "22 a 27 semanas", "28 a 31 semanas", "32 a 36 semanas", "37 a 41 semanas", "42 semanas e mais", "Ignorado"]),
            'type': 'Char (255)'
        }
    },
    {
        "name": "GESTACAO",
        "attributes_to_change": { 
            'domain': ', '.join(["Vaginal", "Cesáreo", "Ignorado"]),
            'type': 'Char (255)'
        }
    },
    {
        "name": "PARTO",
        "attributes_to_change": { 
            'domain': ', '.join(["Antes" , "Durante", "Depois", "Ignorado"]),
            'type': 'Char (255)'
        }
    },
    {
        "name": "OBITOPARTO",
        "attributes_to_change": { 
            'domain': ', '.join(["Antes" , "Durante", "Depois", "Ignorado"]),
            'type': 'Char (255)'
        }
    },
    {
        "name": "OBITOGRAV",
        "attributes_to_change": { 
            'domain': ', '.join(["Sim" , "Não", "Ignorado"]),
            'type': 'Char (255)'
        }
    },
    {
        "name": "OBITOPUERP",
        "attributes_to_change": { 
            'domain': ', '.join(["Sim até 42 dias" , "Sim, de 43 dias a 1 ano", "Não", "Ignorado"]),
            'type': 'Char (255)'
        }
    },
    {
        "name": "ASSISTMED",
        "attributes_to_change": { 
            'domain': ', '.join(["Sim" , "Não", "Ignorado"]),
            'type': 'Char (255)'
        }
    },
    {
        "name": "EXAME",
        "attributes_to_change": { 
            'domain': ', '.join(["Sim" , "Não", "Ignorado"]),
            'type': 'Char (255)'
        }
    },
    {
        "name": "CIRURGIA",
        "attributes_to_change": { 
            'domain': ', '.join(["Sim" , "Não", "Ignorado"]),
            'type': 'Char (255)'
        }
    },
    {
        "name": "NECROPSIA",
        "attributes_to_change": { 
            'domain': ', '.join(["Sim" , "Não", "Ignorado"]),
            'type': 'Char (255)'
        }
    }
]

atlas_section.process.create_process_alter_column_dataset(
    changed_columns,
    'DO-32132322',
    'DO'
)



atlas_section.process.create_process_drop_column_dataset(
    'DO-32132322',
    [
            "ORIGEM",
            "ESC2010",
            "SERIESCFAL",
            "OCUP",
            "LINHAA",
            "LINHAB",
            "LINHAC",
            "LINHAD",
            "LINHAII",
            "DTRECORIGA",
            "ESCMAEAGR1",
            "ESCFALAGR1",
            "STDOEPIDEM",
            "STDONOVA",
            "DIFDATA",
            "CONTADOR",
            "DTRECEBIM",
            "VERSAOSIST",
            "CAUSABAS_O",
            "ATESTANTE",
            "STCODIFICA",
            "CODIFICADO",
            "VERSAOSCB",
            "NUMEROLOTE",
            "TPPOS",
            "CODMUNRES",
            "CODMUNNATU",
            "CODMUNOCOR",
            "ATESTADO"
    ]
)




atlas_section.lineage.create_entity_lineage_by_interval_time_anual({
    'start_year': 2017,
    'end_year': 2022,
}, "DO", "DO-32132322")




'''




'''
atlas_section.process.create_process_alter_column_dataset(
    [
        { 
            'name': 'AQ_ESQU_P2', 
            'attributes_to_change': { 
            'domain': ', '.join({),
            'type': 'Char (255)'
        }
                'name': 'AQ_ESQUEMA_P2'
            }
        },
        { 
           'name': 'AP_CEPPCN', 
           'attributes_to_change': { 
           'domain': ', '.join({),
           'type': 'Char (255)'
        }
               'name': 'AP_CEP_PACIENTE'
           }
        },
    ],
    '948329084902384',
    "AQ"
)

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


