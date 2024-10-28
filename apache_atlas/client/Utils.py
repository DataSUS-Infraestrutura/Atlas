from ..utils.API import HTTPMethod, API
from ..utils.Exception import AtlasServiceException
from ..utils.Types import FileDO
from apache_atlas.client.ApacheAtlas import ApacheAtlasClient
import json
import re

class UtilsClient:

    def __init__(self, client: ApacheAtlasClient):
        self.client = client

    def get_version_lineage(self, total_absolute_process_lineage):
        return (total_absolute_process_lineage // 2) + 1
    
    def find(self, callback, list):
        
        for element in list:
            if callback(element):
                return element
            
        return None
    
    def format_qualifiedName_version(self, qualifiedName):
        version_pattern = r"\.v(\d+)"
        match = re.search(version_pattern, qualifiedName)
       
        if match:
            existing_version = int(match.group(1))
            new_version = existing_version + 1
            formatted_name = qualifiedName[:-len(match.group())] + f".v{new_version}"
        else:
            formatted_name = qualifiedName + ".v1"

        return formatted_name
    
    def format_change_atributes_to_description(self, atributes_to_change):
        description = ""

        for key, value in atributes_to_change.items():
            description += f"Atributo {key} -> {value}, "

        return description[:-2] + "."
    
    # todo ve ser essa ordenação ta certa
    def detect_column_changes(self, files):
        
        def chave_ordenacao(chave):
            ano = int(chave[0][-4:-2])
            mes = int(chave[0][-2:])

            # E tome numeros magicos, são por que o ano só tem 2 ditito e tem uns < 99
            # que é pra do ano de 1990, então faz essa verificação, tem outra parte do codigo com isso tbm
            if ano > 80 and ano <= 99:
               ano = ano + 1900
            else:
               ano = ano + 2000 

            return ano, mes

        items = files.items()
        items_ordenados = sorted(items, key=chave_ordenacao)

        files = dict(items_ordenados)

        sorted_files = list(files.keys())
        
        change_intervals = []
        last_columns = None
        first_file = sorted_files[0]
        
        for i, file in enumerate(sorted_files):
            current_columns = set(files[file])
            
            if last_columns is None:
                last_columns = current_columns
                continue
            
            added_columns = current_columns - last_columns
            removed_columns = last_columns - current_columns
            
            if added_columns or removed_columns:
                interval = {
                    'interval': f"{first_file}-{file}",
                    'added': list(added_columns),
                    'removed': list(removed_columns)
                }
            
                change_intervals.append(interval)
                first_file = file
            
            last_columns = current_columns
        
        return change_intervals