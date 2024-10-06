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
   
    def format_qualifiedName_updated_column(self, qualifiedName):
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