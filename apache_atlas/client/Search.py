from ..utils.API import HTTPMethod, API
from apache_atlas.client.ApacheAtlas import ApacheAtlasClient
import json

class SearchClient:

    SEARCH_BY_ATTRIBUTE = API(
        path="/search/attribute",
        method=HTTPMethod.GET
    )

    def __init__(self, client):
        self.client = client

    def search_unique_entity(self, attributes):
        response = self.search_by_attribute({
             'typeName': attributes['typeName'],
             'attrName': attributes['attrName'],
             'attrValuePrefix': attributes['attrValue'],
             'limit': 1,
             'offset': 0
        })

        if 'entities' not in response:
             return None

        return response['entities'][0]

    def search_by_attribute(self, attributes):
        return self.client.request(
            api_instance=self.SEARCH_BY_ATTRIBUTE.add_query_params(attributes)
        )

    def search_data_repository(self, data_repository_name):
        response = self.search_by_attribute({
             'typeName': 'dt_data_repository',
             'attrName': 'name',
             'attrValuePrefix': data_repository_name,
             'limit': 1,
             'offset': 0
        })

        if 'entities' not in response:
             return None

        return response['entities'][0]

    def search_annual_table(self, name):
        response = self.search_by_attribute(
            attributes={
                'typeName': 'dt_anual_table',
                'attrName': 'name',
                'attrValuePrefix': name,
                'limit': 1,
                'offset': 0
            }
        )

        if 'entities' not in response:
            return None

        return response['entities'][0]

    def search_table_by_acronymus(self, acronymus):
        response = self.search_by_attribute(
            attributes={
                'typeName': 'dt_table',
                'attrName': 'acronymus',
                'attrValuePrefix': acronymus,
                'limit': 1,
                'offset': 0
            }
        )

        if 'entities' not in response:
            return None

        return response['entities'][0]