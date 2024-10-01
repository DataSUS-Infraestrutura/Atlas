from ..utils.API import HTTPMethod, API
from apache_atlas.client.ApacheAtlas import ApacheAtlasClient

class SearchClient:

    SEARCH_BY_ATTRIBUTE = API(
        path="/search/attribute",
        method=HTTPMethod.GET
    )

    def __init__(self, client):
        self.client = client

    def search_by_attribute(self, attributes):
        return self.client.request(
            api_instance=self.SEARCH_BY_ATTRIBUTE.add_query_params(attributes)
        )

    def search_annual_table(self, name):
        response = self.client.search.search_by_attribute(
            attributes={
                'typeName': 'dt_anual_table',
                'attrName': 'name',
                'attrValuePrefix': name,
                'limit': 1,
                'offset': 0
            }
        )

        if not response['entities']:
            return None

        return response['entities'][0]

    def search_table_by_acronymus(self, acronymus):
        response = self.client.search.search_by_attribute(
            attributes={
                'typeName': 'dt_table',
                'attrName': 'acronymus',
                'attrValuePrefix': acronymus,
                'limit': 1,
                'offset': 0
            }
        )

        if not response['entities']:
            return None

        return response['entities'][0]