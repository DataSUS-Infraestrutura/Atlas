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

