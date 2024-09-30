from ..utils.API import HTTPMethod, API
from apache_atlas.client.ApacheAtlas import ApacheAtlasClient

class LineageClient:

    LINEAGE_BY_GUID = API(
        path="/lineage/{guid}",
        method=HTTPMethod.GET
    )

    def __init__(self, client: ApacheAtlasClient):
        self.client = client

    def get_lineage_by_guid(self, guid_entity):
        return self.client.request(
            self.LINEAGE_BY_GUID
                .format_path({ "guid": guid_entity })
                .add_query_params({ "depth": 999_999 })
        )

    def get_last_guid_entity_of_lineage(self, data):
        
        if not data:
            return None

        from_entity_ids = { item["fromEntityId"] for item in data }
        to_entity_ids = { item["toEntityId"] for item in data }

        unique_to_entity = to_entity_ids - from_entity_ids

        if not unique_to_entity:
            return None

        return unique_to_entity.pop()
    