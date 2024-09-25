from ..utils.Enums import HTTPMethod

class LineageClient:
    LINEAGE_BY_GUID = "/v2/lineage"
    
    def __init__(self, client):
        self.client = client

    def get_lineage_by_guid(self, guid_entity):
        return self.client.request(
            f"{self.LINEAGE_BY_GUID}/{guid_entity}",  HTTPMethod.GET,
            params={
                "depth": 999_999,
            }
        )

    def get_last_entity_of_lineage(self, data):
        if not data:
            return None

        from_entity_ids = { item["fromEntityId"] for item in data }
        to_entity_ids = { item["toEntityId"] for item in data }

        unique_to_entity = to_entity_ids - from_entity_ids

        if not unique_to_entity:
            return None

        return unique_to_entity.pop()
    