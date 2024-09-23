from ..utils.Enums import HTTPMethod

class LineageClient:
    LINEAGE_BY_GUID = "/v2/lineage"
    
    def __init__(self, client):
        self.client = client

    def get_lineage_by_guid(self, guid_entity):
        return self.client.request(
            f"{self.LINEAGE_BY_GUID}/{guid_entity}",  HTTPMethod.GET
        )

    
    