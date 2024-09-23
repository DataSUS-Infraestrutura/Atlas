from apache_atlas.client.ApacheAtlas import ApacheAtlas

class LineageClient:
    LINEAGE_BY_GUID = "/v2/lineage/"
    
    def __init__(self, client: ApacheAtlas):
        self.client = client

    def get_lineage_by_guid(self, guid_entity):
        return self.client.request(
            F"{self.LINEAGE_BY_GUID}/{guid_entity}"
        )

    
    