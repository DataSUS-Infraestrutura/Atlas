class ProcessClient:
    
    def __init__(self, client):
        self.client = client

    def create_process_validation(self, guid_entity, process):
        lineage_entity = self.client.get_lineage_by_guid(guid_entity)

        

    
    