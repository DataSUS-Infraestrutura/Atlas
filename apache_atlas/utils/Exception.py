class AtlasServiceException(Exception):

    def __init__(self, msg):
        Exception.__init__(self, f"[AtlasServiceError]: {msg}")

   