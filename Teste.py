from apache_atlas.client.ApacheAtlas import ApacheAtlasClient
import random
import json
import os
from apache_atlas.utils.Constants import EndRelations, TypeNames
from apache_atlas.utils.API import API, HTTPMethod

atlas_section = ApacheAtlasClient(
    "http://localhost:21000",
    "admin",
    "admin"
)

random_int = random.randint(0, 100_000_000)


