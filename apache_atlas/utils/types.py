from typing import TypedDict

class FileDO(TypedDict):
    name: str
    description: str
    extension: str
    file_size: str
    location: str
    state: str
    total_lines: int
    year: int

class AttributesTable(TypedDict):
    name: str
    description: str
    acronymus: str