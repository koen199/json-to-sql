from pydantic import BaseModel

class FilterSchema(BaseModel):
    field:str 
    op:str
    value:str|float|int

def deserialize_filters(filters_data:list[FilterSchema]):
    return None