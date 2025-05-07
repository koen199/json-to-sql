from pydantic import BaseModel
from typing import List, Any, Union
from json_to_sql.filters import FILTERS
from json_to_sql.filters.filters import Filter, CompositeFilter

__FILTER_MAP = {f.OP: f for f in FILTERS}

def _get_filter_class(operator:str)->'Filter':
    try:
        return __FILTER_MAP[operator]
    except KeyError:
        raise KeyError('No filter with operator {} exists'.format(operator), None)

class FilterSchema(BaseModel):
    field:str 
    op:str
    value: Any 

class CompositeFilterSchema(BaseModel):
    op: str
    value: List[Union[FilterSchema, 'CompositeFilterSchema']]

def deserialize_filters(filters_data: List[Union[FilterSchema, CompositeFilterSchema]]) -> List[Filter]:
    filters = []
    
    for f in filters_data:
        if isinstance(f, FilterSchema):
            Class = _get_filter_class(f.op)
            filters.append(Class(f))

        elif isinstance(f, CompositeFilterSchema):
            composite_filter = CompositeFilter(
                op=f.op,
                filters=deserialize_filters(f.value)
            )
            filters.append(composite_filter)
    return filters