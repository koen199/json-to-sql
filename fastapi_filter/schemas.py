from pydantic import BaseModel, ValidationError
from typing import TYPE_CHECKING, List, Any
from fastapi_filter.filters import FILTERS

if TYPE_CHECKING:
    from fastapi_filter.filters.filters import Filter

__FILTER_MAP = {f.OP: f for f in FILTERS}

def _get_filter_class(operator:str)->'Filter':
    try:
        return __FILTER_MAP[operator]
    except KeyError:
        raise ValidationError('No filter with operator {} exists'.format(operator), None)

class FilterSchema(BaseModel):
    field:str 
    op:str
    value: Any 

def deserialize_filters(filters_data:List[FilterSchema])->'List[Filter]':
    filters = []
    for f in filters_data:
        Class = _get_filter_class(f.op)
        filters.append(Class(f))
    return filters