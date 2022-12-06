from typing import TYPE_CHECKING, List, Union, Any
from pydantic import BaseModel

from fastapi_filter.schemas import deserialize_filters


if TYPE_CHECKING:
    from sqlalchemy.orm import Session
    from fastapi_filter.schemas import FilterSchema

def query_with_filters(
    session:'Session',
    class_:type,
    filters: List['FilterSchema'],
    property_map:Union[dict, None]=None,
    order_by:Union[str, None]=None
)->List[Any]:
    _filters = deserialize_filters(filters)
    query = session.query(class_)
    for f in _filters:
        query = f.apply(query, class_, property_map)
    if order_by:
        query = query.order_by(order_by)
    return query.all()
