from typing import TYPE_CHECKING, List, Union, Any
import sqlalchemy as sa
from sqlalchemy.orm import Session
from sqlalchemy import desc
from json_to_sql.schemas import deserialize_filters

if TYPE_CHECKING:
    from json_to_sql.schemas import FilterSchema

def build_query(
    class_:type,
    filters: List['FilterSchema'],
    property_map:Union[dict, None]=None,
    order_by:Union[str, None]=None,
    is_desc:bool=False
):
    _filters = deserialize_filters(filters)
    query = sa.select(class_)
    for f in _filters:
        query = f.apply(query, class_, property_map)
    if order_by is not None:
        query = query.order_by(desc(order_by) if is_desc else order_by)
    return query