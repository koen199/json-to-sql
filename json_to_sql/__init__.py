from typing import TYPE_CHECKING, List, Union, Any
from json_to_sql.schemas import deserialize_filters

if TYPE_CHECKING:
    from sqlalchemy.orm import Session
    from json_to_sql.schemas import FilterSchema

def build_query(
    session:'Session',
    class_:type,
    filters: List['FilterSchema'],
    property_map:Union[dict, None]=None,
    order_by:Union[str, None]=None
):
    _filters = deserialize_filters(filters)
    query = session.query(class_)
    for f in _filters:
        query = f.apply(query, class_, property_map)
    if order_by is not None:
        query = query.order_by(order_by)
    return query