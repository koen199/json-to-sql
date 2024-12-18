from typing import TYPE_CHECKING, List, Union, Any
import sqlalchemy as sa
from json_to_sql.schemas import deserialize_filters

if TYPE_CHECKING:
    from json_to_sql.schemas import FilterSchema

def build_query(
    class_: type,
    filters: List['FilterSchema'],
    property_map: Union[dict, None] = None,
    order_by: Union[str, List[str], None] = None,
    is_desc: Union[bool, List[bool]] = False
):
    _filters = deserialize_filters(filters)
    query = sa.select(class_)
    for f in _filters:
        query = f.apply(query, class_, property_map)

    if isinstance(order_by, str):
        order_by = [order_by]

    if isinstance(is_desc, bool):
        is_desc = [is_desc] * (len(order_by) if order_by else 0)

    if order_by:
        if len(order_by) != len(is_desc):
            raise ValueError("order_by and is_desc must have the same length.")
        
        for field, desc_flag in zip(order_by, is_desc):
            query = query.order_by(sa.desc(field) if desc_flag else field)

    return query