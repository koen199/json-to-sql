from typing import TYPE_CHECKING, List, Union, Any
import sqlalchemy as sa
from json_to_sql.schemas import deserialize_filters
from json_to_sql.filters.filters import CompositeFilter

if TYPE_CHECKING:
    from json_to_sql.schemas import FilterSchema, CompositeFilterSchema

def build_query(
    class_: type,
    filters: Union[List[Union['FilterSchema', 'CompositeFilterSchema']]],
    property_map: Union[dict, None] = None,
    order_by: str = None,
    is_desc: bool|str = False
):
    _filters = deserialize_filters(filters)
    query = sa.select(class_)
    joined_tables = set()

    if isinstance(_filters, list):
        if len(_filters) == 1 and isinstance(_filters[0], CompositeFilter):
            root_filter = _filters[0]
        else:
            root_filter = CompositeFilter(op="and", filters=_filters)
    else:
        root_filter = _filters

    query = sa.select(class_)
    joined_tables = set()

    query = root_filter.apply(query, class_, property_map, joined_tables)

    if order_by:
        if isinstance(order_by, str):
            order_by = order_by.split(',')
        order_by = [
            getattr(class_, property_map[field] if property_map and field in property_map else field)
            for field in order_by
        ]

        if isinstance(is_desc, bool):
            is_desc = [is_desc] * len(order_by)
        if isinstance(is_desc, str):
            is_desc = [value.strip().lower() in {'true', '1'} for value in is_desc.split(',')]
            if len(is_desc) == 1:
                is_desc = is_desc * len(order_by)

        if len(order_by) != len(is_desc):
            raise ValueError("order_by and is_desc must have the same length.")
        for field, desc_flag in zip(order_by, is_desc):
            query = query.order_by(sa.desc(field) if desc_flag else field)

    return query