from typing import TYPE_CHECKING, List, Union, Any
from collections import defaultdict
from sqlalchemy.sql.expression import Select
from sqlalchemy import orm
import sqlalchemy as sa
from json_to_sql.schemas import deserialize_filters

if TYPE_CHECKING:
    from json_to_sql.schemas import FilterSchema
from json_to_sql.filters.filters import Filter
    
def group_filters_by_condition_group(filters:list[Filter])->dict[str, Filter]:
    grouped = defaultdict(list)
    for f in filters:
        group = f.condition_group
        grouped[group].append(f)
    return dict(grouped)

def get_attrib_for_filter(class_:Any, filter:Filter)->Any:
    for field in filter.fields[:-1]:
        class_ = getattr(class_, field).mapper.class_
    return getattr(class_, filter.fields[-1])

def get_attrib_from_tree(tree:dict, fields:list[str])->Any:
    attrib = tree
    for field in fields:
        attrib = attrib[field]
    return attrib
        
def convert_to_tree(filters:list[Filter])->dict:
    tree = {}
    for f in filters:
        current = tree
        for part in f.fields[:-1]:
            current = current.setdefault(part, {})
        current[f.fields[-1]] = None
    return tree

def get_internal_db_field(field:str, property_map:dict|None):
    if not property_map:
        return field
    return property_map.get(field, field) #Return the same field in no mapping is defined
    
def join_required_relations(
    stmt:Select,
    class_:Any,
    tree:dict,
    property_map:dict,
    condition_group:str,
    alias=True
)->Select:
    for k, v in tree.items():
        if v == None:
            fieldname = get_internal_db_field(k, property_map)
            tree[k] = getattr(class_, fieldname)
            continue
        fieldname = get_internal_db_field(k, property_map)
        nested_class_ = getattr(class_, fieldname).mapper.class_
        if alias:
            nested_class_ = orm.aliased(nested_class_, name=condition_group)
        stmt = stmt.join(nested_class_)
        stmt = join_required_relations(stmt, nested_class_, tree[k], property_map, condition_group, alias=False)
    return stmt    

def build_query(
    class_: type,
    filters: List['FilterSchema'],
    property_map: Union[dict, None] = None,
    order_by: Union[str, List[str], None] = None,
    is_desc: Union[bool, List[bool]] = False
):
    _filters = deserialize_filters(filters)
    query = sa.select(class_)
    
    grouped = group_filters_by_condition_group(_filters)
    tree_condition_grouped = {}
    for condition_group, group in grouped.items():
        tree = convert_to_tree(group)
        query = join_required_relations(query, class_, tree, property_map, condition_group)
        tree_condition_grouped[condition_group] = tree
        
    for f in _filters:
        tree = tree_condition_grouped[f.condition_group]
        attrib = get_attrib_from_tree(tree, f.fields)
        query = f.apply(query, attrib, property_map)

    if isinstance(order_by, str):
        order_by = order_by.split(',')
        order_by = [
            getattr(class_, property_map[field] if property_map and field in property_map else field)
            for field in order_by
        ]

    if isinstance(is_desc, bool):
        is_desc = [is_desc] * (len(order_by) if order_by else 0)

    if order_by:
        if len(order_by) != len(is_desc):
            raise ValueError("order_by and is_desc must have the same length.")
        
        for field, desc_flag in zip(order_by, is_desc):
            query = query.order_by(sa.desc(field) if desc_flag else field)

    return query