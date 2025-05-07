import abc
import datetime
import logging
from sqlalchemy.sql.expression import Select
from sqlalchemy import and_, or_
from sqlalchemy.sql.visitors import ClauseVisitor
from sqlalchemy.sql import ColumnElement
from sqlalchemy.sql.elements import Grouping
from typing import Any, TYPE_CHECKING, Union
if TYPE_CHECKING:
    from ..schemas import FilterSchema

logger = logging.getLogger(__name__)

def parse_date_strings(value:str)->Union[datetime.date, datetime.datetime, None]:
    try:
        return datetime.date.fromisoformat(value)
    except:
        pass
    try:
        return datetime.datetime.fromisoformat(value)
    except:
        raise ValueError('Value is not a date or datetime')

class TableCollector(ClauseVisitor):
    def __init__(self):
        self.tables = set()

    def visit_table(self, table):
        self.tables.add(table.name)

def join_relations(stmt: Select, class_: Any, fields: list[str], joined_tables: set) -> tuple[Union[Select, None], Any]:
    nested_class_ = class_
    for field in fields:
        nested_class_ = getattr(nested_class_, field).mapper.class_
        if stmt is not None and nested_class_ not in joined_tables:
            stmt = stmt.join(nested_class_)
            joined_tables.add(nested_class_)
    return stmt, nested_class_

class Filter(abc.ABC):
    OP = None

    def __init__(self, filter_data:'FilterSchema')->'Filter':
        self.nested = None
        self.fields:list[str] = filter_data.field.split('.')
        self.value = self._date_or_value(filter_data.value)
        self.is_valid()

    def __repr__(self)->str:
        fields = '.'.join(self.fields)
        return f"<{type(self).__name__}(field='{'.'.join(fields)}', op='{self.OP}'" \
               f", value={self.value})>"

    def __eq__(self, other)->bool:
        return hash(self) == hash(other)

    def __hash__(self)->int:
        return hash((self.field, self.OP, self.value))

    def apply(self, stmt:'Select', class_:type, property_map:dict, joined_tables:set)->'Select':
        fields = self._get_db_fields(self.fields, property_map)
        stmt, _ = join_relations(stmt, class_, fields[:-1], joined_tables)
        return stmt.where(self.build_condition(class_, property_map))

    @abc.abstractmethod
    def build_condition(self, class_:type, property_map:dict)->ColumnElement:
        raise NotImplementedError('build_condition is an abstract method')

    @abc.abstractmethod
    def is_valid(self)->bool:
        raise NotImplementedError('is_valid is an abstract method')

    def _get_db_field(self, field:str, property_map:dict)->str:
        if not property_map:
            return field
        return property_map.get(field, field)

    def _get_db_fields(self, fields:list[str], property_map:dict):
        db_fields = []
        for field in fields:
            db_field = self._get_db_field(field, property_map)
            db_fields.append(db_field)
        return db_fields

    def _date_or_value(self, value:Any)->Any:
        if not isinstance(value, str):
            return value
        try:
            return parse_date_strings(value)
        except ValueError:
            return value 

    def get_join_paths(self) -> list[list[str]]:
        return [self.fields[:-1]]

class CompositeFilter(Filter):
    def __init__(self, op: str, filters: list[Filter]):
        if op not in {'and', 'or'}:
            raise ValueError(f"Invalid composite operator: {op}")
        self.op = op
        self.filters = filters

    def _combine_conditions(self, class_: type, property_map: dict, joined_tables: set) -> ColumnElement:
        conditions = [
            Grouping(f._combine_conditions(class_, property_map, joined_tables)) if isinstance(f, CompositeFilter)
            else Grouping(f.build_condition(class_, property_map))
            for f in self.filters
        ]
        return and_(*conditions) if self.op == 'and' else or_(*conditions)

    def apply(self, stmt: Select, class_: type, property_map: dict, joined_tables: set) -> Select:
        for path in self.get_join_paths():
            stmt, _ = join_relations(stmt, class_, path, joined_tables)
        return stmt.where(self._combine_conditions(class_, property_map, joined_tables))
    
    def build_condition(self, class_: type, property_map: dict):
        raise NotImplementedError("CompositeFilter does not implement build_condition directly.")

    def is_valid(self) -> bool:
        if not self.filters:
            raise ValueError("CompositeFilter must have at least one sub-filter")
        return all(f.is_valid() for f in self.filters)

    def __repr__(self):
        return f"CompositeFilter(op={self.op}, filters={[repr(f) for f in self.filters]})"

    def get_join_paths(self) -> list[list[str]]:
        paths = []
        for f in self.filters:
            paths.extend(f.get_join_paths())
        return paths

class RelativeComparator(Filter):
    def is_valid(self)->bool:
        try:
            allowed = (int, float, datetime.date, datetime.datetime)
            assert isinstance(self.value, allowed)
        except AssertionError:
            raise ValueError(f"{self} requires an ordinal value", None)

class LTFilter(RelativeComparator):
    OP = "<"

    def build_condition(self, class_: type, property_map: dict) -> ColumnElement:
        fields = self._get_db_fields(self.fields, property_map)
        _, class_ = join_relations(None, class_, fields[:-1], set())
        column = getattr(class_, fields[-1])
        return column < self.value

class LTEFilter(RelativeComparator):
    OP = "<="

    def build_condition(self, class_: type, property_map: dict) -> ColumnElement:
        fields = self._get_db_fields(self.fields, property_map)
        _, class_ = join_relations(None, class_, fields[:-1], set())
        column = getattr(class_, fields[-1])
        return column <= self.value

class GTFilter(RelativeComparator):
    OP = ">"

    def build_condition(self, class_: type, property_map: dict) -> ColumnElement:
        fields = self._get_db_fields(self.fields, property_map)
        _, class_ = join_relations(None, class_, fields[:-1], set())
        column = getattr(class_, fields[-1])
        return column > self.value

class GTEFilter(RelativeComparator):
    OP = ">="

    def build_condition(self, class_: type, property_map: dict) -> ColumnElement:
        fields = self._get_db_fields(self.fields, property_map)
        _, class_ = join_relations(None, class_, fields[:-1], set())
        column = getattr(class_, fields[-1])
        return column >= self.value

class EqualsFilter(Filter):
    OP = "="

    def build_condition(self, class_: type, property_map: dict) -> ColumnElement:
        fields = self._get_db_fields(self.fields, property_map)
        _, class_ = join_relations(None, class_, fields[:-1], set())
        column = getattr(class_, fields[-1])
        return column == self.value

    def is_valid(self)->bool:
        allowed = (str, int, datetime.date, bool, None.__class__)
        try:
            assert isinstance(self.value, allowed)
        except AssertionError:
            raise ValueError(f"{self} requires a string, int, bool or datetime value", None)

class InFilter(Filter):
    OP = "in"

    def build_condition(self, class_: type, property_map: dict) -> ColumnElement:
        fields = self._get_db_fields(self.fields, property_map)
        _, class_ = join_relations(None, class_, fields[:-1], set())
        column = getattr(class_, fields[-1])
        return column.in_(self.value)

    def is_valid(self)->bool:
        try:
            _ = (e for e in self.value)
        except TypeError:
            raise ValueError(f"{self} must be an iterable", None)

class NotEqualsFilter(Filter):
    OP = "!="

    def build_condition(self, class_: type, property_map: dict) -> ColumnElement:
        fields = self._get_db_fields(self.fields, property_map)
        _, class_ = join_relations(None, class_, fields[:-1], set())
        column = getattr(class_, fields[-1])
        return column != self.value

    def is_valid(self)->bool:
        allowed = (str, int, datetime.date, None.__class__)
        try:
            assert isinstance(self.value, allowed)
        except AssertionError:
            raise ValueError(f"{self} requires a string or int value", None)

class LikeFilter(Filter):
    OP = "like"

    def build_condition(self, class_: type, property_map: dict) -> ColumnElement:
        fields = self._get_db_fields(self.fields, property_map)
        _, class_ = join_relations(None, class_, fields[:-1], set())
        column = getattr(class_, fields[-1])
        return column.like(self.value)

    def is_valid(self)->bool:
        try:
            assert isinstance(self.value, str)
        except AssertionError:
            raise ValueError(f"{self} requires a string with a wildcard", None)