import abc
import datetime
import logging
from sqlalchemy.sql.expression import Select
from sqlalchemy import orm
from sqlalchemy import inspect
from sqlalchemy.sql.visitors import ClauseVisitor


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

# Visitor class to collect tables
class TableCollector(ClauseVisitor):
    def __init__(self):
        self.tables = set()

    def visit_table(self, table):
        self.tables.add(table.name)
    
def join_relations(stmt:Select, class_:Any, fields:list[str])->Select:
    nested_class_ = class_
    for field in fields:
        nested_class_  = getattr(nested_class_, field).mapper.class_
        if not already_joined(stmt, nested_class_):
            stmt = stmt.join(nested_class_)
    return stmt, nested_class_

def already_joined(stmt:Select, class_:Any):
    if not isinstance(stmt.get_final_froms()[0], orm.util._ORMJoin):
        #FROM clause contains no join
        return False
    collector = TableCollector()
    collector.traverse(stmt)
    
    tablename = inspect(class_).persist_selectable.name
    return tablename in collector.tables    
    
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

    @abc.abstractmethod
    def apply(self, stmt:'Select', class_:type, property_map:dict)->'Select':
        raise NotImplementedError('apply is an abstract method')

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
            return value #value is just some string 

class RelativeComparator(Filter):
    def is_valid(self)->bool:
        try:
            allowed = (int, float, datetime.date, datetime.datetime)
            assert isinstance(self.value, allowed)
        except AssertionError:
            raise ValueError(f"{self} requires an ordinal value", None)

class LTFilter(RelativeComparator):
    OP = "<"

    def apply(self, stmt:'Select', class_:type, property_map:dict)->'Select':
        fields = self._get_db_fields(self.fields, property_map)        
        stmt, class_ = join_relations(stmt, class_, fields[:-1])
        stmt = stmt.where(getattr(class_, fields[-1]) < self.value)
        return stmt
class LTEFilter(RelativeComparator):
    OP = "<="

    def apply(self, stmt:'Select', class_:type, property_map:dict)->'Select':
        fields = self._get_db_fields(self.fields, property_map)        
        stmt, class_ = join_relations(stmt, class_, fields[:-1])
        stmt = stmt.where(getattr(class_, fields[-1]) <= self.value)
        return stmt

class GTFilter(RelativeComparator):
    OP = ">"

    def apply(self, stmt:'Select', class_:type, property_map:dict)->'Select':
        fields = self._get_db_fields(self.fields, property_map)        
        stmt, class_ = join_relations(stmt, class_, fields[:-1])
        stmt = stmt.where(getattr(class_, fields[-1]) > self.value)
        return stmt

class GTEFilter(RelativeComparator):
    OP = ">="

    def apply(self, stmt:'Select', class_:type, property_map:dict)->'Select':
        fields = self._get_db_fields(self.fields, property_map)        
        stmt, class_ = join_relations(stmt, class_, fields[:-1])
        stmt = stmt.where(getattr(class_, fields[-1]) >= self.value)
        return stmt
class EqualsFilter(Filter):
    OP = "="

    def apply(self, stmt:'Select', class_:type, property_map:dict)->'Select':
        fields = self._get_db_fields(self.fields, property_map)   
        stmt, class_ = join_relations(stmt, class_, fields[:-1])
        stmt = stmt.where(getattr(class_, fields[-1]) == self.value)
        return stmt

    def is_valid(self)->bool:
        allowed = (str, int, datetime.date, bool, None.__class__)
        try:
            assert isinstance(self.value, allowed)
        except AssertionError:
            raise ValueError(f"{self} requires a string, int, bool or datetime value", None)

class InFilter(Filter):
    OP = "in"

    def apply(self, stmt:'Select', class_:type, property_map:dict)->'Select':
        fields = self._get_db_fields(self.fields, property_map)
        stmt, class_ = join_relations(stmt, class_, fields[:-1])
        stmt = stmt.where(getattr(class_, fields[-1]).in_(list(self.value)))
        return stmt

    def is_valid(self)->bool:
        try:
            _ = (e for e in self.value)
        except TypeError:
            raise ValueError(f"{self} must be an iterable", None)

class NotEqualsFilter(Filter):
    OP = "!="

    def apply(self, stmt:'Select', class_:type, property_map:dict)->'Select':
        fields = self._get_db_fields(self.fields, property_map)
        stmt, class_ = join_relations(stmt, class_, fields[:-1])
        stmt = stmt.where(getattr(class_, fields[-1]) != self.value)
        return stmt

    def is_valid(self)->bool:
        allowed = (str, int, datetime.date, None.__class__)
        try:
            assert isinstance(self.value, allowed)
        except AssertionError:
            raise ValueError(f"{self} requires a string or int value", None)


class LikeFilter(Filter):
    OP = "like"

    def apply(self, stmt:'Select', class_:type, property_map:dict)->'Select':
        fields = self._get_db_fields(self.fields, property_map)        
        stmt, class_ = join_relations(stmt, class_, fields[:-1])
        stmt = stmt.where(getattr(class_, fields[-1]).like(self.value))
        return stmt

    def is_valid(self)->bool:
        try:
            assert isinstance(self.value, str)
        except AssertionError:
            raise ValueError(f"{self} requires a string with a wildcard", None)