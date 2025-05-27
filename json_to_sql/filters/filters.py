import abc
import datetime
import logging
from sqlalchemy.sql.expression import Select
from sqlalchemy import orm
from sqlalchemy import inspect
from sqlalchemy.sql.visitors import ClauseVisitor
from sqlalchemy import Column


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

    
    
class Filter(abc.ABC):
    OP = None

    def __init__(self, filter_data:'FilterSchema')->'Filter':
        self.nested = None
        self.fields:list[str] = filter_data.field.split('.')
        self.value = self._date_or_value(filter_data.value)
        self.condition_group = filter_data.condition_group
        self.is_valid()

    def __repr__(self)->str:
        return f"<{type(self).__name__}(field='{'.'.join(self.fields)}', op='{self.OP}'" \
               f", value={self.value})>"

    def __eq__(self, other)->bool:
        return hash(self) == hash(other)

    def __hash__(self)->int:
        return hash((self.field, self.OP, self.value))

    @abc.abstractmethod
    def apply(self, stmt:'Select', attrib:Column, property_map:dict)->'Select':
        raise NotImplementedError('apply is an abstract method')

    @abc.abstractmethod
    def is_valid(self)->bool:
        raise NotImplementedError('is_valid is an abstract method')

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

    def apply(self, stmt:'Select', attrib:Column, property_map:dict)->'Select':
        stmt = stmt.where(attrib < self.value)
        return stmt
class LTEFilter(RelativeComparator):
    OP = "<="

    def apply(self, stmt:'Select', attrib:Column, property_map:dict)->'Select':
        stmt = stmt.where(attrib <= self.value)
        return stmt

class GTFilter(RelativeComparator):
    OP = ">"

    def apply(self, stmt:'Select', attrib:Column, property_map:dict)->'Select':
        stmt = stmt.where(attrib > self.value)
        return stmt

class GTEFilter(RelativeComparator):
    OP = ">="

    def apply(self, stmt:'Select', attrib:Column, property_map:dict)->'Select':
        stmt = stmt.where(attrib >= self.value)
        return stmt
class EqualsFilter(Filter):
    OP = "="

    def apply(self, stmt:'Select', attrib:Column, property_map:dict)->'Select':
        stmt = stmt.where(attrib == self.value)
        return stmt

    def is_valid(self)->bool:
        allowed = (str, int, datetime.date, bool, None.__class__)
        try:
            assert isinstance(self.value, allowed)
        except AssertionError:
            raise ValueError(f"{self} requires a string, int, bool or datetime value", None)

class InFilter(Filter):
    OP = "in"

    def apply(self, stmt:'Select', attrib:Column, property_map:dict)->'Select':
        stmt = stmt.where(attrib.in_(list(self.value)))
        return stmt

    def is_valid(self)->bool:
        try:
            _ = (e for e in self.value)
        except TypeError:
            raise ValueError(f"{self} must be an iterable", None)

class NotEqualsFilter(Filter):
    OP = "!="

    def apply(self, stmt:'Select', attrib:Column, property_map:dict)->'Select':
        stmt = stmt.where(attrib != self.value)
        return stmt

    def is_valid(self)->bool:
        allowed = (str, int, datetime.date, None.__class__)
        try:
            assert isinstance(self.value, allowed)
        except AssertionError:
            raise ValueError(f"{self} requires a string or int value", None)


class LikeFilter(Filter):
    OP = "like"

    def apply(self, stmt:'Select', attrib:Column, property_map:dict)->'Select':
        stmt = stmt.where(attrib.like(self.value))
        return stmt

    def is_valid(self)->bool:
        try:
            assert isinstance(self.value, str)
        except AssertionError:
            raise ValueError(f"{self} requires a string with a wildcard", None)