import abc
import re
import datetime
import logging
import pydantic

from typing import Any, TYPE_CHECKING, Union
if TYPE_CHECKING:
    from sqlalchemy.orm import Query
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
        self.set_field(filter_data.field)
        self.value = self._date_or_value(filter_data.value)
        self.is_valid()

    def __repr__(self)->str:
        return f"<{type(self).__name__}(field='{self.field}', op='{self.OP}'" \
               f", value={self.value})>"

    def __eq__(self, other)->bool:
        return hash(self) == hash(other)

    def __hash__(self)->int:
        return hash((self.field, self.OP, self.value))

    def set_field(self, field:str)->None:
        f = field.split(".")
        self.field = f[0]
        if len(f) == 2:
            self.nested = f[1]
        elif len(f) > 2:
            logger.warning(
                f"you supplied nested fields {f}. Only one level of nesting "
                f"is currently supported. ignoring fields {f[2:]}."
            )

    @abc.abstractmethod
    def apply(self, query:'Query', class_:type, property_map:dict)->'Query':
        raise NotImplementedError('apply is an abstract method')

    @abc.abstractmethod
    def is_valid(self)->bool:
        raise NotImplementedError('is_valid is an abstract method')

    def _get_db_field(self, property_map:dict)->str:
        if not property_map:
            return self.field
        try:
            attr = property_map[self.field]
        except KeyError:
            raise pydantic.ValidationError(f"'{attr}' is not a valid field", None)
        return attr or self.field

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
            raise pydantic.ValidationError(f"{self} requires an ordinal value", None)

class LTFilter(RelativeComparator):
    OP = "<"

    def apply(self, query:'Query', class_:type, property_map:dict)->'Query':
        field = self._get_db_field(property_map)
        return query.filter(getattr(class_, field) < self.value)

class LTEFilter(RelativeComparator):
    OP = "<="

    def apply(self, query:'Query', class_:type, property_map:dict)->'Query':
        field = self._get_db_field(property_map)
        return query.filter(getattr(class_, field) <= self.value)

class GTFilter(RelativeComparator):
    OP = ">"

    def apply(self, query:'Query', class_:type, property_map:dict)->'Query':
        field = self._get_db_field(property_map)
        return query.filter(getattr(class_, field) > self.value)

class GTEFilter(RelativeComparator):
    OP = ">="

    def apply(self, query:'Query', class_:type, property_map:dict)->'Query':
        field = self._get_db_field(property_map)
        return query.filter(getattr(class_, field) >= self.value)


class EqualsFilter(Filter):
    OP = "="

    def apply(self, query:'Query', class_:type, property_map:dict)->'Query':
        field = self._get_db_field(property_map)
        if self.nested:
            q = {self.nested: self.value}
            return query.filter(getattr(class_, field).has(**q))
        return query.filter(getattr(class_, field) == self.value)

    def is_valid(self)->bool:
        allowed = (str, int, datetime.date, None.__class__)
        try:
            assert isinstance(self.value, allowed)
        except AssertionError:
            raise pydantic.ValidationError(f"{self} requires a string or int value", None)

class InFilter(Filter):
    OP = "in"

    def apply(self, query:'Query', class_:type, property_map:dict)->'Query':
        field = self._get_db_field(property_map)
        return query.filter(getattr(class_, field).in_(list(self.value)))

    def is_valid(self)->bool:
        try:
            _ = (e for e in self.value)
        except TypeError:
            raise pydantic.ValidationError(f"{self} must be an iterable", None)

class NotEqualsFilter(Filter):
    OP = "!="

    def apply(self, query:'Query', class_:type, property_map:dict)->'Query':
        field = self._get_db_field(property_map)
        return query.filter(getattr(class_, field) != self.value)

    def is_valid(self)->bool:
        allowed = (str, int, datetime.date, None.__class__)
        try:
            assert isinstance(self.value, allowed)
        except AssertionError:
            raise pydantic.ValidationError(f"{self} requires a string or int value", None)


class LikeFilter(Filter):
    OP = "like"

    def apply(self, query:'Query', class_:type, property_map:dict)->'Query':
        field = self._get_db_field(property_map)
        return query.filter(getattr(class_, field).like(self.value))

    def is_valid(self)->bool:
        try:
            assert isinstance(self.value, str)
        except AssertionError:
            raise pydantic.ValidationError(f"{self} requires a string with a wildcard", None)

class ContainsFilter(Filter):
    OP = "contains"

    def apply(self, query:'Query', class_:type, property_map:dict)->'Query':
        subfield = self.nested or "id"
        q = {subfield: self.value}
        field = self._get_db_field(property_map)
        return query.filter(getattr(class_, field).any(**q))

    def is_valid(self):
        pass