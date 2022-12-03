import abc
import re
import datetime
import logging
import pydantic

from typing import Any, TYPE_CHECKING
if TYPE_CHECKING:
    from sqlalchemy.orm import Query
    from ..schemas import FilterSchema



logger = logging.getLogger(__name__)
RE_DATE = "^([0-9]{4})-([0-9]|1[0-2]|0[1-9])-([1-9]|0[1-9]|1[0-9]|2[1-9]|3[0-1])$"


class Filter(abc.ABC):
    OP = None

    def __init__(self, field: str, value: Any)->'Filter':
        self.nested = None
        self.set_field(field)
        self.value = self._date_or_value(value)
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
    def apply(self, query:Query, class_:type, property_map:pydantic.BaseModel)->Query:
        raise NotImplementedError('apply is an abstract method')

    @abc.abstractmethod
    def is_valid(self)->bool:
        raise NotImplementedError('is_valid is an abstract method')

    def _get_db_field(self, property_map:pydantic.BaseModel)->str:
        """ private method to convert JSON field to SQL column

        :param schema: optional Marshmallow schema to map field -> column
        :return: string field name
        """
        if not property_map:
            return self.field
        attr = getattr(property_map, self.field) #maps exteral fieldname to database field
        if not attr:
            raise pydantic.ValidationError(f"'{attr}' is not a valid field")
        return attr or self.field

    def _date_or_value(self, value:Any)->Any:
        if not isinstance(value, str):
            return value
        if re.match(RE_DATE, value):
            return datetime.datetime.strptime(value, "%Y-%m-%d").date()
        return value

class RelativeComparator(Filter):

    def is_valid(self)->bool:
        try:
            allowed = (int, float, datetime.date, datetime.datetime)
            assert isinstance(self.value, allowed)
        except AssertionError:
            raise pydantic.ValidationError(f"{self} requires an ordinal value")

class LTFilter(RelativeComparator):
    OP = "<"

    def apply(self, query:Query, class_:type, schema:FilterSchema)->Query:
        field = self._get_db_field(schema)
        return query.filter(getattr(class_, field) < self.value)

