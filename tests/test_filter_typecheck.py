import datetime
import pytest
import pydantic

from json_to_sql.schemas import FilterSchema, deserialize_filters
from json_to_sql import filters

def test_ltfilter_accepts_floats():
    json = {"field": "weight", "op": "<", "value": 10.24}
    [f] = deserialize_filters([FilterSchema(**json)])
    assert isinstance(f, filters.LTFilter)

def test_ltfilter_accepts_ints():
    json = {"field": "weight", "op": "<", "value": 10}
    [f] = deserialize_filters([FilterSchema(**json)])
    assert isinstance(f, filters.LTFilter)

def test_ltfilter_accepts_dates():
    json = {"field": "weight", "op": "<", "value": "2018-12-15"}
    [f] = deserialize_filters([FilterSchema(**json)])
    assert isinstance(f, filters.LTFilter)
    assert type(f.value) == datetime.date

def test_ltfilter_accepts_datetimes():
    json = {"field": "weight", "op": "<", "value": "2018-12-15T08:00:23"}
    [f] = deserialize_filters([FilterSchema(**json)])
    assert isinstance(f, filters.LTFilter)
    assert type(f.value) == datetime.datetime

def test_ltfilter_raises_validationerror_against_string():
    json = {"field": "weight", "op": "<", "value": "Fido"}
    with pytest.raises(pydantic.ValidationError):
        deserialize_filters([FilterSchema(**json)])

def test_ltefilter_accepts_floats():
    json = {"field": "weight", "op": "<=", "value": 10.24}
    [f] = deserialize_filters([FilterSchema(**json)])
    assert isinstance(f, filters.LTEFilter)

def test_ltefilter_accepts_ints():
    json = {"field": "weight", "op": "<=", "value": 10}
    [f] = deserialize_filters([FilterSchema(**json)])
    assert isinstance(f, filters.LTEFilter)

def test_ltefilter_accepts_dates():
    json = {"field": "dateOfBirth", "op": "<=", "value": "2018-12-15"}
    [f] = deserialize_filters([FilterSchema(**json)])
    assert isinstance(f, filters.LTEFilter)
    assert type(f.value) == datetime.date

def test_ltefilter_accepts_datetimes():
    json = {"field": "dateOfBirth", "op": "<=", "value": "2018-12-15T03:00:21"}
    [f] = deserialize_filters([FilterSchema(**json)])
    assert isinstance(f, filters.LTEFilter)
    assert type(f.value) == datetime.datetime

def test_ltefilter_raises_validationerror_against_string():
    json = {"field": "name", "op": "<=", "value": "Fido"}
    with pytest.raises(pydantic.ValidationError):
        deserialize_filters([FilterSchema(**json)])

def test_gtfilter_accepts_floats():
    json = {"field": "weight", "op": ">", "value": 10.24}
    [f] = deserialize_filters([FilterSchema(**json)])
    assert isinstance(f, filters.GTFilter)

def test_gtfilter_accepts_ints():
    json = {"field": "weight", "op": ">", "value": 10}
    [f] = deserialize_filters([FilterSchema(**json)])
    assert isinstance(f, filters.GTFilter)

def test_gtfilter_accepts_dates():
    json = {"field": "dateOfBirth", "op": ">", "value": "2018-12-15"}
    [f] = deserialize_filters([FilterSchema(**json)])
    assert isinstance(f, filters.GTFilter)
    assert type(f.value) == datetime.date

def test_gtfilter_accepts_datetimes():
    json = {"field": "dateOfBirth", "op": ">", "value": "2018-12-15T03:21:00"}
    [f] = deserialize_filters([FilterSchema(**json)])
    assert isinstance(f, filters.GTFilter)
    assert type(f.value) == datetime.datetime

def test_gtfilter_raises_validationerror_against_string():
    json = {"field": "name", "op": ">", "value": "Fido"}
    with pytest.raises(pydantic.ValidationError):
        deserialize_filters([FilterSchema(**json)])

def test_gtefilter_accepts_floats():
    json = {"field": "weight", "op": ">=", "value": 10.24}
    [f] = deserialize_filters([FilterSchema(**json)])
    assert isinstance(f, filters.GTEFilter)

def test_gtefilter_accepts_ints():
    json = {"field": "weight", "op": ">=", "value": 10}
    [f] = deserialize_filters([FilterSchema(**json)])
    assert isinstance(f, filters.GTEFilter)

def test_gtefilter_accepts_dates():
    json = {"field": "dateOfBirth", "op": ">=", "value": "2018-12-15"}
    [f] = deserialize_filters([FilterSchema(**json)])
    assert isinstance(f, filters.GTEFilter)
    assert type(f.value) == datetime.date

def test_gtefilter_accepts_datetimes():
    json = {"field": "dateOfBirth", "op": ">=", "value": "2018-12-15T03:01:03"}
    [f] = deserialize_filters([FilterSchema(**json)])
    assert isinstance(f, filters.GTEFilter)
    assert type(f.value) == datetime.datetime

def test_gtefilter_raises_validationerror_against_string():
    json = {"field": "name", "op": ">=", "value": "Fido"}
    with pytest.raises(pydantic.ValidationError):
        deserialize_filters([FilterSchema(**json)])

def test_equalfilter_accepts_ints():
    json = {"field": "weight", "op": "=", "value": 10}
    [f] = deserialize_filters([FilterSchema(**json)])
    assert isinstance(f, filters.EqualsFilter)

def test_equalfilter_accepts_strings():
    json = {"field": "name", "op": "=", "value": "Fido"}
    [f] = deserialize_filters([FilterSchema(**json)])
    assert isinstance(f, filters.EqualsFilter)

def test_equalfilter_accepts_dates():
    json = {"field": "name", "op": "=", "value": "2018-12-15"}
    [f] = deserialize_filters([FilterSchema(**json)])
    assert isinstance(f, filters.EqualsFilter)
    assert type(f.value) == datetime.date

def test_equalfilter_accepts_datetimes():
    json = {"field": "name", "op": "=", "value": "2018-12-15T01:00:23"}
    [f] = deserialize_filters([FilterSchema(**json)])
    assert isinstance(f, filters.EqualsFilter)
    assert type(f.value) == datetime.datetime

def test_equalfilter_accepts_none():
    json = {"field": "name", "op": "=", "value": None}
    [f] = deserialize_filters([FilterSchema(**json)])
    assert isinstance(f, filters.EqualsFilter)

def test_equalsfilter_raises_validationerror_against_float():
    json = {"field": "weight", "op": "=", "value": 12.345}
    with pytest.raises(pydantic.ValidationError):
        deserialize_filters([FilterSchema(**json)])

def test_infilter_accepts_list_of_ints():
    json = {"field": "weight", "op": "in", "value": [1, 2, 3]}
    [f] = deserialize_filters([FilterSchema(**json)])
    assert isinstance(f, filters.InFilter)

def test_infilter_accepts_list_of_strings():
    json = {"field": "weight", "op": "in", "value": ['A', 'B', 'C']}
    [f] = deserialize_filters([FilterSchema(**json)])
    assert isinstance(f, filters.InFilter)

def test_infilter_raises_typeerror_on_non_iterable():
    dates = datetime.date(2019, 3, 17)
    json = {"field": "dateOfBirth", "op": "in", "value": dates}
    with pytest.raises(pydantic.ValidationError):    
        [f] = deserialize_filters([FilterSchema(**json)])

def test_notequalsfilter_accepts_string():
    json = {"field": "name", "op": "!=", "value": "Fido"}
    [f] = deserialize_filters([FilterSchema(**json)])
    assert isinstance(f, filters.NotEqualsFilter)

def test_notequalsfilter_accepts_int():
    json = {"field": "age", "op": "!=", "value": 10}
    [f] = deserialize_filters([FilterSchema(**json)])
    assert isinstance(f, filters.NotEqualsFilter)

def test_notequalsfilter_accepts_date():
    json = {"field": "name", "op": "!=", "value": "2018-12-15"}
    [f] = deserialize_filters([FilterSchema(**json)])
    assert isinstance(f, filters.NotEqualsFilter)
    assert type(f.value) == datetime.date

def test_notequalsfilter_accepts_datetime():
    json = {"field": "name", "op": "!=", "value": "2018-12-15T10:21:33"}
    [f] = deserialize_filters([FilterSchema(**json)])
    assert isinstance(f, filters.NotEqualsFilter)
    assert type(f.value) == datetime.datetime

def test_notequalsfilter_accepts_none():
    json = {"field": "name", "op": "!=", "value": None}
    [f] = deserialize_filters([FilterSchema(**json)])
    assert isinstance(f, filters.NotEqualsFilter)

def test_notequalsfilter_fails_on_float():
    json = {"field": "age", "op": "!=", "value": 10.234}
    with pytest.raises(pydantic.ValidationError):
        deserialize_filters([FilterSchema(**json)])

def test_likefilter_accepts_strings():
    json = {"field": "name", "op": "like", "value": "Fido%"}
    [f] = deserialize_filters([FilterSchema(**json)])
    assert isinstance(f, filters.LikeFilter)

def test_likefilter_fails_on_int():
    json = {"field": "age", "op": "like", "value": 4}
    with pytest.raises(pydantic.ValidationError):
        deserialize_filters([FilterSchema(**json)])

def test_likefilter_fails_on_float():
    json = {"field": "age", "op": "like", "value": 4.20}
    with pytest.raises(pydantic.ValidationError):
        deserialize_filters([FilterSchema(**json)])

def test_likefilter_fails_on_date():
    json = {"field": "dateOfBirth", "op": "like", "value": "2018-12-15"}
    with pytest.raises(pydantic.ValidationError):
        deserialize_filters([FilterSchema(**json)])

def test_filter_schema_raises_validationerror_on_bad_op():
    json = {"field": "weight", "op": "ne", "value": 124}
    with pytest.raises(pydantic.ValidationError):
        deserialize_filters([FilterSchema(**json)])
