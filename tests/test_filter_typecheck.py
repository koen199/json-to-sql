import datetime

from fastapi_filter.schemas import FilterSchema, deserialize_filters
from fastapi_filter import filters

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
    assert isinstance(f.value, datetime.date)

def test_ltfilter_accepts_datetimes():
    json = {"field": "weight", "op": "<", "value": "2018-12-15T08:00:23"}
    [f] = deserialize_filters([FilterSchema(**json)])
    assert isinstance(f, filters.LTFilter)
    assert isinstance(f.value, datetime.datetime)


