from datetime import date, datetime
import pydantic

from tests.petstore import Dog
from fastapi_filter import query_with_filters
from fastapi_filter.schemas import FilterSchema

def test_name_equalsfilter(sqlserver_session_factory, dogs):
    session = sqlserver_session_factory()
    filters = [
        FilterSchema(field="name", op="=", value="Xocomil")
    ]
    results = query_with_filters(session, Dog, filters)
    assert len(results) == 1 
    assert results[0].name == 'Xocomil'

def test_name_likefilter(sqlserver_session_factory, dogs):
    session = sqlserver_session_factory()
    filters = [
        FilterSchema(field="name", op="like", value="J%")
    ]
    results = query_with_filters(session, Dog, filters)
    assert len(results) == 2
    assert results[0].name == 'Jasmine'
    assert results[1].name == 'Jinx'

def test_name_notequalsfilter(sqlserver_session_factory, dogs):
    session = sqlserver_session_factory()
    filters = [
        FilterSchema(field="name", op="!=", value="Xocomil")
    ]
    results = query_with_filters(session, Dog, filters)
    assert len(results) == 4

def test_name_in_filter(sqlserver_session_factory, dogs):
    session = sqlserver_session_factory()
    filters = [
        FilterSchema(field="name", op="in", value=["Jinx", "Kaya"])
    ]
    results = query_with_filters(session, Dog, filters)
    assert len(results) == 2

def test_dob_filter(sqlserver_session_factory, dogs):
    session = sqlserver_session_factory()
    min_date = date(2002, 1, 1).isoformat()
    filters = [
        FilterSchema(field="dateOfBirth", op="<", value=min_date)
    ]
    results = query_with_filters(session, Dog, filters, property_map={'dateOfBirth': 'dob'})
    assert len(results) == 3

def test_dob_filter_datetime(sqlserver_session_factory, dogs):
    session = sqlserver_session_factory()
    min_date = datetime(2002, 1, 1).isoformat()
    filters = [
        FilterSchema(field="dateOfBirth", op="<", value=min_date)
    ]
    results = query_with_filters(session, Dog, filters, property_map={'dateOfBirth': 'dob'})
    assert len(results) == 3

def test_dob_null_equalsfilter(sqlserver_session_factory, dogs):
    session = sqlserver_session_factory()
    filters = [
        FilterSchema(field="dateOfBirth", op="=", value=None)
    ]
    results = query_with_filters(session, Dog, filters, property_map={'dateOfBirth': 'dob'})
    assert results[0].name == 'Kaya'

def test_dob_null_notequalsfilter(sqlserver_session_factory, dogs):
    session = sqlserver_session_factory()
    filters = [
        FilterSchema(field="dateOfBirth", op="!=", value=None)
    ]
    results = query_with_filters(session, Dog, filters, property_map={'dateOfBirth': 'dob'})
    assert len(results) == 4

def test_weight_ltfilter(sqlserver_session_factory, dogs):
    session = sqlserver_session_factory()
    filters = [
        FilterSchema(field="weight", op="<", value=50)
    ]
    results = query_with_filters(session, Dog, filters)
    assert len(results) == 1


def test_weight_ltefilter(sqlserver_session_factory, dogs):
    session = sqlserver_session_factory()
    filters = [
        FilterSchema(field="weight", op="<=", value=50)
    ]
    results = query_with_filters(session, Dog, filters)
    assert len(results) == 2

def test_weight_gtfilter(sqlserver_session_factory, dogs):
    session = sqlserver_session_factory()
    filters = [
        FilterSchema(field="weight", op=">", value=90)
    ]
    results = query_with_filters(session, Dog, filters)
    assert len(results) == 1

def test_weight_gtefilter(sqlserver_session_factory, dogs):
    session = sqlserver_session_factory()
    filters = [
        FilterSchema(field="weight", op=">=", value=90)
    ]
    results = query_with_filters(session, Dog, filters)
    assert len(results) == 2

def test_toys_containsfilter(sqlserver_session_factory, dogs):
    session = sqlserver_session_factory()
    filters = [
        FilterSchema(field="toys.name", op="contains", value='ball')
    ]
    results = query_with_filters(session, Dog, filters)
    assert len(results) == 2

def test_address_nested_eq_filter(sqlserver_session_factory, dogs):
    session = sqlserver_session_factory()
    filters = [
        FilterSchema(field="address.streetname", op="=", value='Spoorweglaan')
    ]
    results = query_with_filters(session, Dog, filters)
    assert len(results) == 1