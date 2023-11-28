from datetime import date, datetime

from tests.petstore import Dog
import json_to_sql
from json_to_sql.schemas import FilterSchema

def test_name_equalsfilter(sqlserver_session_factory, dogs):
    session = sqlserver_session_factory()
    filters = [
        FilterSchema(field="name", op="=", value="Xocomil")
    ]
    results = json_to_sql.build_query(session, Dog, filters).all()
    assert len(results) == 1 
    assert results[0].name == 'Xocomil'

def test_name_likefilter(sqlserver_session_factory, dogs):
    session = sqlserver_session_factory()
    filters = [
        FilterSchema(field="name", op="like", value="J%")
    ]
    results = json_to_sql.build_query(session, Dog, filters).all()
    assert len(results) == 2
    assert results[0].name == 'Jasmine'
    assert results[1].name == 'Jinx'

def test_name_notequalsfilter(sqlserver_session_factory, dogs):
    session = sqlserver_session_factory()
    filters = [
        FilterSchema(field="name", op="!=", value="Xocomil")
    ]
    results = json_to_sql.build_query(session, Dog, filters).all()
    assert len(results) == 4

def test_name_in_filter(sqlserver_session_factory, dogs):
    session = sqlserver_session_factory()
    filters = [
        FilterSchema(field="name", op="in", value=["Jinx", "Kaya"])
    ]
    results = json_to_sql.build_query(session, Dog, filters).all()
    assert len(results) == 2

def test_dob_filter(sqlserver_session_factory, dogs):
    session = sqlserver_session_factory()
    min_date = date(2002, 1, 1).isoformat()
    filters = [
        FilterSchema(field="dateOfBirth", op="<", value=min_date)
    ]
    results = json_to_sql.build_query(session, Dog, filters, property_map={'dateOfBirth': 'dob'}).all()
    assert len(results) == 3

def test_dob_filter_datetime(sqlserver_session_factory, dogs):
    session = sqlserver_session_factory()
    min_date = datetime(2002, 1, 1).isoformat()
    filters = [
        FilterSchema(field="dateOfBirth", op="<", value=min_date)
    ]
    results = json_to_sql.build_query(session, Dog, filters, property_map={'dateOfBirth': 'dob'}).all()
    assert len(results) == 3

def test_dob_null_equalsfilter(sqlserver_session_factory, dogs):
    session = sqlserver_session_factory()
    filters = [
        FilterSchema(field="dateOfBirth", op="=", value=None)
    ]
    results = json_to_sql.build_query(session, Dog, filters, property_map={'dateOfBirth': 'dob'}).all()
    assert results[0].name == 'Kaya'

def test_dob_null_notequalsfilter(sqlserver_session_factory, dogs):
    session = sqlserver_session_factory()
    filters = [
        FilterSchema(field="dateOfBirth", op="!=", value=None)
    ]
    results = json_to_sql.build_query(session, Dog, filters, property_map={'dateOfBirth': 'dob'}).all()
    assert len(results) == 4

def test_weight_ltfilter(sqlserver_session_factory, dogs):
    session = sqlserver_session_factory()
    filters = [
        FilterSchema(field="weight", op="<", value=50)
    ]
    results = json_to_sql.build_query(session, Dog, filters).all()
    assert len(results) == 1


def test_weight_ltefilter(sqlserver_session_factory, dogs):
    session = sqlserver_session_factory()
    filters = [
        FilterSchema(field="weight", op="<=", value=50)
    ]
    results = json_to_sql.build_query(session, Dog, filters).all()
    assert len(results) == 2

def test_weight_gtfilter(sqlserver_session_factory, dogs):
    session = sqlserver_session_factory()
    filters = [
        FilterSchema(field="weight", op=">", value=90)
    ]
    results = json_to_sql.build_query(session, Dog, filters).all()
    assert len(results) == 1

def test_weight_gtefilter(sqlserver_session_factory, dogs):
    session = sqlserver_session_factory()
    filters = [
        FilterSchema(field="weight", op=">=", value=90)
    ]
    results = json_to_sql.build_query(session, Dog, filters).all()
    assert len(results) == 2

def test_toys_containsfilter(sqlserver_session_factory, dogs):
    session = sqlserver_session_factory()
    filters = [
        FilterSchema(field="toys.name", op="contains", value='ball')
    ]
    results = json_to_sql.build_query(session, Dog, filters).all()
    assert len(results) == 2

def test_address_nested_eq_filter_scalar_attribute(sqlserver_session_factory, dogs):
    session = sqlserver_session_factory()
    filters = [
        FilterSchema(field="address.streetname", op="=", value='Spoorweglaan')
    ]
    results = json_to_sql.build_query(session, Dog, filters).all()
    assert len(results) == 1

def test_address_nested_eq_filter_collections_attribute(sqlserver_session_factory, dogs):
    session = sqlserver_session_factory()
    filters = [
        FilterSchema(field="toys.name", op="=", value='rope')
    ]
    results = json_to_sql.build_query(session, Dog, filters).all()
    assert len(results) == 1