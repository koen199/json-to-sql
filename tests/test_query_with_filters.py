from datetime import date, datetime

from tests.petstore import Dog
import json_to_sql
from json_to_sql.schemas import FilterSchema

def test_name_equalsfilter(sqlserver_session_factory, dogs):
    session = sqlserver_session_factory()
    filters = [
        FilterSchema(field="name", op="=", value="Xocomil")
    ]
    stmt = json_to_sql.build_query(Dog, filters)
    results = session.scalars(stmt).all()
    assert len(results) == 1 
    assert results[0].name == 'Xocomil'
    
def test_equalsfilter_single_nested(sqlserver_session_factory, dogs):
    session = sqlserver_session_factory()
    filters = [
        FilterSchema(field="toys.name", op="=", value="rope")
    ]
    stmt = json_to_sql.build_query(Dog, filters)
    results = session.scalars(stmt).all()
    assert len(results) == 1 
    assert results[0].name == 'Xocomil'


def test_name_likefilter(sqlserver_session_factory, dogs):
    session = sqlserver_session_factory()
    filters = [
        FilterSchema(field="name", op="like", value="J%")
    ]
    stmt = json_to_sql.build_query(Dog, filters)
    results = session.scalars(stmt).all()
    assert len(results) == 2
    assert results[0].name == 'Jasmine'
    assert results[1].name == 'Jinx'

def test_name_notequalsfilter(sqlserver_session_factory, dogs):
    session = sqlserver_session_factory()
    filters = [
        FilterSchema(field="name", op="!=", value="Xocomil")
    ]
    stmt = json_to_sql.build_query(Dog, filters)
    results = session.scalars(stmt).all()
    assert len(results) == 4

def test_name_in_filter(sqlserver_session_factory, dogs):
    session = sqlserver_session_factory()
    filters = [
        FilterSchema(field="name", op="in", value=["Jinx", "Kaya"])
    ]
    stmt = json_to_sql.build_query(Dog, filters)
    results = session.scalars(stmt).all()
    assert len(results) == 2
    
def test_nested_in_filter(sqlserver_session_factory, dogs):
    session = sqlserver_session_factory()
    filters = [
        FilterSchema(field="toys.name", op="in", value=["ball", "rope"])
    ]
    stmt = json_to_sql.build_query(Dog, filters)
    results = session.scalars(stmt).unique().all()
    assert len(results) == 2

def test_dob_filter(sqlserver_session_factory, dogs):
    session = sqlserver_session_factory()
    min_date = date(2002, 1, 1).isoformat()
    filters = [
        FilterSchema(field="dateOfBirth", op="<", value=min_date)
    ]
    stmt = json_to_sql.build_query(Dog, filters, property_map={'dateOfBirth': 'dob'})
    results = session.scalars(stmt).all()
    assert len(results) == 3

def test_dob_filter_datetime(sqlserver_session_factory, dogs):
    session = sqlserver_session_factory()
    min_date = datetime(2002, 1, 1).isoformat()
    filters = [
        FilterSchema(field="dateOfBirth", op="<", value=min_date)
    ]
    stmt = json_to_sql.build_query(Dog, filters, property_map={'dateOfBirth': 'dob'})
    results = session.scalars(stmt).all()
    assert len(results) == 3

def test_dob_null_equalsfilter(sqlserver_session_factory, dogs):
    session = sqlserver_session_factory()
    filters = [
        FilterSchema(field="dateOfBirth", op="=", value=None)
    ]
    stmt = json_to_sql.build_query(Dog, filters, property_map={'dateOfBirth': 'dob'})
    results = session.scalars(stmt).all()
    assert results[0].name == 'Kaya'

def test_dob_null_notequalsfilter(sqlserver_session_factory, dogs):
    session = sqlserver_session_factory()
    filters = [
        FilterSchema(field="dateOfBirth", op="!=", value=None)
    ]
    stmt = json_to_sql.build_query(Dog, filters, property_map={'dateOfBirth': 'dob'})
    results = session.scalars(stmt).all()
    assert len(results) == 4

def test_weight_ltfilter(sqlserver_session_factory, dogs):
    session = sqlserver_session_factory()
    filters = [
        FilterSchema(field="weight", op="<", value=50)
    ]
    stmt = json_to_sql.build_query(Dog, filters)
    results = session.scalars(stmt).all()
    assert len(results) == 1


def test_weight_ltefilter(sqlserver_session_factory, dogs):
    session = sqlserver_session_factory()
    filters = [
        FilterSchema(field="weight", op="<=", value=50)
    ]
    stmt = json_to_sql.build_query(Dog, filters)
    results = session.scalars(stmt).all()
    assert len(results) == 2

def test_weight_gtfilter(sqlserver_session_factory, dogs):
    session = sqlserver_session_factory()
    filters = [
        FilterSchema(field="weight", op=">", value=90)
    ]
    stmt = json_to_sql.build_query(Dog, filters)
    results = session.scalars(stmt).all()
    assert len(results) == 1

def test_weight_gtefilter(sqlserver_session_factory, dogs):
    session = sqlserver_session_factory()
    filters = [
        FilterSchema(field="weight", op=">=", value=90)
    ]
    stmt = json_to_sql.build_query(Dog, filters)
    results = session.scalars(stmt).all()
    assert len(results) == 2

def test_address_nested_eq_filter_scalar_attribute(sqlserver_session_factory, dogs):
    session = sqlserver_session_factory()
    filters = [
        FilterSchema(field="address.streetname", op="=", value='Spoorweglaan')
    ]
    stmt = json_to_sql.build_query(Dog, filters)
    results = session.scalars(stmt).all()
    assert len(results) == 1

def test_address_nested_eq_filter_collections_attribute(sqlserver_session_factory, dogs):
    session = sqlserver_session_factory()
    filters = [
        FilterSchema(field="toys.name", op="=", value='rope')
    ]
    stmt = json_to_sql.build_query(Dog, filters)
    results = session.scalars(stmt).all()
    assert len(results) == 1
    
def test_test_address_nested_eq_filter_collections_attribute_with_mappings(sqlserver_session_factory, dogs):
    session = sqlserver_session_factory()
    filters = [
        FilterSchema(field="toys!.name!", op="=", value='rope')
    ]
    property_map = {'toys!':'toys', 'name!':'name'}
    stmt = json_to_sql.build_query(Dog, filters, property_map=property_map)
    results = session.scalars(stmt).all()
    assert len(results) == 1
    
def test_multiple_filters(sqlserver_session_factory, dogs):
    session = sqlserver_session_factory()
    filters = [
        FilterSchema(field="weight", op=">=", value=50),
        FilterSchema(field="weight", op="<", value=56)
    ]
    stmt = json_to_sql.build_query(Dog, filters)
    results = session.scalars(stmt).all()
    assert len(results) == 2
    
def test_multiple_filters_avoid_double_join(sqlserver_session_factory, dogs):
    session = sqlserver_session_factory()
    filters = [
        FilterSchema(field="toys.name", op="=", value='rope'),
        FilterSchema(field="toys.manufacturer", op="=", value='Hasbro')
    ]
    stmt = json_to_sql.build_query(Dog, filters)
    results = session.scalars(stmt).all()
    assert len(results) == 1
    
    
def test_multiple_filters_with_condition_group(sqlserver_session_factory, dogs):
    session = sqlserver_session_factory()
    filters = [
        FilterSchema(field="toys.name", op="=", value='ball', condition_group='A'),
        FilterSchema(field="toys.name", op="=", value='rope', condition_group='B')
    ]
    stmt = json_to_sql.build_query(Dog, filters)
    results = session.scalars(stmt).all()
    assert len(results) == 1
    assert results[0].name == 'Xocomil'
    