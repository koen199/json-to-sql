import json_to_sql
from tests.petstore import Dog


def test_no_filter_no_order(sqlserver_session_factory, dogs):
    session = sqlserver_session_factory()
    filters = []
    results = json_to_sql.build_query(session, Dog, filters).all()
    expected_order = [1,2,3,4,5]
    assert expected_order == [dog.id for dog in results]

def test_no_filter_order_by_name(sqlserver_session_factory, dogs):
    session = sqlserver_session_factory()
    filters = []
    results = json_to_sql.build_query(session, Dog, filters, order_by=Dog.name).all()
    expected_order = [2, 4, 5, 3, 1]    
    assert expected_order == [dog.id for dog in results]

def test_no_filter_order_by_name_as_string(sqlserver_session_factory, dogs):
    session = sqlserver_session_factory()
    filters = []
    results = json_to_sql.build_query(session, Dog, filters, order_by='name').all()
    expected_order = [2, 4, 5, 3, 1]    
    assert expected_order == [dog.id for dog in results]

def test_no_filter_order_by_weight(sqlserver_session_factory, dogs):
    session = sqlserver_session_factory()
    filters = []
    results = json_to_sql.build_query(session, Dog, filters, order_by=Dog.weight).all()
    expected_order = [2, 5, 4, 3, 1]
    assert expected_order == [dog.id for dog in results]

def test_no_filter_order_by_weight_as_string(sqlserver_session_factory, dogs):
    session = sqlserver_session_factory()
    filters = []
    results = json_to_sql.build_query(session, Dog, filters, order_by="weight").all()
    expected_order = [2, 5, 4, 3, 1]
    assert expected_order == [dog.id for dog in results]