from fastapi_filter.schemas import FilterSchema
from fastapi_filter import query_with_filters
from tests.petstore import Dog


def test_no_filter_no_order(sqlserver_session_factory, dogs):
    session = sqlserver_session_factory()
    filters = []
    results = query_with_filters(session, Dog, filters)
    expected_order = [1,2,3,4,5]
    assert expected_order == [dog.id for dog in results]

def test_no_filter_order_by_name(sqlserver_session_factory, dogs):
    session = sqlserver_session_factory()
    filters = []
    results = query_with_filters(session, Dog, filters, order_by=Dog.name)
    expected_order = [2, 4, 5, 3, 1]    
    assert expected_order == [dog.id for dog in results]

def test_no_filter_order_by_name_as_string(sqlserver_session_factory, dogs):
    session = sqlserver_session_factory()
    filters = []
    results = query_with_filters(session, Dog, filters, order_by='name')
    expected_order = [2, 4, 5, 3, 1]    
    assert expected_order == [dog.id for dog in results]

def test_no_filter_order_by_weight(sqlserver_session_factory, dogs):
    session = sqlserver_session_factory()
    filters = []
    results = query_with_filters(session, Dog, filters, order_by=Dog.weight)
    expected_order = [2, 5, 4, 3, 1]
    assert expected_order == [dog.id for dog in results]