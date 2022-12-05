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

