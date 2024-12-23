import json_to_sql
from tests.petstore import Dog
from sqlalchemy.orm import Session
from datetime import date


def test_no_filter_no_order(sqlserver_session_factory, dogs):
    session = sqlserver_session_factory()
    filters = []
    stmt = json_to_sql.build_query(Dog, filters)
    results = session.scalars(stmt).all()
    expected_order = [1,2,3,4,5]
    assert expected_order == [dog.id for dog in results]

def test_no_filter_order_by_name(sqlserver_session_factory:Session, dogs):
    session = sqlserver_session_factory()
    filters = []
    stmt = json_to_sql.build_query(Dog, filters, order_by=[Dog.name])
    results = session.scalars(stmt).all()
    expected_order = [2, 4, 5, 3, 1]    
    assert expected_order == [dog.id for dog in results]

def test_no_filter_order_by_name_as_string(sqlserver_session_factory:Session, dogs):
    session = sqlserver_session_factory()
    filters = []
    stmt = json_to_sql.build_query(Dog, filters, order_by='name')
    results = session.scalars(stmt).all()
    expected_order = [2, 4, 5, 3, 1]    
    assert expected_order == [dog.id for dog in results]

def test_no_filter_order_by_weight(sqlserver_session_factory:Session, dogs):
    session = sqlserver_session_factory()
    filters = []
    stmt = json_to_sql.build_query(Dog, filters, order_by=[Dog.weight])
    results = session.scalars(stmt).all()
    expected_order = [2, 5, 4, 3, 1]
    assert expected_order == [dog.id for dog in results]

def test_no_filter_order_by_weight_as_string(sqlserver_session_factory:Session, dogs):
    session = sqlserver_session_factory()
    filters = []
    stmt = json_to_sql.build_query(Dog, filters, order_by="weight")
    results = session.scalars(stmt).all()
    expected_order = [2, 5, 4, 3, 1]
    assert expected_order == [dog.id for dog in results]

def test_no_filter_order_by_name_and_weight(sqlserver_session_factory:Session):
    session = sqlserver_session_factory()
    dogs = [
        Dog(
            name="A", dob=date(1990, 12, 16), weight=20
        ),
        Dog(
            name="B", dob=date(1997, 4, 20), weight=20,

        ),
        Dog(name="C", dob=date(2000, 5, 24), weight=40),
        Dog(name="D", dob=date(2005, 12, 31), weight=40)
    ]
    session.add_all(dogs)
    session.commit()
    
    filters = []
    stmt = json_to_sql.build_query(Dog, filters, order_by=[Dog.weight, Dog.name], is_desc=True)
    results = session.scalars(stmt).all()
    expected_order = [4,3,2,1]    
    assert expected_order == [dog.id for dog in results]
    
    stmt = json_to_sql.build_query(Dog, filters, order_by=[Dog.weight, Dog.name], is_desc=[True, False])
    results = session.scalars(stmt).all()
    expected_order = [3,4,1,2]    
    assert expected_order == [dog.id for dog in results]

def test_no_filter_order_by_name_and_weight_as_string(sqlserver_session_factory:Session, dogs):
    session = sqlserver_session_factory()
    filters = []
    stmt = json_to_sql.build_query(Dog, filters, order_by='name,weight')
    results = session.scalars(stmt).all()
    expected_order = [2, 4, 5, 3, 1]    
    assert expected_order == [dog.id for dog in results]