from tests.petstore import Dog
def test_kak(sqlserver_session_factory, dogs):
    session = sqlserver_session_factory()
    result = session.query(Dog).all()
    assert True