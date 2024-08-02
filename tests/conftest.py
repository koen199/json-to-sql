import sqlalchemy
from sqlalchemy.orm import sessionmaker
import pytest
from datetime import date

from tests import petstore

def create_engine():
    engine = sqlalchemy.create_engine('sqlite:///:memory:')
    return engine

def create_sessionmaker(engine):
    return sessionmaker(bind=engine)

@pytest.fixture(scope="session")
def sqllite_db():
    engine = create_engine()
    yield engine

@pytest.fixture
def sqlserver_session_factory(sqllite_db):
    petstore.Base.metadata.create_all(sqllite_db)
    yield create_sessionmaker(sqllite_db)
    petstore.Base.metadata.drop_all(sqllite_db)

@pytest.fixture
def dogs(sqlserver_session_factory):
    session =  sqlserver_session_factory()
    ball = petstore.Toy(name='ball')
    rope = petstore.Toy(name='rope')
    squicky_toy = petstore.Toy(name='squicky toy')
    doggos = [
        petstore.Dog(
            name="Xocomil", dob=date(1990, 12, 16), weight=100,
            toys=[ball, rope], 
            address=petstore.Address(streetname='Molenstraat', number=40)
        ),
        petstore.Dog(
            name="Jasmine", dob=date(1997, 4, 20), weight=40,
            toys=[ball, squicky_toy], 
            address=petstore.Address(streetname='Spoorweglaan', number=153)

        ),
        petstore.Dog(name="Quick", dob=date(2000, 5, 24), weight=90),
        petstore.Dog(name="Jinx", dob=date(2005, 12, 31), weight=55),
        petstore.Dog(name="Kaya", dob=None, weight=50)
    ]
    session.add_all(doggos)
    session.commit()