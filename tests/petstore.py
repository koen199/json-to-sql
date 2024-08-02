from sqlalchemy.orm import declarative_base, relationship
from sqlalchemy import Column, Integer, String, Float, Date, Table, ForeignKey
from pydantic import BaseModel
import datetime
from typing import List


Base = declarative_base()

dog_toys = Table(
    "dog_toys",
    Base.metadata, 
    Column("dog_id", Integer, ForeignKey("dog.id")),
    Column("toy_id", Integer, ForeignKey("toy.id"))
)

class Address(Base):
    __tablename__ = 'address'
    id = Column(Integer, primary_key=True)
    dog_id = Column("dog_id", Integer, ForeignKey("dog.id"))
    streetname = Column(String)
    number = Column(Integer)

class Toy(Base):
    __tablename__ = 'toy'
    id = Column(Integer, primary_key=True)
    dog_id = Column(Integer, ForeignKey('dog.id'))
    name = Column(String(32), unique=True)
    manufacturer = Column(String(32), default='Hasbro')

    def __repr__(self):
        return f"<Toy(id={self.id}, name='{self.name}')>"

class ToySchema(BaseModel):
    id:int
    name:str

class DogSchema(BaseModel):
    id:int
    name:str 
    dateOfBirth:datetime.date
    weight:float
    toys:List[ToySchema]

class Dog(Base):
    __tablename__ = 'dog'
    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True)
    dob = Column(Date)
    weight = Column(Float)

    toys = relationship("Toy", backref="dogs")
    address = relationship("Address", uselist=False)

    @property
    def age(self):
        return int((datetime.date.today() - self.dob).days / 365.25)

    def __repr__(self):
        return f"<Dog(id={self.id}, name='{self.name}')>"
