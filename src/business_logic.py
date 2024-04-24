from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Text, Date, TIMESTAMP

Base = declarative_base()

class SerializableBase(Base):
    __abstract__ = True
    
    def to_dict(self):
        return dict((col.name, getattr(self, col.name)) for col in self.__table__.columns)

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__}({self.to_dict()})>"


class Candidate(SerializableBase):
    __tablename__ = 'candidates'
    candidate_id = Column(String(255), primary_key=True)
    candidate_name = Column(String(255))
    party_affiliation = Column(String(255))
    place_of_birth = Column(String(255))
    promises = Column(Text)
    photo_url = Column(Text)


class Voter(SerializableBase):
    __tablename__ = 'voters'
    voter_id = Column(String(255), primary_key=True)
    voter_name = Column(String(255))
    date_of_birth = Column(Date)
    gender = Column(String(255))
    nationality = Column(String(255))
    registration_number = Column(String(255))
    address_street = Column(String(255))
    address_city = Column(String(255))
    address_state = Column(String(255))
    address_country = Column(String(255))
    address_postcode = Column(String(255))
    email = Column(String(255))
    phone_number = Column(String(255))
    picture = Column(Text)
    registered_age = Column(Integer)


class Vote(SerializableBase):
    __tablename__ = 'votes'
    voter_id = Column(String(255), primary_key=True, unique=True)
    candidate_id = Column(String(255), primary_key=True)
    voting_time = Column(TIMESTAMP)
    vote = Column(Integer, default=1)
