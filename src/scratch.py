from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# Create an engine to connect to the database
engine = create_engine('sqlite:///voting_system.db', echo=True)

# Create a base class for declarative models
Base = declarative_base()

# Define a model class for the 'Voter' table
class Voter(Base):
    __tablename__ = 'voters'
    id = Column(Integer, primary_key=True)
    name = Column(String)
    age = Column(Integer)

# Create the database tables
Voter.metadata.create_all(engine)

# Create a session to interact with the database
Session = sessionmaker(bind=engine)
session = Session()

# Insert a new voter into the 'Voter' table
new_voter = Voter(name='John Doe', age=25)
session.add(new_voter)
session.commit()

# Query all voters from the 'Voter' table
voters = session.query(Voter).all()
for voter in voters:
    print(voter.name, voter.age)

# Update a voter's age
voter_to_update = session.query(Voter).filter_by(name='John Doe').first()
voter_to_update.age = 26
session.commit()

# Delete a voter from the 'Voter' table
voter_to_delete = session.query(Voter).filter_by(name='John Doe').first()
session.delete(voter_to_delete)
session.commit()

# Close the session
session.close()