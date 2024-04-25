from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.engine.url import URL
from sqlalchemy import MetaData

class Database:
    def __init__(self, db_uri: str|URL):
        self.create_engine(db_uri)

    def create_engine(self, db_uri: str|URL):
        self.engine = create_engine(db_uri, pool_size=3, max_overflow=0)

    def create_table(self, table: MetaData):
        table.create_all(self.engine)

    def delete_table(self, table: MetaData):
        table.drop_all(self.engine)

    def get_schema(self):
        return self.engine.url.database
    
    def set_schema(self, schema_name: str):
        self.engine.url.database = schema_name

    def get_session(self):
        return sessionmaker(bind=self.engine)


