import os
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

db_conn = os.environ.get("DB_CONN", "postgresql://postgres:postgres@127.0.0.1:15432/oct")
Base = declarative_base()
engine = create_engine(db_conn)
Session = sessionmaker(bind=engine)
session = Session()