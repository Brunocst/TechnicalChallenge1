import csv 
from sqlalchemy import Column, Integer, String
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

engine = create_engine('mysql+pymysql://root:A15j27lh!35h@127.0.0.1:3306/employee')
Base = declarative_base()

class Candidates(Base):
    __tablename__ = 'Candidates'
    id = Column(Integer, primary_key=True, nullable=False)
    first_name = Column(String(255))
    last_name = Column(String(255))
    email = Column(String(255))
    application_date = Column(String(255))
    country = Column(String(255))
    yoe = Column(String(255))
    seniority = Column(String(255))
    technology = Column(String(255))
    code_challenge_score = Column(String(255))
    technical_interview_score = Column(String(255))
    
FILE = '/Users/bruno/Documents/data/Fullstack_challenge/candidates.csv'
with open(FILE, newline='') as f:
    reader = csv.reader(f, delimiter=';')
    data = list(reader)

Base.metadata.create_all(engine)

session = sessionmaker()
session.configure(bind=engine)
s = session()

try:
    candidates = []
    for i in data[1::]:
        record = {
            'first_name':i[0],
            'last_name':i[1],
            'email':i[2],
            'application_date':i[3],
            'country':i[4],
            'yoe':i[5],
            'seniority':i[6],
            'technology':i[7],
            'code_challenge_score':i[8],
            'technical_interview_score':i[9]
        }
        candidates.append(record)

    s.bulk_insert_mappings(Candidates, candidates)
    s.commit()
except:
    s.rollback()
finally:
    s.close()

    