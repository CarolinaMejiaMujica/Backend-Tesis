from sqlalchemy import create_engine, MetaData
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.engine.url import URL

#DATABASE_URL = "mysql+mysqlconnector://admin:carolina19620@localhost:3306/BDTesis"

#driver = 'mysql+pymysql'

#url = URL(driver, 'admin', 'carolina19620', 'instancetesis.ci9voqbe9ybk.us-east-1.rds.amazonaws.com', '3306', 'BDTesis')

driver='postgresql'

url = URL(driver, 'postgres', 'carolina19620', 'instanciatesis.cjfczpppafxb.us-east-1.rds.amazonaws.com', '5432', 'BDTesis')


engine = create_engine(url)
meta = MetaData()
conn = engine.connect()