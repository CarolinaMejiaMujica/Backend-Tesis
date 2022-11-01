from sqlalchemy import create_engine, MetaData
from sqlalchemy.engine.url import URL

driver='postgresql'

url = URL(driver, 'rlnivuldeinkhw', '3a0520892729385ad451ea12c8f120ebd261508942aa67af6a5374eab95087ef', 'ec2-34-227-135-211.compute-1.amazonaws.com', '5432', 'd9h8eju0ocjc4h')
engine = create_engine(url,
            pool_pre_ping=True,
            connect_args={
                "keepalives": 8,
                "keepalives_idle": 50,
                "keepalives_interval": 50,
                "keepalives_count": 50,
            })
meta = MetaData()
conn = engine.connect()