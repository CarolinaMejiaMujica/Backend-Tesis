from sqlalchemy import Table,Column
from sqlalchemy.sql.sqltypes import Integer, String, LargeBinary
from config.db import meta

archivos=Table(
    'archivos',meta,
    Column('id_archivo',Integer,primary_key=True),
    Column('nombre',String(30)),
    Column('archivo',LargeBinary),
)