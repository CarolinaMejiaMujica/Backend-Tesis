from sqlalchemy import Table,Column
from sqlalchemy.sql.sqltypes import Integer, String, LargeBinary
from config.db import meta

modelos= Table(
    'algoritmos',meta,
    Column('id_algoritmo',Integer,primary_key=True),
    Column('nombre',String(20)),
    Column('parametro',Integer),
    Column('algoritmo_entrenado',LargeBinary)
)