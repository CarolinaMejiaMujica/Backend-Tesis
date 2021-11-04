from sqlalchemy import Table,Column
from sqlalchemy.sql.sqltypes import Integer, LargeBinary
from config.db import meta

archivos=Table(
    'archivos',meta,
    Column('id_archivo',Integer,primary_key=True),
    Column('matriz_secuencias',LargeBinary),
    Column('matriz_distancia',LargeBinary),
    Column('mds',LargeBinary),
    Column('pca',LargeBinary),
    Column('landmark',LargeBinary),
    Column('modelo',LargeBinary),
    Column('puntos_antiguos',LargeBinary),
    Column('puntos_nuevos',LargeBinary)
)