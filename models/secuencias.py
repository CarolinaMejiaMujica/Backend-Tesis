from sqlalchemy import Table,Column
from sqlalchemy.sql.sqltypes import Integer, String, Date, Text
from config.db import meta

secuencias = Table(
    'secuencias',meta,
    Column('id_secuencia',Integer,primary_key=True),
    Column('codigo',String(25)),
    Column('secuencia',Text),
    Column('fecha_recoleccion',Date),
    Column('secuencia_alineada',Text),
    Column('id_departamento',Integer),
    Column('linaje_pango',Text),
    Column('variante',Text),
    Column('estado',Integer)
)