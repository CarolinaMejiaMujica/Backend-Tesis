from sqlalchemy import Table,Column
from sqlalchemy.sql.sqltypes import Integer, String, ARRAY, Text
from config.db import meta

variantes= Table(
    'variantes',meta,
    Column('id_variante',Integer,primary_key=True),
    Column('nomenclatura',String(20)),
    Column('linaje_pango',ARRAY(Text)),
    Column('sustituciones_spike',ARRAY(Text)),
    Column('nombre',String(20)),
    Column('color',String(10))
)