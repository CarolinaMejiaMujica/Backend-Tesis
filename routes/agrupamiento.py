from fastapi import APIRouter, Response
from config.db import conn
from models.departamentos import departamentos
import json
from bokeh.embed import json_item
import pandas as pd
from bokeh.plotting import figure
from bokeh.models import CategoricalColorMapper
from bokeh.models.tools import TapTool
from bokeh.models.callbacks import CustomJS
from typing import List
from bokeh.models import DatetimeTickFormatter
from bokeh.models import HoverTool
from math import pi
from bokeh.transform import cumsum
from typing import List
import numpy as np
import pickle
from sklearn.cluster import KMeans, AgglomerativeClustering, DBSCAN

agrupamiento = APIRouter()
#para devolver dos valores https://fastapi.tiangolo.com/advanced/additional-responses/

todos =['Amazonas','Áncash','Apurímac','Arequipa','Ayacucho','Cajamarca','Callao','Cusco',
    'Huancavelica','Huánuco','Ica','Junín','La Libertad','Lambayeque','Lima','Loreto','Madre de Dios',
    'Moquegua','Pasco','Piura','Puno','San Martín','Tacna','Tumbes','Ucayali']

def data_secuencias(ini,fin,deps,algoritmo,parametro):
    if len(deps) == 1:
        valor=deps[0]
        df_secu=pd.DataFrame(conn.execute(f"select s.codigo, s.fecha_recoleccion, d.nombre, v.nomenclatura, v.color,a.num_cluster"+ 
                " from agrupamiento as a"+
                " LEFT JOIN secuencias as s ON a.id_secuencia=s.id_secuencia"+
                " LEFT JOIN departamentos as d ON s.id_departamento=d.id_departamento"+ 
                " LEFT JOIN variantes as v ON a.id_variante=v.id_variante"+
                " LEFT JOIN algoritmos as m ON a.id_algoritmo=m.id_algoritmo where m.nombre like " + algoritmo +
                " and m.parametro= "+ str(parametro)+
                " and s.fecha_recoleccion >= \'"+ ini +"\' and s.fecha_recoleccion<= \'"+ fin +
                "\' and d.nombre in (\'"+ str(valor)+
                "\') order by s.id_secuencia").fetchall())
    elif len(deps)>1:
        df_secu=pd.DataFrame(conn.execute(f"select s.codigo, s.fecha_recoleccion, d.nombre, v.nomenclatura, v.color,a.num_cluster"+ 
                " from agrupamiento as a"+
                " LEFT JOIN secuencias as s ON a.id_secuencia=s.id_secuencia"+
                " LEFT JOIN departamentos as d ON s.id_departamento=d.id_departamento"+ 
                " LEFT JOIN variantes as v ON a.id_variante=v.id_variante"+
                " LEFT JOIN algoritmos as m ON a.id_algoritmo=m.id_algoritmo where m.nombre like " + algoritmo +
                " and m.parametro= "+ str(parametro)+
                " and s.fecha_recoleccion >= \'"+ ini +"\' and s.fecha_recoleccion<= \'"+ fin +
                "\' and d.nombre in "+ str(deps)+
                " order by s.id_secuencia").fetchall())
    else:
        return 'No hay datos'
    if df_secu.empty:
        return 'No hay datos'
    else:
        df_secu.columns=['codigo','fecha', 'departamento', 'variante','color','cluster']
        #Recuperar archivo pca de BD
        archiv=conn.execute(f"select pca from archivos where pca is not null;").fetchall()
        X_pca = pickle.loads(archiv[0][0])
        df_secu['x']=X_pca[:,0]
        df_secu['y']=X_pca[:,1]
        df_agrupamiento=pd.DataFrame(df_secu[['codigo','fecha', 'departamento', 'variante','color','x','y']])
        return df_agrupamiento


@agrupamiento.post("/graficokmeans/")
def graficokmeans(fechaIni: str,fechaFin: str,deps: List[str],algoritmo: str,parametro: int):
    if len(deps)==25:
        deps=todos
    elif 'Todos' in deps:
        deps=todos
    result = tuple(deps)
    df_agrupamiento=data_secuencias(fechaIni,fechaFin,result,algoritmo,parametro)
    if str(df_agrupamiento) == 'No hay datos':
        return 'No hay datos'
    else:

        return 1


@agrupamiento.post("/graficojerarquico/")
def graficojerarquico(fechaIni: str,fechaFin: str,deps: List[str],algoritmo: str,parametro: int):
    if len(deps)==25:
        deps=todos
    elif 'Todos' in deps:
        deps=todos
    result = tuple(deps)
    df_agrupamiento=data_secuencias(fechaIni,fechaFin,result,algoritmo,parametro)
    if str(df_agrupamiento) == 'No hay datos':
        return 'No hay datos'
    else:

        return 1


@agrupamiento.post("/graficodbscan/")
def graficodbscan(fechaIni: str,fechaFin: str,deps: List[str],algoritmo: str,parametro: int):
    if len(deps)==25:
        deps=todos
    elif 'Todos' in deps:
        deps=todos
    result = tuple(deps)
    df_agrupamiento=data_secuencias(fechaIni,fechaFin,result,algoritmo,parametro)
    if str(df_agrupamiento) == 'No hay datos':
        return 'No hay datos'
    else:

        return 1


@agrupamiento.post("/tablaagrupamiento/")
def tablaagrupamiento(fechaIni: str,fechaFin: str,deps: List[str],algoritmo: str,parametro: int):
    if len(deps)==25:
        deps=todos
    elif 'Todos' in deps:
        deps=todos
    result = tuple(deps)
    if len(result) == 1:
        valor=result[0]
        return conn.execute(f"SELECT d.nombre as nombre, s.codigo, s.fecha_recoleccion as fecha,a.num_cluster as cluster, v.nomenclatura as nomenclatura "+
            "from departamentos as d "+
            "LEFT JOIN secuencias as s ON d.id_departamento=s.id_departamento "+
            "LEFT JOIN agrupamiento as a ON s.id_secuencia=a.id_secuencia "+
            "LEFT JOIN variantes as v ON a.id_variante=v.id_variante "+
            "LEFT JOIN algoritmos as m ON a.id_algoritmo=m.id_algoritmo "+
            "where m.nombre like \'"+algoritmo +"\' and m.parametro="+str(parametro)+
            " and s.fecha_recoleccion >= \'"+ fechaIni +"\' and s.fecha_recoleccion<= \'"+ fechaFin +"\' "+
            "and d.nombre in (\'"+ str(valor)+
            "\') ORDER BY d.nombre ASC").fetchall()

    elif len(result) > 1:
        return conn.execute(f"SELECT d.nombre as nombre, s.codigo, s.fecha_recoleccion as fecha,a.num_cluster as cluster, v.nomenclatura as nomenclatura "+
            "from departamentos as d "+
            "LEFT JOIN secuencias as s ON d.id_departamento=s.id_departamento "+
            "LEFT JOIN agrupamiento as a ON s.id_secuencia=a.id_secuencia "+
            "LEFT JOIN variantes as v ON a.id_variante=v.id_variante "+
            "LEFT JOIN algoritmos as m ON a.id_algoritmo=m.id_algoritmo "+
            "where m.nombre like \'"+algoritmo +"\' and m.parametro="+str(parametro)+
            " and s.fecha_recoleccion >= \'"+ fechaIni +"\' and s.fecha_recoleccion<= \'"+ fechaFin +"\' "+
            "and d.nombre in "+ str(result)+
            " ORDER BY d.nombre ASC").fetchall()
    else:
        return 'No hay datos'