from bokeh.core.property.primitive import Null
from fastapi import APIRouter,Response
import numpy as np
from config.db import conn
import json
import pandas as pd
from typing import List
from typing import List
import pickle
from bokeh.layouts import column
from bokeh.models import HoverTool,Legend, Slider
from bokeh.plotting import figure
from bokeh.embed import json_item
import panel as pn
import scipy.cluster.hierarchy as shc
import matplotlib.pyplot as plt
import base64
from io import BytesIO
from collections import defaultdict
from scipy.cluster.hierarchy import dendrogram, linkage
from bokeh.transform import factor_cmap, factor_mark
from bokeh.models import Legend, LegendItem
from models.variantes import variantes
import mpld3
import os
import boto3
from botocore.exceptions import ClientError

agrupamiento = APIRouter()

todos =['Amazonas','Áncash','Apurímac','Arequipa','Ayacucho','Cajamarca','Callao','Cusco',
    'Huancavelica','Huánuco','Ica','Junín','La Libertad','Lambayeque','Lima','Loreto','Madre de Dios',
    'Moquegua','Pasco','Piura','Puno','San Martín','Tacna','Tumbes','Ucayali']

access_key='ASIAQIMIDAYLIAIJRBIL'
access_secret='vjgDk5H3jn/F1XXexLV7QmYrj5J6ixn5PF7WylPx'
session_token='FwoGZXIvYXdzEJj//////////wEaDJdI37aw9RJ4l7oC4CLJAVZZs7wb9n+y4VVRZa+4Cvj9wE6lsYvLotoBYOrgxzogHeW0AkWdBjEkGV3NqKMTvmtS8TO4wJYgY2KfXd31yO2tqzYuheKVSNM5AawoD9MeEG+gAFMNRuTTzQyFJ/HcqnT5XnHgNL0EYHjB1wT4vIYcZv4fDX3NNupxA0XfR1cr2XknID+B+QZ2DYdPYz64DHdm4o4OryNkszt6B3E/Hm+mE1WUXDyDmPts1ckkRd097mJqLlVQnNg69020v3OayQLPGpq7mBjuqiiSxL2MBjItG7OAX5e+3U2sn8SQk1CBffj6zkjjnuh37KJrI/YMGWPUY8Q45JOZYlqlCEov'
bucket_name='dendrograma'
client_s3=boto3.client(
    's3',
    aws_access_key_id=access_key,
    aws_secret_access_key=access_secret,
    aws_session_token=session_token
)

def data_secuencias(ini,fin,deps,algoritmo,parametro):
    if len(deps) == 1:
        valor=deps[0]
        df_secu=pd.DataFrame(conn.execute(f"select s.codigo, s.fecha_recoleccion, d.nombre, v.nomenclatura, v.color,a.num_cluster,s.linaje_pango,s.variante,s.estado"+ 
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
        df_secu=pd.DataFrame(conn.execute(f"select s.codigo, s.fecha_recoleccion, d.nombre, v.nomenclatura, v.color,a.num_cluster,s.linaje_pango,s.variante,s.estado"+ 
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
        df_secu.columns=['codigo','fecha', 'departamento', 'variante_predominante','color','cluster','linaje','variante','estado']
        #Recuperar archivo pca de BD
        archiv=conn.execute(f"select archivo from archivos where nombre=\'puntos antiguos\';").fetchall()
        X_pca = pickle.loads(archiv[0][0])
        df_secu['x']=X_pca[0:len(df_secu),0]
        df_secu['y']=X_pca[0:len(df_secu),1]
        df_secu['leyenda']=''
        for i in range(len(df_secu)):    
            df_secu['leyenda'][i]='Grupo '+str(df_secu['cluster'][i])+' - '+df_secu['variante_predominante'][i]
        df_agrupamiento=df_secu.sort_values('cluster')
        return df_agrupamiento

def data_secuencias_dbscan(ini,fin,deps,algoritmo,parametro):
    if len(deps) == 1:
        valor=deps[0]
        df_secu=pd.DataFrame(conn.execute(f"select s.codigo, s.fecha_recoleccion, d.nombre, v.nomenclatura, v.color,a.num_cluster,s.linaje_pango,s.variante,s.estado"+ 
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
        df_secu=pd.DataFrame(conn.execute(f"select s.codigo, s.fecha_recoleccion, d.nombre, v.nomenclatura, v.color,a.num_cluster,s.linaje_pango,s.variante,s.estado"+ 
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
        df_secu.columns=['codigo','fecha', 'departamento', 'variante_predominante','color','cluster','linaje','variante','estado']
        #Recuperar archivo pca de BD
        archiv=conn.execute(f"select archivo from archivos where nombre=\'puntos antiguos\';").fetchall()
        X_pca = pickle.loads(archiv[0][0])
        df_secu['x']=X_pca[0:len(df_secu),0]
        df_secu['y']=X_pca[0:len(df_secu),1]
        df_secu['leyenda']=''
        for i in range(len(df_secu)):    
            df_secu['leyenda'][i]='Grupo - '+df_secu['variante_predominante'][i]
        df_agrupamiento=df_secu.sort_values('cluster')
        return df_agrupamiento

def merge_dict(d1, d2):
    dd = defaultdict(list)

    for d in (d1, d2):
        for key, value in d.items():
            if isinstance(value, list):
                dd[key].extend(value)
            else:
                dd[key].append(value)
    return dict(dd)

#KMEANS
@agrupamiento.post("/graficokmeans/")
def graficokmeans(fechaIni: str,fechaFin: str,parametro: int,deps: List[str]):
    nombre_algoritmo="'k-means'"
    if len(deps)==25:
        deps=todos
    elif 'Todos' in deps:
        deps=todos
    result = tuple(deps)
    
    #Recuperar los datos
    df_secu=data_secuencias(fechaIni,fechaFin,result,nombre_algoritmo,parametro)
    if str(df_secu) == 'No hay datos':
        return 'No hay datos'
    else:
        df_secu=df_secu.loc[df_secu['estado']==1]
        # Grafico K-means
        MARKERS = ['circle','diamond','triangle','plus','square','star','square_pin','hex','asterisk','cross']
        marcadores=MARKERS[:len(df_secu['variante'].unique())]

        hover=HoverTool(tooltips=[("ID de acceso", "@codigo"),
                ("Departamento", "@departamento"),
                ("Fecha de recolección","@fecha{%d-%m-%Y}"),
                ("Variante de la secuencia","@variante"),
                ("Variante predominante del grupo","@variante_predominante"),
                ("Color del grupo", "$leyenda $swatch:color")],formatters={'@fecha': 'datetime'})
        plot = figure(tools="pan,zoom_in,zoom_out,undo,redo,reset,save,box_zoom", plot_width=900, plot_height=600)
        plot.add_tools(hover)
        plot.xaxis.axis_label = '1er componente PCA'
        plot.yaxis.axis_label = '2do componente PCA'
        r=plot.scatter(x = 'x', y = 'y',size=10,line_color = 'grey',source=df_secu,marker=factor_mark('variante', marcadores, df_secu['variante'].unique()),color='color')

        plot.x_range.renderers = [r]
        plot.y_range.renderers = [r]

        #Grupos
        rc = plot.rect(x=0, y=0, height=1, width=1, color=tuple(df_secu['color'].unique()))
        rc.visible = False
        #Grupos
        legend1 = Legend(items=[
            LegendItem(label=df_secu['leyenda'].unique()[i], renderers=[rc], index=i) for i, c in enumerate(df_secu['color'].unique())
        ], location='center',orientation="horizontal",title='Grupo - Variante predominante')
        plot.add_layout(legend1, 'above')

        #Variantes
        rs = plot.scatter(x=0, y=0, color="grey", marker=marcadores)
        rs.visible = False
        #Variantes
        legend = Legend(items=[
            LegendItem(label=df_secu['variante'].unique()[i], renderers=[rs], index=i) for i, s in enumerate(marcadores)
        ], location="top_right",title = 'Variantes')
        plot.add_layout(legend, 'right')

        plot.legend.label_text_font_style="normal"
        plot.legend.title_text_font_style = "bold"
        plot.legend.title_text_font_size = "13px"
        plot.legend.label_text_font_size = "10pt"

        tabla= tablaagrupamiento(fechaIni,fechaFin,result,nombre_algoritmo,parametro)
        return json.dumps(json_item(plot, "graficokmeans")),tabla

@agrupamiento.post("/graficojerarquico/")
def graficojerarquico(fechaIni: str,fechaFin: str,deps: List[str],parametro: int):
    nombre_algoritmo="'jerarquico'"
    if len(deps)==25:
        deps=todos
    elif 'Todos' in deps:
        deps=todos
    result = tuple(deps)

    #Recuperar los datos  
    df_secu=data_secuencias(fechaIni,fechaFin,result,nombre_algoritmo,parametro)
    if str(df_secu) == 'No hay datos':
        return 'No hay datos'
    else:
        df_secu=df_secu.loc[df_secu['estado']==1]
        # Grafico jerárquico
        MARKERS = ['circle','diamond','triangle','plus','square','star','square_pin','hex','asterisk','cross']
        marcadores=MARKERS[:len(df_secu['variante'].unique())]

        hover=HoverTool(tooltips=[("ID de acceso", "@codigo"),
                ("Departamento", "@departamento"),
                ("Fecha de recolección","@fecha{%d-%m-%Y}"),
                ("Variante de la secuencia","@variante"),
                ("Variante predominante del grupo","@variante_predominante"),
                ("Color del grupo", "$leyenda $swatch:color")],formatters={'@fecha': 'datetime'})
        plot = figure(tools="pan,zoom_in,zoom_out,undo,redo,reset,save,box_zoom", plot_width=800, plot_height=500)
        plot.add_tools(hover)
        plot.xaxis.axis_label = '1er componente PCA'
        plot.yaxis.axis_label = '2do componente PCA'
        r=plot.scatter(x = 'x', y = 'y',size=10,line_color = 'grey',source=df_secu,marker=factor_mark('variante', marcadores, df_secu['variante'].unique()),color='color')

        plot.x_range.renderers = [r]
        plot.y_range.renderers = [r]

        #Grupos
        rc = plot.rect(x=0, y=0, height=1, width=1, color=tuple(df_secu['color'].unique()))
        rc.visible = False
        #Grupos
        legend1 = Legend(items=[
            LegendItem(label=df_secu['leyenda'].unique()[i], renderers=[rc], index=i) for i, c in enumerate(df_secu['color'].unique())
        ], location='center',orientation="horizontal",title='Grupo - Variante predominante')
        plot.add_layout(legend1, 'above')

        #Variantes
        rs = plot.scatter(x=0, y=0, color="grey", marker=marcadores)
        rs.visible = False
        #Variantes
        legend = Legend(items=[
            LegendItem(label=df_secu['variante'].unique()[i], renderers=[rs], index=i) for i, s in enumerate(marcadores)
        ], location="top_right",title = 'Variantes')
        plot.add_layout(legend, 'right')

        plot.legend.label_text_font_style="normal"
        plot.legend.title_text_font_style = "bold"
        plot.legend.title_text_font_size = "13px"
        plot.legend.label_text_font_size = "10pt"

        tabla= tablaagrupamiento(fechaIni,fechaFin,result,nombre_algoritmo,parametro)
        return json.dumps(json_item(plot, "graficojerarquico")),tabla

#DENDROGRAMA
def obtenermatrizdistancia(fechaIni,fechaFin,deps):
    archiv=conn.execute(f"select matriz_distancia from archivos where id_archivo=3;").fetchall()
    if archiv == Null:
        return 'No hay datos'
    else:
        matriz_distancias = pickle.loads(archiv[0][0])
        return matriz_distancias

@agrupamiento.post("/dendrograma/")
def dendrograma(fechaIni: str,fechaFin: str,deps: List[str]):
    if len(deps)==25:
        deps=todos
    elif 'Todos' in deps:
        deps=todos
    result = tuple(deps)
    matriz_distancias=obtenermatrizdistancia(fechaIni,fechaFin,deps)
    if str(matriz_distancias) == 'No hay datos':
        return 'No hay datos'
    else:
        df1=pd.DataFrame(matriz_distancias)
        Z = linkage(df1, 'ward')
        fig1=plt.figure(figsize=(10, 10))
        plt.xlabel('Índices')
        plt.ylabel('Distancia (Ward)')
        ax=plt.gca()
        ax.get_xaxis().set_visible(False)
        dendrogram(Z, labels=df1.index, leaf_rotation=90)
        plt.savefig('dendrograma.png')
        data_file_folder=os.path.join(os.getcwd())
        for file in os.listdir(data_file_folder):
            if file.startswith('d'):
                try:
                    client_s3.upload_file(os.path.join(data_file_folder,file),bucket_name,file)
                except ClientError as e:
                    print(e)

#DBSCAN
@agrupamiento.post("/graficodbscan/")
def graficodbscan(fechaIni: str,fechaFin: str,deps: List[str],parametro: float):
    nombre_algoritmo="'dbscan'"
    if len(deps)==25:
        deps=todos
    elif 'Todos' in deps:
        deps=todos
    result = tuple(deps)
    #Recuperar los datos
    df_secu=data_secuencias_dbscan(fechaIni,fechaFin,result,nombre_algoritmo,parametro)
    if str(df_secu) == 'No hay datos':
        return 'No hay datos'
    else:
        df_secu=df_secu.loc[df_secu['estado']==1]
        # Grafico DBSCAN
        MARKERS = ['circle','diamond','triangle','plus','square','star','square_pin','hex','asterisk','cross']
        marcadores=MARKERS[:len(df_secu['variante'].unique())]

        hover=HoverTool(tooltips=[("ID de acceso", "@codigo"),
                ("Departamento", "@departamento"),
                ("Fecha de recolección","@fecha{%d-%m-%Y}"),
                ("Variante de la secuencia","@variante"),
                ("Variante predominante del grupo","@variante_predominante"),
                ("Color del grupo", "$leyenda $swatch:color")],formatters={'@fecha': 'datetime'})
        plot = figure(tools="pan,zoom_in,zoom_out,undo,redo,reset,save,box_zoom", plot_width=900, plot_height=600)
        plot.add_tools(hover)
        plot.xaxis.axis_label = '1er componente PCA'
        plot.yaxis.axis_label = '2do componente PCA'

        #guardar un nuevo dataframe con las secuencias que son ruidos
        df_ruido=pd.DataFrame(df_secu.loc[df_secu['cluster']==0])
        df_ruido['variante_predominante']="Ruido"
        df_ruido['color']="#A2A2A2"
        plot.scatter(x='x', y='y',size=5,source=df_ruido,color='color')

        df=pd.DataFrame(df_secu.loc[df_secu['cluster']!=0])
        r=plot.scatter(x = 'x', y = 'y',size=10,line_color = 'grey',source=df,marker=factor_mark('variante', marcadores, df['variante'].unique()),color='color')

        plot.x_range.renderers = [r]
        plot.y_range.renderers = [r]

        #Grupos
        rc = plot.rect(x=0, y=0, height=1, width=1, color=tuple(df['color'].unique()))
        rc.visible = False
        #Grupos
        legend1 = Legend(items=[
            LegendItem(label=df['leyenda'].unique()[i], renderers=[rc], index=i) for i, c in enumerate(df['color'].unique())
        ], location='center',orientation="horizontal",title='Grupo - Variante predominante')
        plot.add_layout(legend1, 'above')

        #Variantes
        rs = plot.scatter(x=0, y=0, color="grey", marker=marcadores)
        rs.visible = False
        #Variantes
        legend = Legend(items=[
            LegendItem(label=df['variante'].unique()[i], renderers=[rs], index=i) for i, s in enumerate(marcadores)
        ], location="top_right",title = 'Variantes')
        plot.add_layout(legend, 'right')

        plot.legend.label_text_font_style="normal"
        plot.legend.title_text_font_style = "bold"
        plot.legend.title_text_font_size = "13px"
        plot.legend.label_text_font_size = "10pt"

        tabla= tablaagrupamiento(fechaIni,fechaFin,result,nombre_algoritmo,parametro)
        return json.dumps(json_item(plot, "graficodbscan")),tabla

#LISTA DE DATOS
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
            "where s.estado=1 and m.nombre like "+algoritmo +" and m.parametro="+str(parametro)+
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
            "where s.estado=1 and m.nombre like "+algoritmo +" and m.parametro="+str(parametro)+
            " and s.fecha_recoleccion >= \'"+ fechaIni +"\' and s.fecha_recoleccion<= \'"+ fechaFin +"\' "+
            "and d.nombre in "+ str(result)+
            " ORDER BY d.nombre ASC").fetchall()
    else:
        return 'No hay datos'