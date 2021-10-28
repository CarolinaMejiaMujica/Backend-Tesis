from bokeh.core.property.primitive import Null
from fastapi import APIRouter
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

agrupamiento = APIRouter()

todos =['Amazonas','Áncash','Apurímac','Arequipa','Ayacucho','Cajamarca','Callao','Cusco',
    'Huancavelica','Huánuco','Ica','Junín','La Libertad','Lambayeque','Lima','Loreto','Madre de Dios',
    'Moquegua','Pasco','Piura','Puno','San Martín','Tacna','Tumbes','Ucayali']

def data_secuencias(ini,fin,deps,algoritmo,parametro):
    if len(deps) == 1:
        valor=deps[0]
        df_secu=pd.DataFrame(conn.execute(f"select s.codigo, s.fecha_recoleccion, d.nombre, v.nomenclatura, v.color,a.num_cluster,s.linaje_pango"+ 
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
        df_secu=pd.DataFrame(conn.execute(f"select s.codigo, s.fecha_recoleccion, d.nombre, v.nomenclatura, v.color,a.num_cluster,s.linaje_pango"+ 
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
        df_secu.columns=['codigo','fecha', 'departamento', 'variante_predominante','color','cluster','linaje']
        #Recuperar archivo pca de BD
        archiv=conn.execute(f"select matriz_distancia from archivos where id_archivo=3;").fetchall()
        X_pca = pickle.loads(archiv[0][0])
        df_secu['x']=X_pca[0:len(df_secu),0]
        df_secu['y']=X_pca[0:len(df_secu),1]

        #variantes
        df_secu['variante']=''
        linajes_pangos=[]
        ids_linajes_pangos=[]
        variants=pd.DataFrame(conn.execute(variantes.select()).fetchall())
        variants.columns=['id_variante', 'nomenclatura', 'linaje_pango','sustituciones_spike','nombre','color']
        for i in range(len(df_secu)):
            pango=df_secu.iloc[i].linaje
            #verificar que variante le corresponde
            for v in range(len(variants)):
                valores=variants.iloc[v]['linaje_pango']
                for val in valores:
                    if 'sublinajes' in val:
                        val=val.replace('sublinajes ',"")
                        if val in pango:
                            df_secu['variante'][i]=variants.iloc[v]['nomenclatura']
                    else:
                        if pango in val:
                            df_secu['variante'][i]=variants.iloc[v]['nomenclatura']
            if str('') == str(df_secu['variante'][i]):
                ids_linajes_pangos.append(df_secu.iloc[i].codigo)
                linajes_pangos.append(pango)
                df_secu['variante'][i]='Otro'

        df_secu['leyenda']=''
        for i in range(len(df_secu)):    
            df_secu['leyenda'][i]='Grupo '+str(df_secu['cluster'][i])+' - '+df_secu['variante_predominante'][i]
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

    df_secu=data_secuencias(fechaIni,fechaFin,result,nombre_algoritmo,parametro)
    if str(df_secu) == 'No hay datos':
        return 'No hay datos'
    else:        
        #Recuperar los datos
        #df_secu=data_secuencias(fechaIni,fechaFin,result,nombre_algoritmo,parametro)

        # Grafico K-means
        MARKERS = ['circle','diamond','triangle','plus','square','star','square_pin','hex','asterisk','cross']
        marcadores=MARKERS[:len(df_secu['variante'].unique())]

        hover=HoverTool(tooltips=[("Identificador", "@codigo"),
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

    df_secu=data_secuencias(fechaIni,fechaFin,result,nombre_algoritmo,parametro)
    if str(df_secu) == 'No hay datos':
        return 'No hay datos'
    else:
        #Recuperar los datos      
        #df_agrupamiento=pd.DataFrame(df_secu[['codigo','fecha', 'departamento', 'variante','color','x','y']])
        
        # Grafico jerárquico
        MARKERS = ['circle','diamond','triangle','plus','square','star','square_pin','hex','asterisk','cross']
        marcadores=MARKERS[:len(df_secu['variante'].unique())]

        hover=HoverTool(tooltips=[("Identificador", "@codigo"),
                ("Departamento", "@departamento"),
                ("Fecha de recolección","@fecha{%d-%m-%Y}"),
                ("Variante de la secuencia","@variante"),
                ("Variante predominante del grupo","@variante_predominante"),
                ("Color del grupo", "$leyenda $swatch:color")],formatters={'@fecha': 'datetime'})
        plot = figure(tools="pan,zoom_in,zoom_out,undo,redo,reset,save,box_zoom", plot_width=700, plot_height=500)
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
        ], location="top_right",title='Grupo - Variante predominante')
        plot.add_layout(legend1, 'right')

        #Variantes
        rs = plot.scatter(x=0, y=0, color="grey", marker=marcadores)
        rs.visible = False
        #Variantes
        legend = Legend(items=[
            LegendItem(label=df_secu['variante'].unique()[i], renderers=[rs], index=i) for i, s in enumerate(marcadores)
        ], location='center',orientation="horizontal",title = 'Variantes')
        plot.add_layout(legend, 'above')

        plot.legend.label_text_font_style="normal"
        plot.legend.title_text_font_style = "bold"
        plot.legend.title_text_font_size = "13px"
        plot.legend.label_text_font_size = "10pt"

        tabla= tablaagrupamiento(fechaIni,fechaFin,result,nombre_algoritmo,parametro)
        return json.dumps(json_item(plot, "graficojerarquico")),tabla

#DENDROGRAMA
def obtenermatrizdistancia(fechaIni,fechaFin,deps):
    archiv=conn.execute(f"select matriz_distancia from archivos where id_archivo=1;").fetchall()
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
        plt.figure(figsize=(10, 5))
        plt.xlabel('Índices')
        plt.ylabel('Distancia (Ward)')
        dendrogram(Z, labels=df1.index, leaf_rotation=90)
        

        fig = plt.figure()
        tmpfile = BytesIO()
        fig.savefig(tmpfile, format='png')
        encoded = base64.b64encode(tmpfile.getvalue()).decode('utf-8')
        html = '<img src=\'data:image/png;base64,{}\'>'.format(encoded)
        return html

#DBSCAN
@agrupamiento.post("/graficodbscan/")
def graficodbscan(fechaIni: str,fechaFin: str,deps: List[str],parametro: int):
    nombre_algoritmo="'dbscan'"
    if len(deps)==25:
        deps=todos
    elif 'Todos' in deps:
        deps=todos
    result = tuple(deps)
    df_secu=data_secuencias(fechaIni,fechaFin,result,nombre_algoritmo,parametro)
    if str(df_secu) == 'No hay datos':
        return 'No hay datos'
    else:
        #Recuperar los datos      
        df_agrupamiento=pd.DataFrame(df_secu[['codigo','fecha', 'departamento', 'variante','color','x','y']])
        # Grafico DBSCAN
        hover=HoverTool(tooltips=[("Identificador", "@codigo"),
                ("Departamento", "@departamento"),
                ("Fecha de recolección","@fecha{%d-%m-%Y}"),
                ("Variante predominante","@variante"),
                ("Color", "$variante $swatch:color")],formatters={'@fecha': 'datetime'})

        plot = figure(tools="pan,zoom_in,zoom_out,undo,redo,reset,save,box_zoom", plot_width=800, plot_height=500)
        plot.add_tools(hover)
        plot.xaxis.axis_label = '1er componente PCA'
        plot.yaxis.axis_label = '2do componente PCA'
        plot.add_layout(Legend(), 'right')
        plot.scatter(x = 'x', y = 'y', size=1, color='#9c9c9c',source=df_agrupamiento)

        df=pd.DataFrame(df_secu.loc[df_secu['cluster']!=0][['codigo','fecha', 'departamento', 'variante','color','x','y']])
        plot.scatter(x='x', y='y', color='color',  legend_group='variante', size=5,source=df)

        plot.legend.location = "top_right"
        plot.legend.title = 'Variantes'
        plot.legend.title_text_font_style = "bold"
        plot.legend.title_text_font_size = "15px"
        plot.legend.label_text_font_size = '11pt'
        #Valor de epsilon
        epsilon = Slider(title="Valor de epsilon", value=parametro, start=1, end=10, max_width=700)
        def actualizar_grafico(attrname, old, new):
            #Recuperar los datos
            df_secu=data_secuencias(fechaIni,fechaFin,result,nombre_algoritmo,epsilon.value)
            df_agrupamiento=pd.DataFrame(df_secu[['codigo','fecha', 'departamento', 'variante','color','x','y']])
            plot.scatter(x = 'x', y = 'y', size=1, color='#9c9c9c', source=df_agrupamiento)
            
            df=pd.DataFrame(df_secu.loc[df_secu['cluster']!=0][['codigo','fecha', 'departamento', 'variante','color','x','y']])
            plot.scatter(x='x', y='y', color='color', size=5,source=df)

        for w in [epsilon]:
            w.on_change('value', actualizar_grafico)
        grafico_dbscan = pn.pane.Bokeh(column(epsilon, plot))

        return json.dumps(json_item(plot, "graficodbscan"))

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
            "where m.nombre like "+algoritmo +" and m.parametro="+str(parametro)+
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
            "where m.nombre like "+algoritmo +" and m.parametro="+str(parametro)+
            " and s.fecha_recoleccion >= \'"+ fechaIni +"\' and s.fecha_recoleccion<= \'"+ fechaFin +"\' "+
            "and d.nombre in "+ str(result)+
            " ORDER BY d.nombre ASC").fetchall()
    else:
        return 'No hay datos'