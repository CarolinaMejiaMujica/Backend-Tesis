from bokeh.core.property.primitive import Null
from fastapi import APIRouter
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

agrupamiento = APIRouter()

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
        df_secu['x']=X_pca[0:len(df_secu),0]
        df_secu['y']=X_pca[0:len(df_secu),1]
        return df_secu

#KMEANS
@agrupamiento.post("/graficokmeans/")
def graficokmeans(fechaIni: str,fechaFin: str,deps: List[str],parametro: int):
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
        df_agrupamiento=pd.DataFrame(df_secu[['codigo','fecha', 'departamento', 'variante','color','x','y']])
        # Grafico K-means
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
        plot.scatter(x = 'x', y = 'y', color='color',legend_group='variante',source=df_agrupamiento)
        plot.legend.location = "top_right"
        plot.legend.title = 'Variantes'
        plot.legend.title_text_font_style = "bold"
        plot.legend.title_text_font_size = "15px"
        plot.legend.label_text_font_size = '11pt'
        #Valor de K
        valorK = Slider(title="Valor de k", value=parametro, start=1, end=10, max_width=700)
        def actualizar_grafico(attrname, old, new):
            #Recuperar los datos
            df_secu=data_secuencias(fechaIni,fechaFin,result,nombre_algoritmo,valorK.value)
            df_agrupamiento=pd.DataFrame(df_secu[['codigo','fecha', 'departamento', 'variante','color','x','y']])
            plot.scatter(x = 'x', y = 'y', color='color', source=df_agrupamiento)
        
        for w in [valorK]:
            w.on_change('value', actualizar_grafico)
        grafico_kmeans = pn.pane.Bokeh(column(valorK, plot))
        return json.dumps(json_item(plot, "graficokmeans"))


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
        df_agrupamiento=pd.DataFrame(df_secu[['codigo','fecha', 'departamento', 'variante','color','x','y']])
        # Grafico jerárquico
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
        plot.scatter(x = 'x', y = 'y', color='color',legend_group='variante',source=df_agrupamiento)
        plot.legend.location = "top_right"
        plot.legend.title = 'Variantes'
        plot.legend.title_text_font_style = "bold"
        plot.legend.title_text_font_size = "15px"
        plot.legend.label_text_font_size = '11pt'
        #Cantidad de clusters
        c_cluster = Slider(title="Cantidad de clusters", value=parametro, start=1, end=10, max_width=700)
        def actualizar_grafico(attrname, old, new):
            #Recuperar los datos
            df_secu=data_secuencias(fechaIni,fechaFin,result,nombre_algoritmo,c_cluster.value)
            df_agrupamiento=pd.DataFrame(df_secu[['codigo','fecha', 'departamento', 'variante','color','x','y']])
            plot.scatter(x = 'x', y = 'y', color='color', source=df_agrupamiento)

        for w in [c_cluster]:
            w.on_change('value', actualizar_grafico)
        grafico_jerarquico = pn.pane.Bokeh(column(c_cluster, plot))

        return json.dumps(json_item(plot, "graficojerarquico"))

#DENDROGRAMA
def obtenermatrizdistancia(fechaIni,fechaFin,deps):
    archiv=conn.execute(f"select estandarizada from archivos where estandarizada is not null;").fetchall()
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
        plt.figure(figsize=(10, 5))
        dend = shc.dendrogram(shc.linkage(matriz_distancias, method='ward'))
        plt.xlabel('Índices')
        plt.ylabel('Distancia (Ward)')

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