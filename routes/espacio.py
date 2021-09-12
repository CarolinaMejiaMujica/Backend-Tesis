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


espacio = APIRouter()

@espacio.post("/mapa/")
def grafico(fechaIni: str,fechaFin: str):
    df_departamentos=pd.DataFrame(conn.execute(departamentos.select()).fetchall())
    df_departamentos.columns=['ID', 'Nombre', 'latitud', 'longitud']

    df_dep=pd.DataFrame(conn.execute(f"select count(s.id_secuencia) AS count, d.nombre from departamentos as d "+
                                 "LEFT JOIN secuencias as s ON d.id_departamento=s.id_departamento "+
                                 "LEFT JOIN agrupamiento as a ON s.id_secuencia=a.id_secuencia "+
                                 "LEFT JOIN variantes as v ON a.id_variante=v.id_variante "+
                                 "LEFT JOIN algoritmos as m ON a.id_algoritmo=m.id_algoritmo "+
                                 "where m.nombre like 'k-means' and m.parametro=10 and "+
                                 "s.fecha_recoleccion >= \'"+ fechaIni +"\' and s.fecha_recoleccion<=\'"+ fechaFin +"\' "+
                                 "group by d.nombre order by d.nombre").fetchall())
    df_dep.columns=['count','Nombre']
    df_departamentos=df_departamentos.merge(df_dep, how='left', on='Nombre')
    df_departamentos["variantes"] = "a"
    df_departamentos["variante"] = "a"
    df_departamentos['color']="a"
    df_departamentos.loc[df_departamentos['count'].isnull(),'count']=0

    df_vari=pd.DataFrame(conn.execute(f"SELECT d.nombre, COALESCE(v.id_variante,0) as id_variante, count(a.*), CONCAT(COALESCE(v.nomenclatura,''), ' - ', v.nombre) as nombre_variante, "+
                                  "v.color from departamentos as d LEFT JOIN secuencias as s ON d.id_departamento=s.id_departamento "+
                                  "LEFT JOIN agrupamiento as a ON s.id_secuencia=a.id_secuencia LEFT JOIN variantes as v ON a.id_variante=v.id_variante "+
                                  "LEFT JOIN algoritmos as m ON a.id_algoritmo=m.id_algoritmo "+
                                  "where m.nombre like 'k-means' and m.parametro=10 and "+
                                 "s.fecha_recoleccion >= \'"+ fechaIni +"\' and s.fecha_recoleccion<=\'"+ fechaFin +"\' "+
                                  "GROUP BY d.nombre,v.id_variante ORDER BY d.nombre ASC").fetchall())
    df_vari.columns=['Nombre','id_variante','count_variante','nombre_variante','color']
    lista=set(df_vari['Nombre'])

    for i,d in enumerate(df_departamentos['Nombre']):
        if d in lista:
            var_dep=list((df_vari.loc[df_vari['Nombre']==d]['nombre_variante']).unique())
            count_variante=df_vari.loc[df_vari['Nombre']==d]['count_variante'].max()
            variante_pred=df_vari.loc[(df_vari['count_variante']==count_variante) & (df_vari['Nombre']==d)]['nombre_variante'].iloc[0]
            if len(variante_pred)==0:
                df_departamentos['variantes'].iloc[i]='No hay datos'
                df_departamentos['variante'].iloc[i]='No hay datos'
                df_departamentos['color'].iloc[i]='#CDCDCD'
            else:
                df_departamentos['variantes'].iloc[i]=var_dep
                df_departamentos['variante'].iloc[i]=variante_pred
                df_departamentos['color'].iloc[i]=list(df_vari.loc[df_vari['nombre_variante']==variante_pred]['color'])[0]
        else:
            df_departamentos['variantes'].iloc[i]='No hay datos'
            df_departamentos['variante'].iloc[i]='No hay datos'
            df_departamentos['color'].iloc[i]='#CDCDCD'



    cjs = """
    console.log('Tap');
    console.log(source.selected.indices);
    var valor=source.selected.indices
    """
    
    TOOLTIPS=[("Departamento", "@Nombre"),
          ("Total de secuencias genómicas", "@count"),
          ("Variantes identificadas","@variantes"),
          ("Variante predominante","@variante"),
          ("Color", "$variante $swatch:color")]

    fig = figure(tools="pan,zoom_in,zoom_out,undo,redo,reset,save",plot_width=700, plot_height=600,
                x_axis_location=None, y_axis_location=None,
                tooltips=TOOLTIPS)

    fig.grid.grid_line_color = None
    valor=fig.patches("longitud", "latitud", source=df_departamentos,
                fill_color={'field': 'variante', 'transform': CategoricalColorMapper(palette=df_departamentos['color'], factors=df_departamentos['variante'].to_list())},
                fill_alpha=0.7, line_color="white", line_width=0.5,legend_group='variante')

    fig.legend.location = "bottom_left"
    fig.legend.title = 'Variantes'
    fig.legend.title_text_font_style = "bold"
    fig.legend.title_text_font_size = "15px"
    fig.legend.label_text_font_size = '11pt'
    fig.legend.label_standoff = 2
    fig.legend.glyph_width =20
    fig.legend.glyph_height=20
    fig.legend.spacing = 0
    fig.legend.padding = 1
    fig.legend.margin = 5

    cb = CustomJS(args=dict(source=valor.data_source), code=cjs)
    ttool = TapTool(callback=cb)
    fig.tools.append(ttool)

    return json.dumps(json_item(fig, "mapa"))

@espacio.post("/tablaespacio/")
def grafico(fechaIni: str,fechaFin: str):
    return conn.execute(f"SELECT d.nombre as nombre, s.codigo, s.fecha_recoleccion as fecha, v.nomenclatura as nomenclatura,v.nombre as variante "+
                        "from departamentos as d "+
                        "LEFT JOIN secuencias as s ON d.id_departamento=s.id_departamento "+
                        "LEFT JOIN agrupamiento as a ON s.id_secuencia=a.id_secuencia "+
                        "LEFT JOIN variantes as v ON a.id_variante=v.id_variante "+
                        "LEFT JOIN algoritmos as m ON a.id_algoritmo=m.id_algoritmo "+
                        "where m.nombre like 'k-means' and m.parametro=10 and "+
                        "s.fecha_recoleccion >= \'"+ fechaIni +"\' and s.fecha_recoleccion<= \'"+ fechaFin +"\' "+
                        "ORDER BY d.nombre ASC").fetchall()