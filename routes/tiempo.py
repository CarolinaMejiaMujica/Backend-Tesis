from fastapi import APIRouter
from config.db import conn
import json
from bokeh.embed import json_item
import pandas as pd
from bokeh.plotting import figure
from bokeh.models import DatetimeTickFormatter
from bokeh.models import HoverTool
from math import pi
from bokeh.transform import cumsum
from typing import List
import numpy as np
pd.options.mode.chained_assignment = None  # default='warn'

tiempo = APIRouter()

todos =['Amazonas','Áncash','Apurímac','Arequipa','Ayacucho','Cajamarca','Callao','Cusco',
    'Huancavelica','Huánuco','Ica','Junín','La Libertad','Lambayeque','Lima','Loreto','Madre de Dios',
    'Moquegua','Pasco','Piura','Puno','San Martín','Tacna','Tumbes','Ucayali']

def data_secuencias(ini,fin,deps):
    if len(deps) == 1:
        valor=deps[0]
        df_secu=pd.DataFrame(conn.execute(f"select count(s.id_secuencia),s.fecha_recoleccion, v.nomenclatura as variante, v.color from secuencias as s "+
            "LEFT JOIN departamentos as d ON s.id_departamento=d.id_departamento "+
            "LEFT JOIN agrupamiento as a ON s.id_secuencia=a.id_secuencia "+
            "LEFT JOIN variantes as v ON a.id_variante=v.id_variante "+
            "LEFT JOIN algoritmos as m ON a.id_algoritmo=m.id_algoritmo "+
            "where m.nombre like 'k-means' and m.parametro=10 and "+
            "s.fecha_recoleccion >= \'"+ ini +"\' and s.fecha_recoleccion<= \'"+ fin +
            "\' and d.nombre in (\'"+ str(valor)+
            "\') group by s.fecha_recoleccion, v.nomenclatura,v.color order by s.fecha_recoleccion").fetchall())
    elif len(deps)>1:
        df_secu=pd.DataFrame(conn.execute(f"select count(s.id_secuencia),s.fecha_recoleccion, v.nomenclatura as variante, v.color from secuencias as s "+
                "LEFT JOIN departamentos as d ON s.id_departamento=d.id_departamento "+
                "LEFT JOIN agrupamiento as a ON s.id_secuencia=a.id_secuencia "+
                "LEFT JOIN variantes as v ON a.id_variante=v.id_variante "+
                "LEFT JOIN algoritmos as m ON a.id_algoritmo=m.id_algoritmo "+
                "where m.nombre like 'k-means' and m.parametro=10 and "+
                "s.fecha_recoleccion >= \'"+ ini +"\' and s.fecha_recoleccion<= \'"+ fin +
                "\' and d.nombre in "+ str(deps)+
                " group by s.fecha_recoleccion, v.nomenclatura,v.color order by s.fecha_recoleccion").fetchall())
    else:
        return 'No hay datos'
    if df_secu.empty:
        return 'No hay datos'
    else:
        df_secu.columns=['count','fecha','variante','color']
        return df_secu

@tiempo.post("/graficolineal/")
def grafico(fechaIni: str,fechaFin: str,deps: List[str]):
    if len(deps)==25:
        deps=todos
    elif 'Todos' in deps:
        deps=todos
    result = tuple(deps)
    df_secu=data_secuencias(fechaIni,fechaFin,result)
    if str(df_secu) == 'No hay datos':
        return 'No hay datos'
    else:
        fechas=list(set(df_secu['fecha']))
        variantes=list(set(df_secu['variante']))
        colores=list()
        for v in variantes:
            fechas_variante=list(set(df_secu.loc[df_secu['variante']==v]['fecha']))
            colores.append(list(df_secu.color.loc[df_secu['variante']==v])[0])
            for m in fechas:
                if m not in fechas_variante:
                    df_secu=df_secu.append(pd.Series([0, m, v,df_secu.loc[df_secu['variante']==v]['color'].iloc[0]], index=df_secu.columns),ignore_index=True)

        ####Nueva versión############
        df_secu['fecha'] = pd.to_datetime(df_secu['fecha'])

        df_secuencias = pd.DataFrame()
        for name,color in zip(variantes,colores):
            df_variante = pd.DataFrame(df_secu.loc[df_secu['variante']==name].sort_values('fecha')).reset_index(drop=True)
            df_variante = df_variante.set_index('fecha') 
            if variantes[0] == name:
                df_secuencias['count'] = df_variante['count'].resample('W-MON').sum()
                df_secuencias['count_sum'] = df_variante['count'].resample('W-MON').sum()
                df_secuencias['variante']=name
                df_secuencias['color']=df_variante.loc[df_variante['variante']==name]['color'].iloc[0]
                df_secuencias.reset_index(inplace=True, drop=False)
            else:
                df=pd.DataFrame()
                df['count'] = df_variante['count'].resample('W-MON').sum()
                df['count_sum'] = df_variante['count'].resample('W-MON').sum()
                df['variante']=name
                df['color']=df_variante.loc[df_variante['variante']==name]['color'].iloc[0]
                df.reset_index(inplace=True, drop=False)
                df_secuencias = pd.concat([df_secuencias, df])

        #####################################

        p = figure(tools="pan,zoom_in,zoom_out,undo,redo,reset,save",plot_width=1450, plot_height=700, x_axis_type="datetime")

        hover=HoverTool(tooltips=[('Fecha de recolección', '@fecha{%d-%m-%Y}'),
                                ("Cantidad de secuencias genómicas","@count"),
                                ("Variante",'@variante'),
                                ("Color","$variante $swatch:color")],formatters={'@fecha': 'datetime'})
        p.add_tools(hover)

        #for name,color in zip(variantes,colores):
        #    df = pd.DataFrame(df_secu.loc[df_secu['variante']==name].sort_values('fecha')).reset_index(drop=True)
        #    p.line('fecha','count',source=df, line_width=2, color=color, alpha=0.8,
        #        muted_color=color, muted_alpha=0.2, legend_label=name)

        count=0
        temporal=pd.DataFrame()
        temporal['count_sum']=0
        for name,color in zip(variantes,colores):
            df = pd.DataFrame(df_secuencias.loc[df_secuencias['variante']==name].sort_values('fecha')).reset_index(drop=True)
            if count == 0:
                #guardar en un temporal
                temporal['count_sum']=df['count']
                #guardar en el principal
                df['count_sum']=temporal['count_sum']
                count=count+1
            else:
                #sumar el count con count_sum
                temporal['count_sum']=np.array(temporal['count_sum'])+np.array(df['count'])
                #actualizar el count_sum
                df['count_sum']=temporal['count_sum']
                count=count+1
            p.line('fecha','count_sum',source=df, line_width=2, color=color, alpha=0.8,
                    muted_color=color, muted_alpha=0.2, legend_label=name)

        p.legend.location = "top_left"
        p.legend.title = 'Variantes'
        p.legend.orientation = "horizontal"
        p.legend.title_text_font_style = "bold"
        p.legend.title_text_font_size = "15px"
        p.legend.label_text_font_size = '11pt'
        p.legend.click_policy = "hide"
        p.xaxis.axis_label = "Día-Mes-Año"
        p.yaxis.axis_label="Número de secuencias genómicas"
        p.xaxis.axis_label_text_font_style = "bold"
        p.yaxis.axis_label_text_font_style = "bold"
        p.xaxis.axis_label_text_font_size = "15px"
        p.yaxis.axis_label_text_font_size = "15px"
        p.xaxis.major_label_text_font_style="bold"
        p.yaxis.major_label_text_font_style="bold"
        p.xaxis.major_label_text_font_size="12px"
        p.yaxis.major_label_text_font_size="12px"
        p.xaxis.formatter=DatetimeTickFormatter(days=["%d-%m-%Y"],months=["%d-%m-%Y"],years=["%d-%m-%Y"])
        return json.dumps(json_item(p, "graficolineal"))


@tiempo.post("/graficocircular/")
def grafico(fechaIni: str,fechaFin: str,deps: List[str]):
    if len(deps)==25:
        deps=todos
    elif 'Todos' in deps:
        deps=todos
    result = tuple(deps)
    df_secu=data_secuencias(fechaIni,fechaFin,result)
    if str(df_secu) == 'No hay datos':
        return 'No hay datos'
    else:
        data=pd.DataFrame(df_secu.groupby(by=["variante","color"]).sum()[["count"]])
        data.rename(columns={"variante":"variante","color":"color","count":"count"})
        data=data.reset_index()
        data['angulo'] = data['count']/data['count'].sum()*2*pi
        data['porcentaje'] = round(data['count']/data['count'].sum()*100,2)

        p = figure(tools="pan,zoom_in,zoom_out,undo,redo,reset,save",plot_width=700, plot_height=600,
                tooltips=[("Variante","@variante"),("Porcentaje","@porcentaje{1.11} %"), ("Cantidad","@count")])

        p.wedge(x=0, y=1, radius=0.8,
                start_angle=cumsum('angulo', include_zero=True), end_angle=cumsum('angulo'),
                line_color="white", fill_color='color', legend_field='variante', source=data)

        p.axis.axis_label=None
        p.axis.visible=False
        p.grid.grid_line_color = None

        p.legend.title = 'Variantes'
        p.legend.title_text_font_style = "bold"
        p.legend.title_text_font_size = "15px"
        p.legend.label_text_font_size = '11pt'
        p.legend.label_standoff = 2
        p.legend.glyph_width =20
        p.legend.glyph_height=20
        p.legend.spacing = 0
        p.legend.padding = 1
        p.legend.margin = 5
        return json.dumps(json_item(p, "graficocircular"))