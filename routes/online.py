from bokeh.core.property.primitive import Int
from fastapi import APIRouter, File, UploadFile
from starlette.responses import JSONResponse
from config.db import conn
import pandas as pd
from typing import List
import numpy as np
from Bio import SeqIO, Seq
import shutil
import pickle
from pygrok import Grok
from models.variantes import variantes
from models.archivos import archivos
from Bio.Align import MultipleSeqAlignment
from datetime import date
from scipy.spatial.distance import hamming
#from keras.models import model_from_json
import psycopg2
conexion = psycopg2.connect("dbname='BDTesis' user='postgres' host='instanciatesis.cjfczpppafxb.us-east-1.rds.amazonaws.com' password='carolina19620'")
cur = conexion.cursor()

online = APIRouter()

grok = Grok('%{YEAR:year}-%{MONTHNUM:month}-%{MONTHDAY:day}')
lista = ['AMA','ANC','APU','ARE','AYA','CAJ','CUS','HUV','HUC','HVC','HUA','HCV','HUN','ICA','JUN','LAL','LAM','LIM','LOR','MDD','MOQ','PAS','PIU','PUN','SAM','SAN','TAC','TUM','UCA','CAL','C01','C02','C03']
diccionario = {'AMA' : 'Amazonas','ANC' : 'Áncash','APU' : 'Apurímac','ARE' : 'Arequipa',
               'AYA' : 'Ayacucho','CAJ' : 'Cajamarca','CUS' : 'Cusco',
               'HUV' : 'Huancavelica','HUC' : 'Huánuco',
               'HVC' : 'Huancavelica','HUA' : 'Huánuco',
               'HCV' : 'Huancavelica','HUN' : 'Huánuco',
               'ICA' : 'Ica','JUN' : 'Junín','LAL' : 'La Libertad',
               'LAM' : 'Lambayeque','LIM' : 'Lima','LOR' : 'Loreto','MDD' : 'Madre de Dios',
               'MOQ' : 'Moquegua','PAS' : 'Pasco','PIU' : 'Piura','PUN' : 'Puno',
               'SAM' : 'San Martín', 'SAN' : 'San Martín',
               'TAC' : 'Tacna','TUM' : 'Tumbes','UCA' : 'Ucayali',
               'CAL' : 'Callao', 'C01' : 'Callao', 'C02': 'Callao','C03': 'Callao'}

def matriz_secuencias_recuperado():
    archiv=conn.execute(f"select matriz_secuencias from archivos where id_archivo=2;").fetchall()
    return pickle.loads(archiv[0][0])

def matriz_distancias_recuperado():
    archiv=conn.execute(f"select matriz_distancia from archivos where id_archivo=3;").fetchall()
    return pickle.loads(archiv[0][0])

def mds_recuperado():
    archiv=conn.execute(f"select mds from archivos where id_archivo=4;").fetchall()
    return pickle.loads(archiv[0][0])

def matriz_mds_recuperado():
    archiv=conn.execute(f"select matriz_distancia from archivos where id_archivo=4;").fetchall()
    return pickle.loads(archiv[0][0])

def pca_recuperado():
    archiv=conn.execute(f"select pca from archivos where id_archivo=5;").fetchall()
    return pickle.loads(archiv[0][0])

def matriz_pca_recuperado():
    archiv=conn.execute(f"select matriz_distancia from archivos where id_archivo=5;").fetchall()
    return pickle.loads(archiv[0][0])

def landmark_recuperado():
    archiv=conn.execute(f"select landmark from archivos where id_archivo=6;").fetchall()
    return pickle.loads(archiv[0][0])

def array_landmark_recuperado():
    archiv=conn.execute(f"select puntos_antiguos from archivos where id_archivo=6;").fetchall()
    return pickle.loads(archiv[0][0])

'''def modelo_recuperado():
    archiv=conn.execute(f"select modelo from archivos where id_archivo=7;").fetchall()
    modelo_bd = pickle.loads(archiv[0][0])
    modelo_recuperado = model_from_json(modelo_bd)
    return modelo_recuperado'''

def puntos_antiguos_recuperado():
    archiv=conn.execute(f"select puntos_antiguos from archivos where id_archivo=8;").fetchall()
    return pickle.loads(archiv[0][0])

#Función de lectura de las secuencias
def lectura(registros):
    #Recuperar los códigos de las secuencias genómicas guardadas en BD
    secuencias_guardadasBD=pd.DataFrame(conn.execute(f"SELECT codigo FROM secuencias").fetchall())
    secuencias_guardadasBD.columns=['codigo']
    codigos=secuencias_guardadasBD['codigo'].tolist()

    abrevPlaces_1=list() #Lista de las abreviaciones de los lugares
    fechas_1=list() #Lista de las fechas
    secuencias_1=list() #Lista de las secuencias
    secuenciasEliminadas_1=list() #Lista de las secuencias eliminadas
    for i in range(len(registros)):
        name=registros[i].id
        #Obtener la abreviación del nombre del departamento
        primer_indice=name.find('/')
        segundo_indice = name.find('/', primer_indice + 1)
        place=name[segundo_indice+1:segundo_indice+4]
        #Secuencias que no tienen un lugar definido
        if not place in lista:
            secuenciasEliminadas_1.append(registros[i])
            continue
        else:
            if not place in abrevPlaces_1:
                abrevPlaces_1.append(place)
            #Obtener el código de la secuencia
            primer_indice=name.find('|')
            segundo_indice = name.find('|', primer_indice + 1)
            codigo=name[primer_indice+1:segundo_indice]
            #Obtener la fecha de recolección
            valor=grok.match(name)
            if valor == None:
                secuenciasEliminadas_1.append(registros[i])
                continue
            else:
                if not codigo in codigos:
                    fecha=valor['year'] + '-' + valor['month'] + '-' + valor['day']
                    #Guardar los datos obtenidos
                    registros[i].name=diccionario[place]
                    registros[i].description=fecha   
                    registros[i].id=codigo 
                    fechas_1.append(fecha)
                    #Guardar la secuencia
                    secuencias_1.append(registros[i])
    return secuencias_1

#Función para la Eliminación de las secuencias genómicas SARS-CoV-2 con errores de lectura
def eliminación_secuencias(secuencias_1):
    pos_1,cantSecEli_1=0,0
    while pos_1<len(secuencias_1):
        registro=set(secuencias_1[pos_1].seq)
        if 'N' in registro or 'K' in registro or 'M' in registro or 'R' in registro or 'S' in registro or 'W' in registro or 'Y' in registro:
            secuencias_1.pop(pos_1)
            cantSecEli_1+=1
        else:
            pos_1+=1
    return secuencias_1

#Función para realizar el Alineamiento múltiple de las secuencias genómicas SARS-CoV-2
def alineamiento_multiple(secuencias_1):
    maxlongitud=max(len(registro.seq) for registro in secuencias_1)
    i=0
    secuenciaAlineada=[]
    for registro in secuencias_1:
        if len(registro.seq) != maxlongitud:
            secuencia = str(registro.seq).ljust(maxlongitud, '.')
            registro.seq = Seq.Seq(secuencia)
        secuenciaAlineada.append(registro.seq)
        i=i+1
    secuenciasAlineadas_nuevas = MultipleSeqAlignment(secuencias_1)
    return secuenciasAlineadas_nuevas,secuencias_1,secuenciaAlineada

#Función que calcula la distancia hamming
def ham(seq1,seq2):
    return hamming(seq1,seq2)

#Función que calcula la distancia de las secuencias a cada landmark
def distancia_landmark(matriz_secuencias_nuevas,matriz_secuencias_antigua):
    landmark =landmark_recuperado()
    X1 = np.empty((0, 20), int)
    for i in range(len(matriz_secuencias_nuevas)):
        fila=[]
        #calcular la distancia de cada punto a los landmark
        for j in range(20):
            fila.append(np.around(ham(matriz_secuencias_nuevas[i],matriz_secuencias_antigua[landmark[j]]),2))
        X1 = np.append(X1, np.array([fila]), axis=0)
    return X1

#Función que calcula la distancia hamming de las nuevas secuencias
def distancia_hamming(secuenciasAlineadas_nuevas,matriz_secuencias_recuperado):
    matriz_secuencias_nuevas=np.array(secuenciasAlineadas_nuevas)

    antigua_maxlongitud = len(matriz_secuencias_recuperado[0])
    nueva_maxlongitud = len(matriz_secuencias_nuevas[0])

    if nueva_maxlongitud > antigua_maxlongitud:
        matriz_secuencias_antigua=np.empty((len(matriz_secuencias_recuperado), nueva_maxlongitud), str)
        for i in range(len(matriz_secuencias_recuperado)):
            for j in range(nueva_maxlongitud-antigua_maxlongitud):
                matriz_secuencias_antigua[i]=np.append(matriz_secuencias_recuperado[i], '.')
        X1=distancia_landmark(matriz_secuencias_nuevas,matriz_secuencias_antigua)
        nuevas_matriz_secuencias=np.append(matriz_secuencias_antigua, matriz_secuencias_nuevas, axis=0)
    else:
        for i in range(len(matriz_secuencias_nuevas)):
            matriz_secuencias_nuevas2=np.empty((len(matriz_secuencias_nuevas), antigua_maxlongitud), str)
            for j in range(antigua_maxlongitud-nueva_maxlongitud):
                matriz_secuencias_nuevas2[i]=np.append(matriz_secuencias_nuevas[i], '.')
        X1=distancia_landmark(matriz_secuencias_nuevas2,matriz_secuencias_recuperado)
        nuevas_matriz_secuencias=np.append(matriz_secuencias_recuperado, matriz_secuencias_nuevas2, axis=0)
    #Guardar en BD nuevas_matriz_secuencias
    pickle_matriz_secuencias = pickle.dumps(nuevas_matriz_secuencias)
    conn.execute(archivos.update().where(archivos.c.id_archivo == 2).values(matriz_secuencias=pickle_matriz_secuencias))
    return X1

#Función para guardar datos de las secuencias en BD
def guardar_datos(secuencias,df_info,secuenciaAlineada):
    df = pd.DataFrame(columns=['id', 'secuencia', 'secuenciaAlineada','lugar','fecha','linaje','id_variante','variante','color'])
    for secu in secuencias:
        df = df.append({'id': secu.id, 'secuencia':secu.seq, 'lugar':secu.name,'fecha':secu.description}, ignore_index=True)
    ids = np.array(df['id'])
    ids = ids.astype('str')
    cant=[]
    for i in range(len(df_info)):
        id=df_info.iloc[i]['Accession ID']
        variante=df_info.iloc[i]['Lineage']
        indice=np.where(ids == id)
        if len(indice[0]) == 1:
            df['linaje'][indice[0][0]]=variante
        else:
            #secuencias eliminadas
            cant=np.append(cant,[id],axis= 0)
    variants=pd.DataFrame(conn.execute(variantes.select()).fetchall())
    variants.columns=['id_variante', 'nomenclatura', 'linaje_pango','sustituciones_spike','nombre','color']
    linajes_pangos=[]
    ids_linajes_pangos=[]
    for i in range(len(df)):
        df['secuenciaAlineada'][i]=secuenciaAlineada[i]
        pango=str(df.iloc[i].linaje)
        #verificar que variante le corresponde
        for v in range(len(variants)):
            valores=variants.iloc[v]['linaje_pango']
            for val in valores:
                if 'sublinajes' in val:
                    val=val.replace('sublinajes ',"")
                    if val in pango:
                        df['id_variante'][i]=variants.iloc[v]['id_variante']
                        df['variante'][i]=variants.iloc[v]['nomenclatura']
                        df['color'][i]=variants.iloc[v]['color']
                else:
                    if pango in val:
                        df['id_variante'][i]=variants.iloc[v]['id_variante']
                        df['variante'][i]=variants.iloc[v]['nomenclatura']
                        df['color'][i]=variants.iloc[v]['color']
        if str("nan") == str(df['variante'][i]):
            ids_linajes_pangos.append(df.iloc[i].id)
            linajes_pangos.append(pango)
            df['id_variante'][i]=variants.iloc[10]['id_variante']
            df['variante'][i]='Otro'
            df['color'][i]=variants.iloc[10]['color']

    dic_dep=dict(conn.execute(f"SELECT nombre,id_departamento FROM departamentos").fetchall())
    array=[]
    for i in range(len(df)):
        id_dep=dic_dep[df.iloc[i]['lugar']]
        tupla=(df.iloc[i]['id'],str(df.iloc[i]['secuencia']),date.fromisoformat(df.iloc[i]['fecha']),str(df.iloc[i]['secuenciaAlineada']),int(id_dep),str(df.iloc[i]['linaje']),str(df.iloc[i]['variante']))
        array.append(tupla)
    
    #INSERTAR EN BD
    args_str = b','.join(cur.mogrify("(%s,%s,%s,%s,%s,%s,%s)", x) for x in array)
    cur.execute(b"INSERT INTO public.secuencias(codigo, secuencia, fecha_recoleccion,secuencia_alineada,id_departamento,linaje_pango,variante) VALUES " + args_str)
    conexion.commit()

    prueba=pd.DataFrame(conn.execute(f"SELECT id_secuencia, codigo FROM secuencias").fetchall())
    prueba.columns=['id_secuencia', 'id']
    df = pd.merge(prueba, df, on="id")

@online.post("/online/")
async def subir_varios_archivos(parametro: int,archivos: List[UploadFile] = File(...)):
    try:
        for arc in archivos:
            with open(arc.filename, "wb") as buffer:
                shutil.copyfileobj(arc.file, buffer)
        if "fasta" in archivos[0].filename:
            nombreFasta=archivos[0].filename
        elif "fasta" in archivos[1].filename:
            nombreFasta=archivos[1].filename
        if "tsv" in archivos[0].filename:
            nombreTsv=archivos[0].filename
        elif "tsv" in archivos[1].filename:
            nombreTsv=archivos[1].filename
        #registros = list(SeqIO.parse(nombreFasta, "fasta"))
        #df_info = pd.read_csv(nombreTsv,sep='\t')
        '''secuencias=lectura(registros)
        secuencias_1=eliminación_secuencias(secuencias)
        alineamiento_valores=alineamiento_multiple(secuencias_1)
        secuenciasAlineadas_nuevas=alineamiento_valores[0]
        secuencias_1=alineamiento_valores[1]
        secuenciaAlineada=alineamiento_valores[2]
        
        #Guardar secuencias en BD
        guardar_datos(secuencias_1,df_info,secuenciaAlineada)

        matriz_secuencias=matriz_secuencias_recuperado()
        X1=distancia_hamming(secuenciasAlineadas_nuevas,matriz_secuencias)
        modelo=modelo_recuperado()
        valores1=modelo.predict(X1)
        if parametro==0:
            #sin agrupamiento

            #GUARDAR DATOS EN BD
            archiv=conn.execute(f"select puntos_nuevos from archivos where id_archivo=9;").fetchall()
            if archiv:
                #hay datos
                valores_nuevos=np.append(archiv, valores1, axis=0)
                pickle_puntos_nuevos = pickle.dumps(valores_nuevos)
                conn.execute(archivos.update().where(archivos.c.id_archivo == 9).values(puntos_nuevos=pickle_puntos_nuevos))
            else:
                pickle_puntos_nuevos = pickle.dumps(valores1)
                conn.execute(archivos.insert().values(
                    puntos_nuevos = pickle_puntos_nuevos
                ))
        else:
            #realizar el agrupamiento de nuevo
            pass'''
        return True
    except FileNotFoundError:
        return False



@online.post("/eliminar/")
def eliminarSecuencias(codigos: List[str]):
    try:
        args_str = b','.join(cur.mogrify("%s", (x,)) for x in codigos)
        cur.execute(b"UPDATE secuencias SET estado = 0 WHERE codigo = " + args_str)
        conexion.commit()
        table=tabla()
        return True,table
    except:
        return False

@online.post("/tabla/")
def tabla():
    return conn.execute(f"SELECT d.nombre as nombre, s.codigo, s.fecha_recoleccion as fecha, s.linaje_pango as nomenclatura,s.variante as variante "+
                        "from departamentos as d "+
                        "LEFT JOIN secuencias as s ON d.id_departamento=s.id_departamento WHERE s.estado=1"+
                        "ORDER BY d.nombre ASC").fetchall()
