from bokeh.core.property.primitive import Int
from fastapi import APIRouter, File, UploadFile
from starlette.responses import JSONResponse
from config.db import conn
import json
from bokeh.embed import json_item
import pandas as pd
from bokeh.plotting import figure
from bokeh.models import HoverTool
from math import pi
from bokeh.transform import cumsum
from typing import List
import numpy as np
from Bio import SeqIO, Seq
from Bio.SeqIO.FastaIO import FastaIterator
import shutil
import pickle
from pygrok import Grok
from models.secuencias import secuencias
from models.variantes import variantes
from Bio.Align import MultipleSeqAlignment
from datetime import date
from sklearn.cluster import KMeans
from scipy.spatial.distance import pdist, hamming, squareform
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

'''def ham(seq1,seq2):
    return hamming(seq1,seq2)

def obtenerDatos(registros):
    abrevPlaces=list() #Lista de las abreviaciones de los lugares
    fechas=list() #Lista de las fechas
    secuencias1=list() #Lista de las secuencias
    secuenciasEliminadas=list() #Lista de las secuencias eliminadas
    for i in range(len(registros)):
        name=registros[i].id
        #Obtener la abreviación del nombre del departamento
        primer_indice=name.find('/')
        segundo_indice = name.find('/', primer_indice + 1)
        place=name[segundo_indice+1:segundo_indice+4]
        #Secuencias que no tienen un lugar definido
        if not place in lista:
            secuenciasEliminadas.append(registros[i])
            continue
        else:
            if not place in abrevPlaces:
                abrevPlaces.append(place)
            #Obtener el código de la secuencia
            primer_indice=name.find('|')
            segundo_indice = name.find('|', primer_indice + 1)
            codigo=name[primer_indice+1:segundo_indice]
            #Obtener la fecha de recolección
            valor=grok.match(name)
            if valor == None:
                secuenciasEliminadas.append(registros[i])
                continue
            else:
                fecha=valor['year'] + '-' + valor['month'] + '-' + valor['day']
                #Guardar los datos obtenidos
                registros[i].name=diccionario[place]
                registros[i].description=fecha   
                registros[i].id=codigo 
                fechas.append(fecha)
                #Guardar la secuencia
                secuencias1.append(registros[i])
    #Eliminarción de secuencias con errores de lectura
    pos,cantSecEli=0,0
    while pos<len(secuencias1):
        registro=set(secuencias1[pos].seq)
        if 'N' in registro or 'K' in registro or 'M' in registro or 'R' in registro or 'S' in registro or 'W' in registro or 'Y' in registro:
            secuencias1.pop(pos)
            cantSecEli+=1
        else:
            pos+=1
    return secuencias1

def obtenerDf(df):
    variants=pd.DataFrame(conn.execute(variantes.select()).fetchall())
    variants.columns=['id_variante', 'nomenclatura', 'linaje_pango','sustituciones_spike','nombre','color']
    linajes_pangos=[]
    ids_linajes_pangos=[]
    for i in range(len(df)):
        pango=df.iloc[i].linaje
        #verificar que variante le corresponde
        for v in range(len(variants)):
            valores=variants.iloc[v]['linaje_pango']
            for val in valores:
                if 'sublinajes' in val:
                    val=val.replace('sublinajes ',"")
                    if val in pango:
                        df['variante'][i]=variants.iloc[v]['nomenclatura']
                        df['color'][i]=variants.iloc[v]['color']
                else:
                    if pango in val:
                        df['variante'][i]=variants.iloc[v]['nomenclatura']
                        df['color'][i]=variants.iloc[v]['color']
        if str("nan") == str(df['variante'][i]):
            ids_linajes_pangos.append(df.iloc[i].id)
            linajes_pangos.append(pango)
            df['variante'][i]='Otro'
            df['color'][i]=variants.iloc[10]['color']
    return df

def clustering(df,X_pca,matriz_mds2):
    k=len(df['variante'].unique())
    k_means = KMeans(init = "k-means++", n_clusters = k, n_init = 12).fit(matriz_mds2)
    k_means_labels = k_means.labels_
    df_k_means=pd.DataFrame(k_means.labels_)
    df_k_means.columns=['cluster_kmeans_10']
    df_k_means['x']=X_pca[:,0]
    df_k_means['y']=X_pca[:,1]
    df_k_means['color']=''
    df_k_means['grupo']=''
    df_k_means['variante']=df['variante']
    df_k_means['id']=df['id']
    todas_variantes=np.array(df['variante'].unique())
    todas_variantes=todas_variantes.tolist()
    lista_variantes=[]
    for i in range(k):
        variantes_x_cantidad=df_k_means.loc[df_k_means['cluster_kmeans_10']==i]['variante'].value_counts()
        cantidades_x_grupo=np.array(variantes_x_cantidad.values) #cantidades de las variantes
        variantes_x_grupo=np.array(variantes_x_cantidad.index) #nombre de las variantes
        máximo_valor=np.amax(variantes_x_cantidad) #el valor de la variante que más se repite
        indice=np.where(variantes_x_cantidad==máximo_valor)[0][0] #se obtiene el indice del máximo valor
        name_variante=variantes_x_grupo[indice] #Se obtiene el nombre de la variante que más se repite
        bandera=0
        while bandera==0:
            if name_variante in lista_variantes:
                #Si esta, se opta por el siguiente mayor
                cantidades_x_grupo = np.delete(cantidades_x_grupo, indice)
                variantes_x_grupo = np.delete(variantes_x_grupo, indice)
                if len(cantidades_x_grupo)==0:
                    variantes_restantes = [v for v in todas_variantes if v not in lista_variantes]
                    name_variante = variantes_restantes[0]
                else:
                    máximo_valor=np.amax(cantidades_x_grupo)
                    indice=np.where(cantidades_x_grupo==máximo_valor)[0][0]
                    name_variante=variantes_x_grupo[indice]
                bandera=0
            else:
                #Sino esta, significa que se puede signar a este grupo
                df_k_means['grupo']=np.where(df_k_means['cluster_kmeans_10']==i,name_variante,df_k_means['grupo'])
                df_k_means['color']=np.where(df_k_means['cluster_kmeans_10']==i,np.array(df.loc[df['variante']==name_variante]['color'])[0],df_k_means['color'])
                #agregar a la lista
                lista_variantes.append(name_variante)
                bandera=1
    return df_k_means

def guardarDatosConAgrupamiento(df_k_means,df):
    k=len(df['variante'].unique())
    #GUARDAR grupos K-MEANS
    nombre_algoritmo="'k-means'"
    df_k_means1=pd.DataFrame(conn.execute(f"select id_algoritmo, parametro from algoritmos where nombre={nombre_algoritmo}").fetchall())
    df_k_means1.columns=['id_algoritmo','parametro']
    #cantidad=conn.execute(f"SELECT count(*) FROM variantes").fetchall()[0][0]
    array=[]

    algoritmo_id=df_k_means1.loc[df_k_means1['parametro']==k]['id_algoritmo'].iloc[0]
    for i in range(len(df_k_means)):
        indice=i
        id=conn.execute(f"select id_secuencia from secuencias where codigo=\'{df['id'][indice]}\'").fetchall()[0][0]
        variante_id=conn.execute(f"select id_variante from variantes where nomenclatura=\'{df_k_means['grupo'][indice]}\'").fetchall()[0][0]
        tupla=(int(algoritmo_id),int(id),int(variante_id),int(df_k_means['cluster_kmeans_10'][indice]+1))
        array.append(tupla)
    #INSERTAR EN BD
    args_str = b','.join(cur.mogrify("(%s,%s,%s,%s)", x) for x in array)
    cur.execute(b"INSERT INTO public.agrupamiento(id_algoritmo, id_secuencia, id_variante,num_cluster) VALUES " + args_str)
    conexion.commit()

def guardarDatos(df):
    k=len(df['variante'].unique())
    nombre_algoritmo="'k-means'"
    df_k_means1=pd.DataFrame(conn.execute(f"select id_algoritmo, parametro from algoritmos where nombre={nombre_algoritmo}").fetchall())
    df_k_means1.columns=['id_algoritmo','parametro']
    array=[]
    algoritmo_id=df_k_means1.loc[df_k_means1['parametro']==k]['id_algoritmo'].iloc[0]
    for i in range(len(df)):
        indice=i
        id=conn.execute(f"select id_secuencia from secuencias where codigo=\'{df['id'][indice]}\'").fetchall()[0][0]
        variante_id=conn.execute(f"select id_variante from variantes where nomenclatura=\'{df['variante'][indice]}\'").fetchall()[0][0]
        tupla=(int(algoritmo_id),int(id),int(variante_id),int(k+1))
        array.append(tupla)
    #INSERTAR EN BD
    args_str = b','.join(cur.mogrify("(%s,%s,%s,%s)", x) for x in array)
    cur.execute(b"INSERT INTO public.agrupamiento(id_algoritmo, id_secuencia, id_variante,num_cluster) VALUES " + args_str)
    conexion.commit()

@online.post("/online/")
async def subir_varios_archivos(valor: Int,archivos: List[UploadFile] = File(...)):
    try:
        #Recuperar archivos
        archiv=conn.execute(f"select pca from archivos where id_archivo=3;").fetchall()
        pca = pickle.loads(archiv[0][0])
        archiv=conn.execute(f"select matriz_distancia from archivos where id_archivo=2;").fetchall()
        matriz_secuencias = pickle.loads(archiv[0][0])
        archiv=conn.execute(f"select matriz_distancia from archivos where id_archivo=3;").fetchall()
        X_pca = pickle.loads(archiv[0][0])
        archiv=conn.execute(f"select mds from archivos where id_archivo=2;").fetchall()
        mds = pickle.loads(archiv[0][0])
        archiv=conn.execute(f"select matriz_distancia from archivos where id_archivo=1;").fetchall()
        matriz_distancias = pickle.loads(archiv[0][0])
        #Leer neuvos archivos
        for arc in archivos:
            with open(arc.filename, "wb") as buffer:
                shutil.copyfileobj(arc.file, buffer)
        registros = list(SeqIO.parse(archivos[0].filename, "fasta"))
        df_info = pd.read_csv(archivos[1].filename,sep='\t')
        #Obtener datos de las nuevas secuencias genómicas
        secuencias1=obtenerDatos(registros)
        #Recuperar secuencias registradas
        secuencias_registradas=pd.DataFrame(conn.execute(secuencias.select()).fetchall())
        secuencias_registradas.columns=['id_secuencia', 'codigo', 'secuencia','fecha_recoleccion','secuencia_alineada','id_departamento','linaje_pango']
        #Obtener las nuevas secuencias
        secuencias_nuevas=list()
        for i in range(len(secuencias1)):
            #si no se encuentra el id de la secuencia ya regsitrada entonces es una nueva secuencia
            if not secuencias1[i].id in list(secuencias_registradas['codigo']):
                secuencias_nuevas.append(secuencias1[i])
        #Obtener el linake pango de las secuencias
        df = pd.DataFrame(columns=['id', 'secuencia', 'secuenciaAlineada','lugar','fecha','linaje','variante','color'])
        for secu in secuencias_nuevas:
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
        #Alineamiento múltiple de las secuencias1
        maxlongitud = len(matriz_secuencias[0])
        i=0
        for registro in secuencias_nuevas:
            if len(registro.seq) != maxlongitud:
                secuencia = str(registro.seq).ljust(maxlongitud, '.')
                registro.seq = Seq.Seq(secuencia)
            df['secuenciaAlineada'][i]=registro.seq
            i=i+1
        secuenciasAlineadas_nuevas = MultipleSeqAlignment(secuencias_nuevas)
        #Insertar datos en BD
        dic_dep=dict(conn.execute(f"SELECT nombre,id_departamento FROM departamentos").fetchall())
        array=[]
        for i in range(len(df)):
            id_dep=dic_dep[df.iloc[i]['lugar']]
            tupla=(df.iloc[i]['id'],str(df.iloc[i]['secuencia']),date.fromisoformat(df.iloc[i]['fecha']),str(df.iloc[i]['secuenciaAlineada']),int(id_dep),str(df.iloc[i]['linaje']))
            array.append(tupla)
        args_str = b','.join(cur.mogrify("(%s,%s,%s,%s,%s,%s)", x) for x in array)
        cur.execute(b"INSERT INTO public.secuencias(codigo, secuencia, fecha_recoleccion,secuencia_alineada,id_departamento,linaje_pango) VALUES " + args_str)
        conexion.commit()

        #Calcular la matriz de distancia Hamming
        matriz_secuencias_nuevas= np.array([ list(secuencia.seq) for secuencia in secuenciasAlineadas_nuevas])
        if len(matriz_secuencias_nuevas)==1:
            matrizNueva=np.append(matriz_secuencias,[matriz_secuencias_nuevas],axis= 0)
        else:
            matrizNueva=np.concatenate((matriz_secuencias, matriz_secuencias_nuevas), axis=0)
        distancia_condensada2=pdist(matrizNueva, ham)
        dist_condensada2=np.around(np.array(distancia_condensada2),2)
        matriz_distancias2=squareform(dist_condensada2)
        #Calculo de MDS
        matriz_mds2=mds.fit_transform(matriz_distancias2,matriz_distancias)
        #PCA
        cantAgregadas=len(matriz_secuencias_nuevas)
        matriz_pca=matriz_mds2[len(matriz_mds2)-cantAgregadas:len(matriz_mds2)]
        if len(matriz_pca) == 1:
            matriz_pca=matriz_mds2[len(matriz_mds2)-cantAgregadas-1:len(matriz_mds2)]
            X_pca2=pca.transform(matriz_pca)
            X_pca=np.append(X_pca,[X_pca2[1]],axis= 0)
        else:
            X_pca2=pca.transform(matriz_pca)
            X_pca=np.concatenate((X_pca,X_pca2),axis= 0)

        if valor ==1:
            #Sin agrupamiento
            df=obtenerDf(df)
            k=len(df['variante'].unique())
            #Guardar datos en BD
            guardarDatos(df)

        elif valor == 2:
            #Con agrupamiento
            df=pd.DataFrame(conn.execute(secuencias1.select()).fetchall())
            df.columns=['id', 'codigo', 'secuencia','fecha','secuenciaAlineada','lugar','linaje']
            df['variante']=''
            df['color']=''
            df=obtenerDf(df)
            #Entrenar modelos
            df_agrup=clustering(df,X_pca,matriz_mds2)
            #SE ELIMINA LA AGRUPACIÓN ANTERIOR Y SE GUARDA LA ACTUAL
            conn.execute(f"truncate table agrupamiento").fetchall()
            #Guardar datos en BD
            guardarDatosConAgrupamiento(df_agrup,df)

        return JSONResponse(content={"upload_files": True},status_code=200)
    except FileNotFoundError:
        return JSONResponse(content={"upload_files": False},status_code=404)


@online.post("/eliminar/")
def eliminarSecuencias(ids: List[str]):
    cur.execute(b"DELETE FROM agrupamiento WHERE id_secuencia " + ids)
    conexion.commit()
    return JSONResponse(content={"eliminar": True},status_code=200)'''