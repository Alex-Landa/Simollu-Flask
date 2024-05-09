import pandas as pd
import os
import time
from datetime import datetime
#import mongoServe.PostgresData.conexion as post 
import Functions.mongoServe.PostgresData.conexion as post 
def __sumatoria(valores,pp):
    cantidad=0.0
    if len(valores)>0:
        for x in valores:
            cantidad=cantidad+float(x)
        cantidad=cantidad*pp
    return cantidad
def __lluviaTotal(ruta,nombre:str,pp:float):
    df = pd.read_csv(f"{ruta}/{nombre}.csv", sep=';')
    ext=[]

    for x in range(0,len(df),1):
        if float(df.loc[x, 'Value_Acum'])!=0.0:
            ext.append(df.loc[x,'Value_Acum'])
    
    return __sumatoria(ext,pp) 


def avgGprs(nombre="actual",rep=True):
        promedio=0
        aux=[]
    # try:
        stations=post.ppGPRS()
        ruta=os.getcwd()
        ruta=f"{ruta}/datos"
        for x in stations.keys():
            aux.append(__lluviaTotal(f"{ruta}/{x}",nombre,stations.get(x)))
        for y in aux:
            promedio=promedio+y
        return round(promedio, 4)  
    # except:
    #     time.sleep(1)
    #     if(rep):
    #         return avgGprs(nombre,False)
    #     else:
    #         return 0

def avgRadio(nombre="actual",rep=True):
    promedio=0
    aux=[]
    try:
        stations:dict=post.ppRADS()
        ruta=os.getcwd()
        ruta=f"{ruta}/datosSQL"
        for x in stations.keys():
            aux.append(__lluviaTotal(f"{ruta}/{x}",nombre,stations.get(x)))
        for y in aux:
            promedio=promedio+y
        return round(promedio, 4)
    except:
        time.sleep(1)
        if(rep):
            return avgRadio(nombre,False)
        else:
            return 0