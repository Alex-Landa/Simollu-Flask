import os
import pandas as pd
import csv
from pyproj import Proj, transform
import shutil
# import PostgresData.conexion as post
import Functions.mongoServe.PostgresData.conexion as post
import subprocess 
from datetime import datetime
import time
import Functions.isoyetas.pdfGenerator as pdf
__ruta=os.getcwd()

def __executer( route, coords:dict,day):
        id:int
        total:int
        utmx:float
        utmy:float
        __Reescribir(ruta=f'{__ruta}/Functions/isoyetas/isoHist',planchar=True)
        for x in coords.keys():
            id=int(x)-1000
            print(f"{route}/{x}",f"{day}_historico")
            total=__sumatoria(f"{route}/{x}",f"{day}_historico")
            try:
                utmx,utmy=__transformacion(coords.get(int(x))[3],coords.get(int(x))[4],14)
                __Reescribir(f'{__ruta}/Functions/isoyetas/isoHist',id,total,utmx,utmy,coords.get(int(x))[3],coords.get(int(x))[4])
            except Exception as e:
                print(e)
                continue
        subprocess.run(['Rscript', f'{__ruta}/Functions/isoyetas/Isoyetas_Historical.R'], check=False)

def __Reescribir(ruta:str,id:int=0,acum:float=0,utmx:float=0,utmy:float=0,lat:float=0,lon:float=0,planchar=False):
    if planchar:
        with open(f'{ruta}.csv', 'w', newline='') as csvfile:
            spamwriter = csv.writer(csvfile, delimiter=';',
                                    quotechar=';', quoting=csv.QUOTE_MINIMAL)
            spamwriter.writerow(['station_id'] + ['value_acum']+['valor_utm_x']+['valor_utm_y']+['lng']+['lat'])
    else:        
        with open(f'{ruta}.csv', 'a', newline='') as csvfile:
            spamwriter = csv.writer(csvfile, delimiter=';', quotechar=';', quoting=csv.QUOTE_MINIMAL)
            spamwriter.writerow([f'{id}', f'{acum}',f'{utmx}',f'{utmy}',f'{lon}',f'{lat}'])


def __total(valores):
    cantidad=0.0
    if len(valores)>0:
        for x in valores:
            cantidad=cantidad+float(x)
    return cantidad
def __sumatoria(ruta,nombre="actual"):
    df = pd.read_csv(f"{ruta}/{nombre}.csv", sep=';')
    lista=[]
    for x in range(0,len(df),1):
        if float(df.loc[x, 'Value_Acum'])!=0.0:
            lista.append(df.loc[x, 'Value_Acum'])
    
    acum=__total(lista)
    return acum
def __transformacion(lat, lon, zone_number):
    # Definir el sistema de coordenadas para latitud y longitud (EPSG:4326)
    wgs84 = Proj(init='epsg:4326')

    # Definir el sistema de coordenadas UTM para la zona especificada
    utm = Proj(proj='utm', zone=zone_number, datum='WGS84')

    # Convertir latitud y longitud a UTM
    utm_x, utm_y = transform(wgs84, utm, lon, lat)

    return utm_x, utm_y

def Generation(type:str,day,series:str,pp):
    isoRoute=f"{__ruta}/Functions/isoyetas/historico"
    img=""
    if (not os.path.exists(isoRoute)):
        os.mkdir(isoRoute)
        os.mkdir(f"{isoRoute}/imgHistGp")
        os.mkdir(f"{isoRoute}/imgHistRad")
    if(type=="GPRS"):
        aux=__ruta+"/datos"
        coords=post.APIGP()
        img="historico/imgHistGp"
        if (not os.path.exists(f"{isoRoute}/imgHistGp/{day}kriging.png")):
            __executer(aux,coords,day)
            shutil.move(f"{__ruta}/Functions/isoyetas/idwHist.png",f"{isoRoute}/imgHistGp/{day}idw.png")
            shutil.move(f"{__ruta}/Functions/isoyetas/krigingHist.png",f"{isoRoute}/imgHistGp/{day}kriging.png")
            shutil.move(f"{__ruta}/Functions/isoyetas/ThiensenHist.png",f"{isoRoute}/imgHistGp/{day}Thiensen.png")
            time.sleep(0.5)
    else:
        aux=__ruta+"/datosSQL"
        coords=post.APIRAD()
        img="historico/imgHistRad"
        if (not os.path.exists(f"{isoRoute}/imgHistRad/{day}kriging.png")):
            __executer(aux,coords,day)
            shutil.move(f"{__ruta}/Functions/isoyetas/idwHist.png",f"{isoRoute}/imgHistRad/{day}idw.png")
            shutil.move(f"{__ruta}/Functions/isoyetas/krigingHist.png",f"{isoRoute}/imgHistRad/{day}kriging.png")
            shutil.move(f"{__ruta}/Functions/isoyetas/ThiensenHist.png",f"{isoRoute}/imgHistRad/{day}Thiensen.png")
            time.sleep(0.5)
    pdf.create(type,f"{day}{series}","Hist",datetime.strptime(day,"%Y-%m-%d"),pp,img)
    return f"{isoRoute}/{day}{series}.pdf"