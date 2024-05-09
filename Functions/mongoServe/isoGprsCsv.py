import os
import pandas as pd
import csv
from pyproj import Proj, transform
#import PostgresData.conexion as post
import Functions.mongoServe.PostgresData.conexion as post
__ruta=os.getcwd()

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
def __sumatoria(id,nombre="actual"):
    df = pd.read_csv(f"{__ruta+id}/{nombre}.csv", sep=';')
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

def Generation():
    id:int
    total:int
    utmx:float
    utmy:float
    coords=post.APIGP()

    __Reescribir(ruta=f'{__ruta}/Functions/isoyetas/isoGP',planchar=True)
    for x in coords.keys():
        id=int(x)-1000
        total=__sumatoria(f"/datos/{x}")
        try:
            utmx,utmy=__transformacion(coords.get(int(x))[3],coords.get(int(x))[4],14)
            __Reescribir(f'{__ruta}/Functions/isoyetas/isoGP',id,total,utmx,utmy,coords.get(int(x))[3],coords.get(int(x))[4])
        except:
            continue
#print(__ruta)