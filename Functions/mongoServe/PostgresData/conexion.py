import psycopg2
import asyncio 
import time
import os
from datetime import datetime,timezone
import pandas as pd
class __BaseDatos:
    def __init__(self):
        self.__connection = psycopg2.connect(
                host = os.getenv("Postgres_IP"),
                user = os.getenv("Postgres_User"),
                password = os.getenv("Postgres_pass"),
                database = os.getenv("Postgres_DB"),
                port=os.getenv("Postgres_Port")
            )
        # self.__connection.autocommit(True)
        print("Conexión Exitosa")
    def qwerysAll(self,consulta):
        self.cursor = self.__connection.cursor()
        try:
            self.sql = consulta
            self.cursor.execute(self.sql)
            self.rows = self.cursor.fetchall()
            self.cursor.close()
            return self.rows
        except psycopg2.InterfaceError:
            self.__connection = psycopg2.connect(
                host = os.getenv("Postgres_IP"),
                user = os.getenv("Postgres_User"),
                password = os.getenv("Postgres_pass"),
                database = os.getenv("Postgres_DB"),
                port=os.getenv("Postgres_Port")
            )
            print("Conexión Exitosa")
        except Exception as e:
            self.__connection.rollback()
            self.cursor.close()
            print(f"error de consulta {e}")

    def updateOne(self,consulta):
        self.cursor = self.__connection.cursor()
        try:
            self.sql = consulta
            self.cursor.execute(self.sql)
            self.__connection.commit()
            self.cursor.close()
        except psycopg2.InterfaceError:
            self.connection = psycopg2.connect(
                host = os.getenv("Postgres_IP"),
                user = os.getenv("Postgres_User"),
                password = os.getenv("Postgres_pass"),
                database = os.getenv("Postgres_DB"),
                port=os.getenv("Postgres_Port")
            )
            print("Conexión Exitosa")
        except Exception as e:
            self.__connection.rollback()
            self.cursor.close()
            print( f"Error de commit {e}")
    def Back(self):
        self.__connection.rollback()
        self.cursor.close()
__db=__BaseDatos()
events=asyncio.get_event_loop()

#________________________________procesos____________________________________________________#
def GPRS(val=True):
    try:
        id=[]
        name=[]
        for x in __db.qwerysAll("select station_code,mongo_name FROM stations where types ='GPRS' and mongo_name is not null  and active=True"):
            id.append(x[0])
            name.append(x[1])
        tupla = {id:name for (id,name) in zip(id,name)}
    except:
        if val:
            time.sleep(0.5)
            tupla=GPRS(False)
            time.sleep(0.5)
        else:
            tupla={}
    return tupla
def GPRS_5(val=True):
    try:
        id=[]
        name=[]
        for x in __db.qwerysAll("select station_code,mongo_name FROM stations where types ='GPRS_5' and mongo_name is not null  and active=True"):
            id.append(x[0])
            name.append(x[1])
        tupla = {id:name for (id,name) in zip(id,name)}
    except:
        if val:
            time.sleep(0.5)
            tupla=GPRS_5(False)
            time.sleep(0.5)
        else:
            tupla={}
    return tupla
def fails_d(val=True):
    truco=[]
    try:
        for x in  __db.qwerysAll("select station from failures where disconect=true"):
            truco.append(x[0])
    except:
        if val:
             time.sleep(0.5)
             truco= fails_d(False)
             time.sleep(0.5)
        else:
            truco=[]
    return truco
def fails_f(val=True):
    truco=[]
    try:
        for x in  __db.qwerysAll("select station from failures where disconect=false"):
            truco.append(x[0])
    except:
        if val:
             time.sleep(0.5)
             truco= fails_f(False)
             time.sleep(0.5)
        else:
            truco=[]
    return truco
def rad(val=True):
    id=[]
    try:
        for x in  __db.qwerysAll("select station_code FROM stations_rads where active=true order by station_code ASC"):
            id.append(x[0])
    except:
        if val:
             time.sleep(0.5)
             id= fails_f(False)
             time.sleep(0.5)
        else:
            id=[]
        __db.Back()
    return id
#________________________________procesos___________________________________________
def getMuertos():
    return fails_d()

def getEstacionesF():
    return fails_f()
def getMinutal():
    return GPRS()

def get5Minutal():
    return GPRS_5()
def rad_active():
    return rad()
#________________________________getters___________________________________________


def RegistrarNuevo(estacion,hora):
   __db.updateOne(f"insert into failures values ('{estacion}',1,'f','{hora}','{hora}')")
def AgregarFallo(estacion,fallo,hora):
    __db.updateOne(f"update failures set falls={fallo},hour_last='{hora}' where station = '{estacion}' ")
def Quitar(estacion): 
    __db.updateOne(f"DELETE FROM failures where station = '{estacion}' ")
    try:
        __db.updateOne(f"update stations set active = True where mongo_name='{estacion}'")
    except:
        None
def Matar(estacion):
    __db.updateOne(f"update failures set disconect = True where station = '{estacion}' ")


#---------------------------------------------------------devop---------------------------------------------------


# def __Foraneas(tabla,columna,referenia,llave,const):
#     __db.updateOne(f"ALTER TABLE {tabla} ADD CONSTRAINT {const} FOREIGN KEY ({columna}) REFERENCES {referenia} ({llave});")
# async def __ConsultaFallo(estacion):
#     fin=await __db.qwerysAll(f"select hour_last from failures where station='{estacion}'")
#     ini=await __db.qwerysAll(f"select hour_start from failures where station='{estacion}'")
#     ini=ini[0][0]
#     fin=fin[0][0]
#     ultimo=ini-timedelta(minutes=5)
#     print("ultimo registro: {Anio:"+ultimo.strftime("%Y")+",Mes:"+ultimo.strftime("%m")+",Dia:"+ultimo.strftime("%d")+",Hora:"+ultimo.strftime("%H"),",Minuto:"+ultimo.strftime("%M")+"}" )
#     print("primera falla: {Anio:"+ini.strftime("%Y")+",Mes:"+ini.strftime("%m")+",Dia:"+ini.strftime("%d")+",Hora:"+ini.strftime("%H"),",Minuto:"+ini.strftime("%M")+"}" )
#     print("ultima falla: {Anio:"+fin.strftime("%Y")+",Mes:"+fin.strftime("%m")+",Dia:"+fin.strftime("%d")+",Hora:"+fin.strftime("%H"),",Minuto:"+fin.strftime("%M")+"}" )
def getFallos(estacion,Try=False):
    try:
        truco=__db.qwerysAll(f"select falls,hour_last from failures where station='{estacion}'")
        truco=truco[0]
        return truco
    except:
        if not Try: 
            caida=datetime.now(timezone.utc)
            caida.strftime("%Y-%m-%d %H:%M:%S")
            #RegistrarNuevo(estacion,caida)
            time.sleep(0.3)
            return getFallos(estacion,True)
        else:
             return "Fallo"
#print(getMinutal())
#events.run_until_complete(__ConsultaFallo("SLCHAL067Q_1001"))
def APIGP():
    id=[]
    name=[]
    addr=[]
    lat=[]
    lon=[]
    for x in __db.qwerysAll("select station_code,name,address_label,latitude,longitude FROM stations where mongo_name is not null  and active=True ORDER BY station_code"):
       id.append(x[0])
       name.append(x[1])
       addr.append(x[2])
       lat.append(x[3])
       lon.append(x[4])
    tupla = {id:[id,name,addr,lat,lon] for (id,name,addr,lat,lon) in zip(id,name,addr,lat,lon)}
    return(tupla)
def APIRAD():
    id=[]
    name=[]
    addr=[]
    lat=[]
    lon=[]
    for x in __db.qwerysAll("select station_code,name,address_label,latitude,longitude FROM stations_rads where active=True ORDER BY station_code"):
       id.append(x[0])
       name.append(x[1])
       addr.append(x[2])
       lat.append(x[3])
       lon.append(x[4])
    tupla = {id:[id,name,addr,lat,lon] for (id,name,addr,lat,lon) in zip(id,name,addr,lat,lon)}
    return tupla
#######################################################peticiones ISOYETAS########################################################################
def isoGprs():
    id=[]
    lat=[]
    lon=[]
    for x in __db.qwerysAll("select station_code,latitude,longitude FROM stations where mongo_name is not null  and active=True ORDER BY station_code"):
       id.append(x[0])
       lat.append(x[1])
       lon.append(x[2])
    tupla = {id:[id,lat,lon] for (id,lat,lon) in zip(id,lat,lon)}
    return(tupla)
def isoRad():
    id=[]
    lat=[]
    lon=[]
    for x in __db.qwerysAll("select station_code,latitude,longitude FROM stations_rads where active=True ORDER BY station_code"):
       id.append(x[0])
       lat.append(x[1])
       lon.append(x[2])
    tupla = {id:[id,lat,lon] for (id,lat,lon) in zip(id,lat,lon)}
    return tupla
#################################################################################################
def ppGPRS():
    id=[]
    pp=[]
    for x in __db.qwerysAll("select station_code , weight FROM stations where active=True and weight is not null and mongo_name is not null ORDER BY station_code"):
       id.append(x[0])
       pp.append(x[1])
    tupla = {id:pp for (id,pp) in zip(id,pp)}
    return tupla
def ppRADS():
    id=[]
    pp=[]
    for x in __db.qwerysAll("select station_code , weight FROM stations_rads where active=True and weight is not null ORDER BY station_code"):
       id.append(x[0])
       pp.append(x[1])
    tupla = {id:pp for (id,pp) in zip(id,pp)}
    return tupla
# def pp_update():
#     df=pd.read_csv("C:/Users/sacmex6704/Documents/sevicio/simollu-flask/resources/auxData/pp.csv", sep=';')
#     for x in  range(len(df)):
#        __db.updateOne(f"update stations set weight={df.loc[x, 'valor_coeficiente_pp']} where station_code = '{df.loc[x, 'cod_estacion']}' ")
#        __db.updateOne(f"update stations_rads set weight={df.loc[x, 'valor_coeficiente_pp']} where station_code = '{df.loc[x, 'cod_estacion']}' ")
# pp_update()