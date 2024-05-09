import pymysql
from datetime import datetime,timedelta,timezone
import os
import csv
import pandas as pd
#import conexion as post
import Functions.myServe.Conexiones.conexionPost as post
###MYSQL conexion
class __Mysql:
    def __init__(self):
        self.__connection = pymysql.connect(
            host = os.getenv("Mysql_ip"),
            user = os.getenv("Mysql_User"),
            password = os.getenv("Mysql_pass"),
            database = os.getenv("Mysql_DB"),
            port=int(os.getenv("Mysql_port"))
            )
        self.ides=[1001,1002,1003,1004,1005,1006,1007,1008,1009,1010,1011,1012,1013,1014,1015,1016,1017,1018,1019,1020,1021,1022,1023,1024,1025,1026,1027,1028,1029,1030,1031,1032,1033,1034,1035,1036,1037,1038,1039,1040,1041,1042,1043,1044,1045,1046,1047,1048,1049,1050,1051,1052,1053,1054,1055,1056,1057,1058,1059,1060,1061,1062,1063,1064,1065,1068,1070,1071,1073,1074,1075,1076,1077,1078]
        print("Conexión Exitosa")
    def __qwerysAll(self,consulta):
        self.cursor = self.__connection.cursor()
        try:
            self.sql = consulta
            self.cursor.execute(self.sql)
            self.rows = self.cursor.fetchall()
            self.cursor.close()
            return self.rows
        except Exception as e:
            print(f"error de consulta {e}")
    def IDs(self):##obtiene los id que hay en mysql 
        array=[]
        tupla=self.__qwerysAll("select SiteID from prueba_cinco_minutal group by(SiteID)")
        for x in tupla:
            array.append(x[0])
        return array
    
    def value(self,year,month,day,hour,min,id):
        date=datetime(year=year,month=month,day=day,hour=hour,minute=min,second=0)
        #print(date)
        res=0.0
        try:
            res=self.__qwerysAll(f"select Value_Acum from prueba_cinco_minutal where (TimeIni='{date}' or TimeEnd='{date}')and SiteID={id}")
            res=res[0][0]
        except:
            res=0.0
        return res
__DB=__Mysql()
##############_____________________________________________________________________Metodos_para_Mysql_______________________________________________________##########################
def __cercano_str(min:str):
    cambio=int(min/5)
    cambio=cambio*5
    if cambio==5:
        cambio="05"
    elif cambio==0:
        cambio="00"
    else:
        cambio=str(cambio)
    return cambio
def __cercano(min:int):
    cambio=int(min/5)
    return cambio*5
def __Multiplo(min:int):
    if min%5 ==0:
        multiplo=True
    else:
        multiplo=False
    return multiplo
##############################Formatos##########################################################


def __Manejo_CVS(ruta,valor,hora,nDay=False):
    N_nombre="actual"
    nombre="historico"
    if nDay:
        hoy=(datetime.now()-timedelta(days=1)).strftime("%Y-%m-%d")
        os.rename(f"{ruta}/{N_nombre}.csv", f'{ruta}/{hoy}_{nombre}.csv')

    if not os.path.isfile(f"{ruta}/{N_nombre}.csv"):
        with open(f'{ruta}/{N_nombre}.csv', 'w', newline='') as csvfile:
            spamwriter = csv.writer(csvfile, delimiter=';',
                                    quotechar=';', quoting=csv.QUOTE_MINIMAL)
            spamwriter.writerow(['TimeIni_Human'] + ['Value_Acum'])
            spamwriter.writerow([f'{hora}', f'{valor}'])
    else:        
        with open(f'{ruta}/{N_nombre}.csv', 'a', newline='') as csvfile:
            spamwriter = csv.writer(csvfile, delimiter=';', quotechar=';', quoting=csv.QUOTE_MINIMAL)
            spamwriter.writerow([f'{hora}', f'{valor}'])
def __actualizarDatos(ruta,valor,hora):
    try:
        df= pd.read_csv(f"{ruta}/actual.csv", sep=';')
        df.loc[len(df)-1]=[hora,valor]
        df = df.sort_index().reset_index(drop=True)
        df.to_csv(f"{ruta}/actual.csv", index=False, sep=';')
    except:
        print("error")

def __archivos(ruta,hora,valor,cambio):
    if os.path.exists(ruta)==False:
        os.makedirs(ruta)
    if __Multiplo(int(hora.strftime("%M"))):
        hora=hora.strftime("%Y-%m-%d ") +hora.strftime("%H:")+__cercano_str(int(hora.strftime("%M")))
        __Manejo_CVS(ruta,valor,hora,cambio)
    else:
        hora=hora.strftime("%Y-%m-%d ") +hora.strftime("%H:")+__cercano_str(int(hora.strftime("%M")))
        __actualizarDatos(ruta,valor,hora)


###########################procesos previos
def __ejecutar(ruta):
    today=datetime.now()
    anio=int(today.strftime("%Y"))
    dia=int(today.strftime("%d"))
    mes=int(today.strftime("%m"))
    min=__cercano(int(today.strftime("%M")))
    hora=int(today.strftime("%H"))
    if hora==6 and min==0:
        for x in post.active():
            val=__DB.value(anio,mes,dia,hora,min,x)
            __archivos(f"{ruta}/{x}",today,val,True)
    else:
        for x in post.active():
            val=__DB.value(anio,mes,dia,hora,min,x)
            __archivos(f"{ruta}/{x}",today,val,False)
    bandera=datetime.now()
    diferencia=bandera-today
    return diferencia

##############
def iniciar():
    ruta=os.getcwd()
    ruta=f"{ruta}/datosSQL"
    delta=__ejecutar(ruta)
    return f"myMinuto tarda: {delta}"
#print(__DB.IDs())
