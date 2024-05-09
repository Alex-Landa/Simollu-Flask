import pymysql
from datetime import datetime,timedelta,timezone
import os
import csv
import pandas as pd
#import Conexiones.conexionPost as post
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
        #print("Conexión Exitosa")
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
        #print(month)
        date=datetime(year=year,month=month,day=day,hour=hour,minute=min,second=0)
        #print(date)
        res=0.0
        try:
            #print(f"select Value_Acum from prueba_cinco_minutal where (TimeIni='{date}' or TimeEnd='{date}')and SiteID={id}")
            #print("-----------------------------------------------------------------------------------------------")
            res=self.__qwerysAll(f"select Value_Acum from prueba_cinco_minutal where (TimeIni='{date}' or TimeEnd='{date}')and SiteID={id}")
            res=res[0][0]
        except:
            print(f"ErrorSQL {id}, {date}")
            res=0.0
        #print(res)
        #print("-----------------------------------------------------------------------------------------------")
        return res
    def value_Range(self,Y,M,D,ID):
        Hour=[]
        Val=[]
        for x in self.__qwerysAll(f"SELECT Value_Acum,TimeIni FROM `prueba_cinco_minutal` where Year={Y} and `Month`={M} and `Day`={D} and SiteID={ID}"):
            Hour.append(x[1])
            Val.append(x[0])
        tupla = {Hour:Val for (Hour,Val) in zip(Hour,Val)}
        return tupla
__DB=__Mysql()

#--------------------------------------------------------------------------------proceso de csv---------------------------------------------------------------------------------------#
def __cercano_str(min:int):
    cambio=int(min/5)
    cambio=cambio*5
    if cambio==5:
        cambio="05"
    elif cambio==0:
        cambio="00"
    else:
        cambio=str(cambio)
    return cambio
def __correctionHour(hour:datetime):
    minute=__cercano_str(int(hour.strftime("%H")))
    delay=hour.strftime("%Y-%m-%d %H:")+minute
    delay=datetime.strptime(delay,"%Y-%m-%d %H:%M")-timedelta(hours=1)
    return delay
def __consultaLon():
    dia=datetime.now()
    hora_base = dia.strftime("%Y-%m-%d")+' 6:00'
    anterior=datetime.strptime(hora_base,"%Y-%m-%d %H:%M")-timedelta(days=1)
    hora_base = dia.strftime("%Y-%m-%d")+' 5:55'
    teorico =datetime.strptime(hora_base,"%Y-%m-%d %H:%M")-anterior
    return int(teorico/timedelta(minutes=5))
    
def __calculoTotal(dia:datetime):
    if int(dia.strftime("%H"))<6:
        dia=dia-timedelta(days=1)
    hora_base = dia.strftime("%Y-%m-%d")+' 6:00'
    teorico =datetime.now()-datetime.strptime(hora_base,"%Y-%m-%d %H:%M")
    teorico=teorico/timedelta(minutes=5)  
    return int(teorico)
###---------------------------------------------------------------------------------insercion----------------------------------------
def __correcionDatos(df:pd,index,corte,estacion):
    utc=corte
    valor=__DB.value(id=estacion,year=int(utc.strftime("%Y")),month=int(utc.strftime("%m")),day=int(utc.strftime("%d")),hour=int(utc.strftime("%H")),min=int(utc.strftime("%M")))
    df.loc[index]=[corte.strftime("%Y-%m-%d %H:%M"),valor]
    df = df.sort_index().reset_index(drop=True)
    return df

def __Reescribir(ruta,N_nombre,valor,hora,planchar=False):
    if os.path.exists(ruta)==False:
        os.makedirs(ruta)
    if os.path.exists(f"{ruta}/{N_nombre}.csv")==False:
        print("crea actual")
        with open(f'{ruta}/{N_nombre}.csv', 'w', newline='') as csvfile:
                spamwriter = csv.writer(csvfile, delimiter=';',
                                        quotechar=';', quoting=csv.QUOTE_MINIMAL)
                spamwriter.writerow(['TimeIni_Human'] + ['Value_Acum'])
                spamwriter.writerow([f'{hora}', f'{round(valor,2)}'])
    if planchar:
        with open(f'{ruta}/{N_nombre}.csv', 'w', newline='') as csvfile:
            spamwriter = csv.writer(csvfile, delimiter=';',
                                    quotechar=';', quoting=csv.QUOTE_MINIMAL)
            spamwriter.writerow(['TimeIni_Human'] + ['Value_Acum'])
            spamwriter.writerow([f'{hora}', f'{round(valor,2)}'])
    else:        
        with open(f'{ruta}/{N_nombre}.csv', 'a', newline='') as csvfile:
            spamwriter = csv.writer(csvfile, delimiter=';', quotechar=';', quoting=csv.QUOTE_MINIMAL)
            spamwriter.writerow([f'{hora}', f'{round(valor,2)}'])


###---------------------------------------------------------------------------------insercion----------------------------------------
#-------------------------------------------------------------------------------limpieza de csv---------------------------------------------------------------------------   
def __csvDepuracion(ruta,id,nombre,memoria=any):
    revision=False
    df = pd.read_csv(f"{ruta}/datosSQL/{id}/{nombre}.csv", sep=';')
    for pos in range(len(df)):#solo si 
            if memoria == df.loc[pos, 'TimeIni_Human']:
                df.drop(pos, axis=0, inplace=True)
                df.to_csv(f"{ruta}/datosSQL/{id}/{nombre}.csv", index=False, sep=';')
                revision=True
                break
            else:
                memoria=df.loc[pos, 'TimeIni_Human']
            # print(df.loc[pos, 'TimeIni_Human'])
            # print(df)        
    if revision:
        __csvDepuracion(ruta,id,nombre,memoria)
 #---------------------------------------fin limpieza de csv---------------------------------------------------------------------------  

def __planchaCSVGeneral(ruta,id,corte:datetime,nombre,cant):
    ruta=f"{ruta}/datosSQL/{id}"
    utc=corte
    min_utc=int(utc.strftime("%M"))
    valor=__DB.value(id=id,year=int(utc.strftime("%Y")),month=int(utc.strftime("%m")),day=int(utc.strftime("%d")),hour=int(utc.strftime("%H")),min=min_utc)
    __Reescribir(ruta,nombre,valor,corte.strftime("%Y-%m-%d %H:%M"),True)
    corte=corte+timedelta(minutes=5)
    utc=utc+timedelta(minutes=5)
    truers= __DB.value_Range(int(utc.strftime("%Y")),int(utc.strftime("%m")),int(utc.strftime("%d")),ID=id)
    for x in range(cant):
        if int(utc.strftime("%H"))==0 and int(utc.strftime("%M"))==0 :
            truers= __DB.value_Range(int(utc.strftime("%Y")),int(utc.strftime("%m")),int(utc.strftime("%d")),ID=id)
        if utc in truers:
            valor=truers.get(utc)
            __Reescribir(ruta,nombre,valor,corte.strftime("%Y-%m-%d %H:%M"))
        else:
            __Reescribir(ruta,nombre,0.0,corte.strftime("%Y-%m-%d %H:%M"))
        corte=corte+timedelta(minutes=5)
        utc=utc+timedelta(minutes=5)  
    print("\t----------------------------------------------------------\n")  
    
#----------------------------------------------------------------------------fin proceso de csv---------------------------------------------------------------------------------------#
#--------------------------------------------------------------revisiones---------------------------------------------------------#
def __general(corte,hist=True):
    ruta=os.getcwd()
    anterior=datetime.strftime(corte,"%Y-%m-%d")
    if hist:
        nombre=f"{anterior}_historico"
    else:
        nombre="actual"
    valor=__consultaLon()
    for x in post.active():
        __planchaCSVGeneral(ruta,x,__dia(corte),nombre,valor)

def __dia(actual:datetime):
    if int(actual.strftime("%H"))>=6:
        truco=actual.strftime("%Y-%m-%d")+' 6:00'
        truco=datetime.strptime(truco,"%Y-%m-%d %H:%M")
    else:
        truco=actual.strftime("%Y-%m-%d")+' 6:00'
        truco=datetime.strptime(truco,"%Y-%m-%d %H:%M")-timedelta(days=1)
        #print(f"truco de: {truco}")
    return truco
def __gen(ruta:str,N_nombre:str):
    if os.path.exists(ruta)==False:
        os.makedirs(ruta)
        with open(f'{ruta}/{N_nombre}.csv', 'w', newline='') as csvfile:
                spamwriter = csv.writer(csvfile, delimiter=';',
                                        quotechar=';', quoting=csv.QUOTE_MINIMAL)
                spamwriter.writerow(['TimeIni_Human'] + ['Value_Acum'])
                spamwriter.writerow(["6:00", 0.0])
def __comprobar(actual:datetime,rev):
    ruta=os.getcwd()
    lista=os.listdir(ruta+"/datosSQL")
    estimado=__calculoTotal(actual)
    #print(lista)
    for x in lista:
        #print(f"{x in lista } N" )
        if x in lista:
            df = pd.read_csv(f"{ruta}/datosSQL/{x}/actual.csv", sep=';')
            #print(".------------------------------------------")
            if len(df)!=estimado:
                    df.to_csv(f"{ruta}/datosSQL/{x}/actual.csv", index=False, sep=';')
                    __planchaCSVGeneral(ruta,x,__dia(actual),"actual",estimado)
            else:
                truco=__correctionHour(actual)
                if(len(df)>=rev):
                    for y in range(len(df)-rev,len(df)):
                        df=__correcionDatos(df,y,truco,x)
                        truco=truco+timedelta(minutes=5)
                else:
                    for y in range(0,len(df)):
                        df=__correcionDatos(df,y,truco,x)
                        truco=truco+timedelta(minutes=5)
                df.to_csv(f"{ruta}/datosSQL/{x}/actual.csv", index=False, sep=';')


#--------------------------------------------------------------fin revisiones---------------------------------------------------------#
def checar(iterBack=12):
    local=datetime.now()#hora de mexico
    hora_local=int(local.strftime("%H"))
    min_local=int(local.strftime("%M"))
    corte=datetime.now(timezone.utc)-timedelta(days=1,hours=hora_local,minutes=min_local)
    corte=datetime.strptime(corte.strftime("%Y-%m-%d %H:%M"),"%Y-%m-%d %H:%M")
    if hora_local==8 :#planchar todo el dia anterior a las 8
    #if True:
        __general(corte)
    else:
        __comprobar(local,iterBack)
    delta=datetime.now()-local
    print(f"revision 'my' tarda: {delta}")

def redo(station:int=-1):
    ruta=os.getcwd()
    local=datetime.now()#hora de mexico
    if station==-1:
        for x in post.active():
            __planchaCSVGeneral(ruta,x,__dia(local),"actual",__calculoTotal(local))
    else:
         __planchaCSVGeneral(ruta,station,__dia(local),"actual",__calculoTotal(local))
    delta=datetime.now()-local
    print(f"revision 'my' tarda: {delta}")

def redo_Historical(dias=1):
    actual=datetime.now()
    redo()
    for x in range(dias,0,-1):
        hora_local=int(datetime.now().strftime("%H"))
        min_local=int(datetime.now().strftime("%M"))
        corte=datetime.now(timezone.utc)-timedelta(days=x,hours=hora_local,minutes=min_local)
        corte=datetime.strptime(corte.strftime("%Y-%m-%d %H:%M"),"%Y-%m-%d %H:%M")
        print("#-------------------------------------#")
        __general(corte)
        print(f"rehecha la fecha de {corte}")
    delta=datetime.now()-actual
    print(f"tarda {delta}")
# def temp():
#     ruta=os.getcwd()
#     for x in post.active():
#         try:
#             os.remove(f"{ruta}/datosSQL/{x}/30-10-2023_historico.csv")
#         except:
#             continue
#--------------------------------------------------------------pruebas---------------------------------------------------------#
#checar()
#redo()
#redo_Historical(365)
#print(corte)
#print(datetime(year=2023,month=9,day=16,hour=19,minute=20)  in __DB.value_Range(2023,9,16,1001))
