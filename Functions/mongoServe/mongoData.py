from pymongo import MongoClient
import csv
import os
from datetime import datetime, timezone, timedelta
import pandas as pd
import calendar, time
import asyncio  
#import PostgresData.conexion as post #testeo singular
import Functions.mongoServe.PostgresData.conexion as post #testeo de ini

import requests

#--------------------------------------------------------------------------------conexion a mongo-------------------------------------------------------------------------------------#
class __Mongo:
    def __init__(self):
        self.__conexion=MongoClient(os.getenv("mongo_Conection"))
        self.__db=self.__conexion[os.getenv("mongo_db")]
        self.EstacionesQ = post.getMinutal()
        self.EstacionesN = post.get5Minutal()
        self.__fallos=post.getEstacionesF()
        self.__desconectado=post.getMuertos()    


    def __MessageOut(self,est,hora):
        if len(self.__fallos)>4:
            texto=f"La estación {est} ha dejado de transmitir a las {hora} \n \
                ya son {len(self.__fallos)} estaciones que han caido"
        else:
            texto=f"La estación {est} ha dejado de transmitir a las {hora}"
        #print(texto)
        return texto
    def __MessageIn(self,est):
        texto=f"la estacion {est} vuelve a estar en linea :)"
        print(texto)
        return texto
    def __consulta(self,estacion,Anio,Mes,Dia,hora,min):
        for x in self.__db[estacion].find({"Anio":Anio,"Mes":Mes,"Dia":Dia,"Hora":hora,"Minuto":min}):
            #print(x)
            return x
    def consultaQ(self,estacion,Anio,Mes,Dia,hora,min,delta=5):#se suman los datos de minuto a minuto para 5 por default
        Valor=0.0
        for y in range(min,min + delta):
                try:
                    Aux=self.__consulta(estacion,Anio,Mes,Dia,hora,y)
                    Valor+=Aux.get("Valor")
                    if estacion in self.__desconectado:
                        post.Quitar(estacion)
                        self.__desconectado=post.getMuertos()
                    elif estacion in self.__fallos:
                        post.Quitar(estacion) 
                        self.__fallos=post.getEstacionesF()          
                except:
                    try:
                        caida=datetime(Anio,Mes,Dia,hora,y)
                        if estacion not in self.__desconectado:    
                            if estacion not in self.__fallos:
                                self.__fallos=post.getEstacionesF()
                            if estacion not in self.__fallos:
                                post.RegistrarNuevo(estacion,caida.strftime("%Y-%m-%d %H:%M:%S"))
                                self.__fallos.append(estacion)
                            else:
                                datos=post.getFallos(estacion)
                                if datos[1]< caida: 
                                    missed=datos[0]+1
                                    post.AgregarFallo(estacion,missed,caida.strftime("%Y-%m-%d %H:%M:%S"))    
                                    if missed>=120:
                                        self.__MessageOut(estacion,caida.strftime("%Y-%m-%d %H:%M:%S"))
                                        post.Matar(estacion)
                                        self.__desconectado=post.getMuertos()
                                        self.__fallos=post.getEstacionesF() 
                        else:
                            datos=post.getFallos(estacion)
                            if datos[1]< caida: 
                                post.AgregarFallo(estacion,datos[0]+1,caida.strftime("%Y-%m-%d %H:%M:%S"))
                                self.__fallos=post.getEstacionesF()
                    except:
                        Valor=Valor

        return Valor
    
    def consultaN(self,estacion,Anio,Mes,Dia,hora,min):
        Valor=0.0
        try:
            Aux=self.__consulta(estacion,Anio,Mes,Dia,hora,min)
            Valor=Aux.get("Valor")
            if estacion in self.__desconectado:
                post.Quitar(estacion)
                self.__desconectado=post.getMuertos()
            elif estacion in self.__fallos:
                post.Quitar(estacion)
                self.__fallos=post.getEstacionesF()             
        except:
            try:
                caida=datetime(Anio,Mes,Dia,hora,min)
                if estacion not in self.__desconectado:
                    caida=datetime(Anio,Mes,Dia,hora,min)
                    if estacion not in self.__fallos:
                            self.__fallos=post.getEstacionesF()
                    if estacion not in self.__fallos:
                            post.RegistrarNuevo(estacion,caida.strftime("%Y-%m-%d %H:%M:%S"))
                            self.__fallos.append(estacion)
                    else:
                        datos=post.getFallos(estacion)
                        if datos[1]< caida: 
                            missed=datos[0]+1
                            post.AgregarFallo(estacion,missed,caida.strftime("%Y-%m-%d %H:%M:%S"))    
                            if missed>=24:
                                post.Matar(estacion)
                                self.__MessageOut(estacion,caida.strftime("%Y-%m-%d %H:%M:%S"))
                                self.__desconectado=post.getMuertos()
                                self.__fallos=post.getEstacionesF()       
                        else:
                            datos=post.getFallos(estacion)
                            if datos[1]< caida: 
                                post.AgregarFallo(estacion,datos[0]+1,caida.strftime("%Y-%m-%d %H:%M:%S"))
                                self.__fallos=post.getEstacionesF()
            except:
                Valor=Valor
        return Valor
__DB=__Mongo()
#--------------------------------------------------------------------------------aproximaciones----------------------------------------------------------------------------#
def __cercano(min):
    cambio=int(min/5)
    return cambio*5
def __cercano_str(min):
    cambio=int(min/5)
    cambio=cambio*5
    if cambio==5:
        cambio="05"
    elif cambio==0:
        cambio="00"
    else:
        cambio=str(cambio)
    return cambio
def __Multiplo(min:int):
    if min%5 ==0:
        multiplo=True
    else:
        multiplo=False
    return multiplo
#--------------------------------------------------------------------------------proceso de csv---------------------------------------------------------------------------------------#
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

def __cd_salir(ruta):
    if "\\" in ruta:
        N=ruta.split("\\")
    if "/" in ruta:
        N=ruta.split("/")
    N.pop(len(N)-1)
    N="/".join(N)
    return N
def __cd(ruta, en):
    if "\\" in ruta:
        N=ruta.split("\\")
    if "/" in ruta:
        N=ruta.split("/")
    N.append(en)
    N="/".join(N)
    return N

def __archivos(ruta,hora,valor,cambio):
    if os.path.exists(ruta)==False:
        os.makedirs(ruta)
        __Manejo_CVS(ruta,valor,hora,False)
    if __Multiplo(int(hora.strftime("%M"))):
        hora=hora.strftime("%Y-%m-%d ") +hora.strftime("%H:")+__cercano_str(int(hora.strftime("%M")))
        __Manejo_CVS(ruta,valor,hora,cambio)
    else:
        hora=hora.strftime("%Y-%m-%d ") +hora.strftime("%H:")+__cercano_str(int(hora.strftime("%M")))
        __actualizarDatos(ruta,valor,hora)
#--------------------------------------------------------------------------------Telegram msg-----------------------------------------------------------------------------------------#

def __send_telegram_message(self, message,TOKEN = "1143340693:AAEXAyPwkPOA62NE8Q-6sMtmcxMsqCoeAL4",CHAT_ID = "-1001735046614"):
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage?chat_id={CHAT_ID}&text={message}"
    print(requests.get(url).json())  # this sends the message
#----------------------------------------------------------------------------------Tiempos-----------------------------------------------------------------------------------------------#

def __ejecutar(ruta):
    local=datetime.now()#hora de mexico
    utc=datetime.now(timezone.utc)#hora de la zona 0
    cambio=False
    min_utc=int(utc.strftime("%M"))
    hora_local=int(local.strftime("%H"))
        #margen para que los pluviometros manden datos
    min_utc=__cercano(min_utc)
    min=__cercano(int(local.strftime("%M")))

    if hora_local == 6 and min == 0:
        cambio=True
    min=__cercano_str(int(local.strftime("%M")))
    ultima=datetime.now()
    for x in __DB.EstacionesN:
        val=__DB.consultaN(__DB.EstacionesN.get(x),int(utc.strftime("%Y")),int(utc.strftime("%m")),int(utc.strftime("%d")),int(utc.strftime("%H")),min_utc)
        __archivos(f"{ruta}/{x}",local,val,cambio)
    for x in __DB.EstacionesQ:
        val2=__DB.consultaQ(__DB.EstacionesQ.get(x),int(utc.strftime("%Y")),int(utc.strftime("%m")),int(utc.strftime("%d")),int(utc.strftime("%H")),min_utc)
        __archivos(f"{ruta}/{x}",local,val2,cambio)
    bandera=datetime.now()-local
    return bandera
#----------------------------------------------------------------------------------MAIN-----------------------------------------------------------------------------------------------#
def iniciar():
    ruta=os.getcwd()
    ruta=f"{ruta}/datos"
    delta=__ejecutar(ruta)
    return f"mon tarda: {delta}"

#----------------------------------------------------------------------------------DEV--------------------------------------------------------------------------------        
#pruebasC(mes=8,dia= 29 ,anio=2023,hora=2,min=0)
#ahora.strftime("%Y-%m-%d %H:%M:%S") =timestamp