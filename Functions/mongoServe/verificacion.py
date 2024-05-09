from pymongo import MongoClient
import os
from datetime import datetime, timezone, timedelta
import pandas as pd
import csv
#import PostgresData.conexion as post #testeo singular
import Functions.mongoServe.PostgresData.conexion as post #testeo de ini
class __Mongo:
    def __init__(self):
        self.__conexion=MongoClient(
            os.getenv('mongo_Conection'))
        self.__db=self.__conexion[os.getenv('mongo_db')]
        self.EstacionesQ = post.getMinutal()
        self.EstacionesN = post.get5Minutal()
        self.fallos=post.getEstacionesF()
        self.desconectado=post.getMuertos()   
    def __consulta(self,estacion,Anio,Mes,Dia,hora,min):
        for x in self.__db[estacion].find({"Anio":Anio,"Mes":Mes,"Dia":Dia,"Hora":hora,"Minuto":min}):
            #print(x)
            return x
    def consultaOpt(self,estacion:str,Anio:int,Mes:int,Dia:int,HoraI:int,HoraF:int):
        query={"Anio":Anio,"Mes":Mes,"Dia":Dia,"Hora":{"$gte":HoraI,"$lte":HoraF},"Valor":{"$gt":0}}
        Hour=[]
        Val=[]
        #print(self.__db[estacion].find(query),end="\n \n" )
        for x in self.__db[estacion].find(query):
            #print(x)
            Hour.append(datetime(year=x.get("Anio"),month=x.get("Mes"),day=x.get("Dia"),hour=x.get("Hora"),minute=x.get("Minuto")))
            Val.append(x.get("Valor"))
        tupla = {Hour:Val for (Hour,Val) in zip(Hour,Val)}
        return tupla
    def consQopt(self,estacion:str,matrix:dict,date:datetime):
        valor=0.0
        for y in range( 5):
            ndate=date+timedelta(minutes=y)
            if ndate in matrix:
                valor=valor+matrix.get(ndate)
            else:
                valor=valor+0.0
        #print(valor)
        return valor
    def consultaQ(self,estacion,Anio,Mes,Dia,hora,min,delta=5):#se suman los datos de minuto a minuto para 5 por default
        Valor=0.0
        for y in range(min,min + delta):
                try:
                    Aux=self.__consulta(estacion,Anio,Mes,Dia,hora,y)
                    Valor+=Aux.get("Valor")
                    if estacion in self.desconectado:
                        self.desconectado.remove(estacion)
                        post.Quitar(estacion)
                    elif estacion in self.fallos:
                        self.fallos.remove(estacion)
                        post.Quitar(estacion)           
                except:
                    try:
                        caida=datetime(Anio,Mes,Dia,hora,y)
                        print(f"Error en la consulta Mon {estacion}, {caida}")
                        if estacion not in self.desconectado:    
                            #print(caida)
                            if estacion not in self.fallos:
                                self.fallos=post.getEstacionesF()
                                if estacion not in self.fallos:
                                     post.RegistrarNuevo(estacion,caida.strftime("%Y-%m-%d %H:%M:%S"))
                                     self.fallos.append(estacion)
                            else:
                                datos=post.getFallos(estacion)
                                #print(datos[1]< caida)
                                if datos[1]< caida: 
                                    fallos=datos[0]+1
                                    post.AgregarFallo(estacion,fallos,caida.strftime("%Y-%m-%d %H:%M:%S"))    
                                    if fallos>=120:
                                        #self.__MessageOut(estacion,caida.strftime("%Y-%m-%d %H:%M:%S"))
                                        self.desconectado.append(estacion)
                                        self.fallos.remove(estacion)
                                        post.Matar(estacion)
                        else:
                            datos=post.getFallos(estacion)
                            if datos[1]< caida: 
                                post.AgregarFallo(estacion,datos[0]+1,caida.strftime("%Y-%m-%d %H:%M:%S"))
                    except:
                        Valor=Valor

        #print(Valor)
        return Valor
    
    def consultaN(self,estacion,Anio,Mes,Dia,hora,min):
        Valor=0.0
        try:
            Aux=self.__consulta(estacion,Anio,Mes,Dia,hora,min)
            Valor=Aux.get("Valor")
            if estacion in self.desconectado:
                self.desconectado.remove(estacion)
                post.Quitar(estacion)
            elif estacion in self.fallos:   
                self.fallos.remove(estacion)
                post.Quitar(estacion)           
        except:
            try:
                caida=datetime(Anio,Mes,Dia,hora,min)
                print(f"Error en la consulta Mon {estacion}, {caida}")
                if estacion not in self.desconectado:
                    caida=datetime(Anio,Mes,Dia,hora,min)
                    if estacion not in self.fallos:
                        self.fallos=post.getEstacionesF()
                    if estacion not in self.fallos:
                        post.RegistrarNuevo(estacion,caida.strftime("%Y-%m-%d %H:%M:%S"))
                        self.fallos.append(estacion)
                    else:
                        datos=post.getFallos(estacion)
                        if datos[1]< caida: 
                            fallos=datos[0]+1
                            post.AgregarFallo(estacion,fallos,caida.strftime("%Y-%m-%d %H:%M:%S"))    
                            if fallos>=24:
                                post.Matar(estacion)
                                #self.__MessageOut(estacion,caida.strftime("%Y-%m-%d %H:%M:%S"))
                                self.desconectado.append(estacion)
                                self.fallos.remove(estacion)
                else:
                    datos=post.getFallos(estacion)
                    if datos[1]< caida: 
                        post.AgregarFallo(estacion,datos[0]+1,caida.strftime("%Y-%m-%d %H:%M:%S"))
            except:
                Valor=Valor
        #print(Valor)
        return Valor
__DB=__Mongo()


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
def __correcionDatos(df:pd,index,corte,tipo,estacion):
    utc=corte+timedelta(hours=6)
    if tipo=="Q":
        valor=__DB.consultaQ(estacion,int(utc.strftime("%Y")),int(utc.strftime("%m")),int(utc.strftime("%d")),int(utc.strftime("%H")),int(utc.strftime("%M")))
    if tipo=="N":
        valor=__DB.consultaQ(estacion,int(utc.strftime("%Y")),int(utc.strftime("%m")),int(utc.strftime("%d")),int(utc.strftime("%H")),int(utc.strftime("%M")))
    df.loc[index]=[corte.strftime("%Y-%m-%d %H:%M"),valor]
    df = df.sort_index().reset_index(drop=True)
    return df

def __Reescribir(ruta,N_nombre,valor,hora,planchar=False):
    if planchar:
        with open(f'{ruta}/{N_nombre}.csv', 'w', newline='') as csvfile:
            spamwriter = csv.writer(csvfile, delimiter=';',
                                    quotechar=';', quoting=csv.QUOTE_MINIMAL)
            spamwriter.writerow(['TimeIni_Human'] + ['Value_Acum'])
            spamwriter.writerow([f'{hora}', f'{valor}'])
    else:        
        with open(f'{ruta}/{N_nombre}.csv', 'a', newline='') as csvfile:
            spamwriter = csv.writer(csvfile, delimiter=';', quotechar=';', quoting=csv.QUOTE_MINIMAL)
            spamwriter.writerow([f'{hora}', f'{valor}'])


###---------------------------------------------------------------------------------insercion----------------------------------------
#-------------------------------------------------------------------------------limpieza de csv---------------------------------------------------------------------------   
def __csvDepuracion(ruta,id,nombre,memoria=any):
    revision=False
    df = pd.read_csv(f"{ruta}/datos/{id}/{nombre}.csv", sep=';')
    for pos in range(len(df)):#solo si 
            if memoria == df.loc[pos, 'TimeIni_Human']:
                df.drop(pos, axis=0, inplace=True)
                df.to_csv(f"{ruta}/datos/{id}/{nombre}.csv", index=False, sep=';')
                revision=True
                break
            else:
                memoria=df.loc[pos, 'TimeIni_Human']
            # print(df.loc[pos, 'TimeIni_Human'])
            # print(df)        
    if revision:
        __csvDepuracion(ruta,id,nombre,memoria)
 #---------------------------------------fin limpieza de csv---------------------------------------------------------------------------  

def __planchaCSVGeneral(ruta,id,estacion,corte:datetime,nombre,tipo,cant):
    ruta=f"{ruta}/datos/{id}"
    utc=corte+timedelta(hours=6)
    min_utc=int(utc.strftime("%M"))
    if tipo=="N":#optimizado
        valor=__DB.consultaN(estacion,int(utc.strftime("%Y")),int(utc.strftime("%m")),int(utc.strftime("%d")),int(utc.strftime("%H")),min_utc)
        __Reescribir(ruta,nombre,valor,corte.strftime("%Y-%m-%d %H:%M"),True)
        corte=corte+timedelta(minutes=5)
        utc=utc+timedelta(minutes=5)
        truers=__DB.consultaOpt(estacion,int(utc.strftime("%Y")),int(utc.strftime("%m")),int(utc.strftime("%d")),12,23)
        for x in range(cant):
            if int(utc.strftime("%H"))==0 and int(utc.strftime("%M"))==0 :
                truers= __DB.consultaOpt(estacion,int(utc.strftime("%Y")),int(utc.strftime("%m")),int(utc.strftime("%d")),0,12)
            if utc in truers:
                valor=truers.get(utc)
                __Reescribir(ruta,nombre,valor,corte.strftime("%Y-%m-%d %H:%M"))
            else:
                __Reescribir(ruta,nombre,0.0,corte.strftime("%Y-%m-%d %H:%M"))
            #valor=__DB.consultaN(estacion,int(utc.strftime("%Y")),int(utc.strftime("%m")),int(utc.strftime("%d")),int(utc.strftime("%H")),int(utc.strftime("%M")))
            corte=corte+timedelta(minutes=5)
            utc=utc+timedelta(minutes=5)
    elif tipo=="Q":#por optimizar
        valor=__DB.consultaQ(estacion,int(utc.strftime("%Y")),int(utc.strftime("%m")),int(utc.strftime("%d")),int(utc.strftime("%H")),min_utc)
        __Reescribir(ruta,nombre,valor,corte.strftime("%Y-%m-%d %H:%M"),True)
        corte=corte+timedelta(minutes=5)
        utc=utc+timedelta(minutes=5)
        truers=__DB.consultaOpt(estacion,int(utc.strftime("%Y")),int(utc.strftime("%m")),int(utc.strftime("%d")),12,17)
        for x in range(cant):
            min_utc=int(utc.strftime("%M"))
            hor_utc=int(utc.strftime("%H"))
            if hor_utc==18 and min_utc==0 :
                truers= __DB.consultaOpt(estacion,int(utc.strftime("%Y")),int(utc.strftime("%m")),int(utc.strftime("%d")),18,23)
            elif hor_utc==0 and min_utc==0 :
                truers= __DB.consultaOpt(estacion,int(utc.strftime("%Y")),int(utc.strftime("%m")),int(utc.strftime("%d")),0,5)
            elif hor_utc==6 and min_utc==0 :
                truers= __DB.consultaOpt(estacion,int(utc.strftime("%Y")),int(utc.strftime("%m")),int(utc.strftime("%d")),6,12)
            #print(f"{x} -> {utc}")
            #print(truers, end="\n \n")
            valor=__DB.consQopt(estacion,truers,utc)
            __Reescribir(ruta,nombre,valor,corte.strftime("%Y-%m-%d %H:%M"))
            corte=corte+timedelta(minutes=5)
            utc=utc+timedelta(minutes=5)
            
    print("\t ------------------------------------------------------\n")    
    
#----------------------------------------------------------------------------fin proceso de csv---------------------------------------------------------------------------------------#
#--------------------------------------------------------------revisiones---------------------------------------------------------#
def __general(corte):
    ruta=os.getcwd()
    anterior=datetime.strftime(corte,"%Y-%m-%d")
    nombre=f"{anterior}_historico"
    valor=__consultaLon()
    for x in __DB.EstacionesN:
        __planchaCSVGeneral(ruta,x,__DB.EstacionesN.get(x),corte,nombre,"N",valor)
    for x in __DB.EstacionesQ:
        __planchaCSVGeneral(ruta,x,__DB.EstacionesQ.get(x),corte,nombre,"Q",valor)

def __dia(actual:datetime):
    if int(actual.strftime("%H"))>=6:
        truco=actual.strftime("%Y-%m-%d")+' 6:00'
        truco=datetime.strptime(truco,"%Y-%m-%d %H:%M")
    else:
        truco=actual.strftime("%Y-%m-%d")+' 6:00'
        truco=datetime.strptime(truco,"%Y-%m-%d %H:%M")-timedelta(days=1)
    return truco

def __comprobar(actual:datetime,rev):
    ruta=os.getcwd()
    lista=os.listdir(ruta+"/datos")
    estimado=__calculoTotal(actual)
    print(estimado+" estimado")
    for x in lista:
        id_est=int(x)
        #print(f"{id_est in __DB.EstacionesN.keys()} N" )
        
        df = pd.read_csv(f"{ruta}/datos/{x}/actual.csv", sep=';')
        if id_est in __DB.EstacionesN.keys():
            #print("N")
            tipo="N"
        elif id_est in __DB.EstacionesQ.keys():
            tipo="Q"
            #print("Q")
        else:
            tipo="NA"
        if len(df)!=estimado and tipo!="NA":#si los datos no cuadran
                df.to_csv(f"{ruta}/datos/{x}/actual.csv", index=False, sep=';')
                if tipo=="Q":
                    __planchaCSVGeneral(ruta,x,__DB.EstacionesQ.get(id_est),__dia(actual),"actual",tipo,estimado)
                if tipo=="N":
                    __planchaCSVGeneral(ruta,x,__DB.EstacionesN.get(id_est),__dia(actual),"actual",tipo,estimado)
        elif tipo!="NA":
            truco=__correctionHour(actual)
            if(len(df)>=rev):
                for y in range(len(df)-rev,len(df)):
                    if tipo=="Q":
                        df=__correcionDatos(df,y,truco,tipo,__DB.EstacionesQ.get(id_est))
                    if tipo=="N":
                        df=__correcionDatos(df,y,truco,tipo,__DB.EstacionesN.get(id_est))
                    truco=truco+timedelta(minutes=5)
            else:
                for y in range(0,len(df)):
                    if tipo=="Q":
                        df=__correcionDatos(df,y,truco,tipo,__DB.EstacionesQ.get(id_est))
                    if tipo=="N":
                        df=__correcionDatos(df,y,truco,tipo,__DB.EstacionesN.get(id_est))
                    truco=truco+timedelta(minutes=5)
            df.to_csv(f"{ruta}/datos/{x}/actual.csv", index=False, sep=';')
        else:
            print(f"{x} no se ha encontrado")


#--------------------------------------------------------------fin revisiones---------------------------------------------------------#
def checar(iterBack=12):
    local=datetime.now()#hora de mexico
    hora_local=int(local.strftime("%H"))
    min_local=int(local.strftime("%M"))
    corte=datetime.now(timezone.utc)-timedelta(days=1,hours=hora_local,minutes=min_local)
    corte=datetime.strptime(corte.strftime("%Y-%m-%d %H:%M"),"%Y-%m-%d %H:%M")###no borrar
    if hora_local==6 :#planchar todo el dia anterior a las 8
    #if True:###preguntar a laura
        __general(corte)
    else:
        __comprobar(local,rev=iterBack)
    delta=datetime.now()-local
    print(f"verif GP tarda: {delta} (del print)")
def __gen(ruta:str,N_nombre:str):
    if os.path.exists(ruta)==False:
        os.makedirs(ruta)
    with open(f'{ruta}/{N_nombre}.csv', 'w', newline='') as csvfile:
            spamwriter = csv.writer(csvfile, delimiter=';',
                                    quotechar=';', quoting=csv.QUOTE_MINIMAL)
            spamwriter.writerow(['TimeIni_Human'] + ['Value_Acum'])
def redo(station:int=-1):
    local=datetime.now()
    actual=datetime.now()
    ruta=os.getcwd()
    if station==-1:
        lista=os.listdir(ruta+"/datos")
    else:
        lista=[station]
    estimado=__calculoTotal(actual)
    for x in lista:
        id_est=int(x)
        if(x==1065):
            print("jajas")
        if not os.path.exists(f"{ruta}/datos/{x}/actual.csv"):
            __gen(f"{ruta}/datos/{x}","actual")
        if id_est in __DB.EstacionesN.keys():
            tipo="N"
            __planchaCSVGeneral(ruta,x,__DB.EstacionesN.get(id_est),__dia(actual),"actual",tipo,estimado)
        elif id_est in __DB.EstacionesQ.keys():
            tipo="Q"
            __planchaCSVGeneral(ruta,x,__DB.EstacionesQ.get(id_est),__dia(actual),"actual",tipo,estimado)
        else:
            tipo="NA"
        print("#----------------------------------------------------------------------#")
        print(f"Estacion: {x}")
    delta=datetime.now()-local
    print(f"verif GP tarda: {delta} (del print)")

def redoHist(dias=1):
    actual=datetime.now()
    redo()
    #estimado=__calculoTotal(actual)
    for x in range(dias,0,-1):
        hora_local=int(datetime.now().strftime("%H"))
        min_local=int(datetime.now().strftime("%M"))
        corte=datetime.now(timezone.utc)-timedelta(days=x,hours=hora_local,minutes=min_local)
        corte=datetime.strptime(corte.strftime("%Y-%m-%d %H:%M"),"%Y-%m-%d %H:%M")###no borrar
        print("#-------------------------------------#")
        __general(corte)  
    delta=datetime.now()-actual
    print(f"tarda {delta}")
#--------------------------------------------------------------pruebas---------------------------------------------------------#
#checar()
#redo()
#print(corte)
#print(datetime.now())
# tupla={datetime(year=2023,month=9,day=15,hour=11,minute=55):0.25}
#redoHist(1)
# aux=datetime(year=2023,month=9,day=15,hour=11,minute=55,second=10)
# if aux in tupla:
#     print("jajas")
# __planchaCSVGeneral("si","si","RIOHON085Q_1001",aux,"si","Q",80)
#__DB.consultaOpt("RIOHON085Q_1001",2023,9,15,23,23)
