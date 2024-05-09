from datetime import datetime,timedelta,date
import pandas as pd
import os
import math
import Functions.Graphics.UnZipper as unZip
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
def __daysDiff(cant:int):
    delta=cant/288
    delta=math.ceil(delta)
    return delta

def __calculo(ini:datetime,fin:datetime):
    dif=fin-ini
    cant=dif/timedelta(minutes=5)
    cant=math.ceil(cant)
    return cant
def __premiere(archivos:list,cont:int,Fin:datetime,ruta:str,cant:int):#se lee del fin al inicio
    Aux=Fin.strftime("%Y-%m-%d")
    tempV=[]
    tempT=[]
    if Aux==str(date.today()):
        df=pd.read_csv(f"{ruta}/actual.csv", sep=';')
        cont=len(archivos)-1
    else:
        df=pd.read_csv(f"{ruta}/{Aux}_historico.csv", sep=';')
        for x in archivos:
            if x!= f"{Aux}_historico.csv":
                cont+=1
            else:
                break
    Aux=Fin.strftime("%Y-%m-%d %H:")+__cercano_str(int(Fin.strftime("%M")))
    activer=False
    for x in range(len(df)-1,-1,-1):
        if not activer and Aux ==df.loc[x,"TimeIni_Human"]:
            activer=True
        if activer and cant>0:
            tempT.append(df.loc[x,"TimeIni_Human"])
            tempV.append(df.loc[x,"Value_Acum"])
            cant-=1
    cont-=1
    return tempT,tempV,cont,cant

def __RadProcess(cant:int,id:int,End:datetime,Start:datetime):
    ruta=os.getcwd()
    ruta=ruta+f"/datosSQL/{id}"
    archivos=os.listdir(ruta)
    vals=[]
    dia=[]
    aux3=[]
    aux4=[]
    try:
        archivos.remove("historical")
    except:
        None
    necesarios=__daysDiff(cant)
    print(necesarios)
    # if len(archivos)<necesarios or (End.strftime("%m")!=str(date.today().month))or(Start.strftime("%m")!=str(date.today().month)):
    #     aux3,aux4,cant=unZip.router(ruta,cant,Start)
    if cant !=0: 
        trick=End
        trick=trick-timedelta(days=1)
        cont=0
        aux,aux2,cont,cant=__premiere(archivos,cont,End,ruta,cant)
        while cant>0:
            hist=trick.strftime("%Y-%m-%d")
            df=pd.read_csv(f"{ruta}/{hist}_historico.csv", sep=';')
            for x in range(len(df)-1,-1,-1):
                cant-=1
                if cant<=0:
                    break
                try:
                    dia.append(df.loc[x,"TimeIni_Human"])
                    vals.append(df.loc[x,"Value_Acum"])
                except Exception as e:
                    print(e)
                    continue
            cont-=1
            trick=trick-timedelta(days=1)
            if cant<=0 or cont<0:
                break
        vals=aux2+vals+aux4
        dia=aux+dia+aux3
        return dia,vals

def __GPprocess(cant:int,id:int,End:datetime,Start:datetime):
    ruta=os.getcwd()
    ruta=ruta+f"/datos/{id}"
    archivos=os.listdir(ruta)
    vals=[]
    dia=[]
    aux3=[]
    aux4=[]
    try:
        archivos.remove("historical")
    except:
        None
    necesarios=__daysDiff(cant)
    print(necesarios)
    # if len(archivos)<necesarios or (End.strftime("%m")!=str(date.today().month))or(Start.strftime("%m")!=str(date.today().month)):
    #     aux3,aux4,cant=unZip.router(ruta,cant,Start)
    if cant !=0: 
        trick=End
        trick=trick-timedelta(days=1)
        cont=0
        aux,aux2,cont,cant=__premiere(archivos,cont,End,ruta,cant)
        while cant>0:
            hist=trick.strftime("%Y-%m-%d")
            df=pd.read_csv(f"{ruta}/{hist}_historico.csv", sep=';')
            for x in range(len(df)-1,-1,-1):
                cant-=1
                if cant<=0:
                    break
                try:
                    dia.append(df.loc[x,"TimeIni_Human"])
                    vals.append(df.loc[x,"Value_Acum"])
                except Exception as e:
                    print(e)
                    continue
            cont-=1
            trick=trick-timedelta(days=1)
            if cant<=0 or cont<0:
                break
        vals=aux2+vals+aux4
        dia=aux+dia+aux3
        return dia,vals



def Historic(id:int,ini:str,fin:str,tipo:str):
    init=datetime.strptime(ini,"%Y-%m-%d %H:%M")
    fint=datetime.strptime(fin,"%Y-%m-%d %H:%M")
    req=__calculo(init,fint)
    if req < 0:
        req=req*-1
        truco=init
        init=fint
        fint=truco
    if tipo=="RADIO":
        date,pts=__RadProcess(req,id)
    elif tipo=="GPRS":
        date,pts=__GPprocess(req,id,fint,init)
    else:
        date=[]
        pts=[]
    return {"date":date,"value":pts}
# __GPprocess(1,1001)
# test=[2,3,1]
# test.append(5)
# # test.reverse()
# print(test)
# test=datetime.now()-timedelta(days=30,minutes=30)
# print(Historic(1001,test,datetime.now(),"GPRS"))