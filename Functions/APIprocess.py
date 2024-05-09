import pandas as pd
import os
import time
from datetime import datetime
import Functions.mongoServe.PostgresData.conexion as post 
import Functions.mongoServe.verificacion as reMon
import Functions.myServe.verificacion as reMy
# import mongoServe.PostgresData.conexion as post 
def __datos(rango,df):
    if rango==15:
        rango=3
    elif rango==30:
        rango=6
    elif rango==1:
        rango=12
    else:
        rango=len(df)
    return rango

def __colores(valores):
    cantidad=0.0
    if len(valores)>0:
        for x in valores:
            cantidad=cantidad+float(x)
    if cantidad==0.0:
        color="#fafafa"#blanco
    elif cantidad>0.0 and cantidad<5.0:
        color="#397757"#verde
    elif cantidad>=5.0 and cantidad<15.0:
        color="#fac400"#amarillo
    elif cantidad>=15.0 and cantidad<30.0:
        color="#ff8800"#naranja 
    elif cantidad>=30.0 and cantidad<50.0:
        color="#db1414 "#rojo
    else:
        color="#a3009e"#morado
    return {"color":color}

def __total(valores,txt:str="total"):
    cantidad=0.0
    if len(valores)>0:
        for x in valores:
            cantidad=cantidad+float(x)
        
    return {txt:round(cantidad,2)}
######################################################################Funciones Basicas##################################################################
def __lectorBloques(ruta,rango,nombre="actual"):
    df = pd.read_csv(f"{ruta}/{nombre}.csv", sep=';')
    lista=[{"TimeIni_Human":[]}]
    ext=[]
    last=[]
    rango=__datos(rango,df)
    for x in range(len(df)-rango,len(df),1):
        if float(df.loc[x, 'Value_Acum'])!=0.0:
            lista[0]["TimeIni_Human"].append(df.loc[x,'TimeIni_Human'])
            ext.append(df.loc[x,'Value_Acum'])
            if x>=(len(df)-3):#medida temporal a 15 min
                last.append(df.loc[x,'Value_Acum'])
    lista.append(__colores(ext))
    lista.append(__total(ext))
    lista.append(__total(last,"last"))
    return lista

def __leectorUnico(ruta,rango,nombre:str):
    df = pd.read_csv(f"{ruta}/{nombre}.csv", sep=';')
    lista=[{"TimeIni_Human":[]},{"Value_Acum":[]}]
    ext=[]
    rango=__datos(rango,df)
    for x in range(len(df)-rango,len(df),1):
        if float(df.loc[x, 'Value_Acum'])!=0.0:
            lista[0]["TimeIni_Human"].append(df.loc[x,'TimeIni_Human'])
            lista[1]["Value_Acum"].append(df.loc[x,'Value_Acum'])
            if x>=(len(df)-3):#medida temporal a 15 min
                ext.append(df.loc[x,'Value_Acum'])
    color=__colores(lista[1]["Value_Acum"])
    total=__total(lista[1]["Value_Acum"])
    lista.append(color)
    lista.append(total)
    lista.append(__total(ext,"last"))
    return lista

def precipitacionRadio(id,rango):
    ruta=os.getcwd()
    ruta=f"{ruta}/datosSQL/{id}"
    return __leectorUnico(ruta,rango,"actual")

def precipitacion(id,rango):
    ruta=os.getcwd()
    ruta=f"{ruta}/datos/{id}"
    return __leectorUnico(ruta,rango,"actual")


def allBlocks(rango=6,__rep=False):
    temp:any
    try:
        ruta=os.getcwd()
        ruta=f"{ruta}/datos"
        ides=os.listdir(ruta)
        general=[]
        est=post.APIGP()
        for x in ides:
            temp=int(x)
            if temp in est:
                general.append({"station_code":est.get(temp)[0],"name":est.get(temp)[1],
                                "values":__lectorBloques(f"{ruta}/{x}",rango,"actual"),})
    except Exception as e:
        time.sleep(0.5)
        if __rep==False:
            print(e)
            try:
                print(temp)
                reMon.redo(temp)
            except:
                errors=open(f"{os.getcwd()}/req_logs.txt","a")
                txt=datetime.now().strftime("%d-%m-%Y %H:%M")
                errors.write(f"{txt}: {e}, blockGPRS; \n")
                time.sleep(0.1)
            general=precipitacionAll(rango,True)
        else:
            if os.path.exists(f"{os.getcwd()}/req_logs.txt"):
                errors=open(f"{os.getcwd()}/req_logs.txt","a")
                txt=datetime.now().strftime("%d-%m-%Y %H:%M")
                errors.write(f"{txt}: {e}, GPRS-{x}; \n")
                print("exist")
                errors.close()
            else:
                errors=open(f"{os.getcwd()}/req_logs.txt","w")
                txt=datetime.now().strftime("%d-%m-%Y %H:%M")
                print("creating...")
                errors.write(f"{txt}: {e}, GPRS; \n")
                errors.close()
    return general




def precipitacionAll(rango,day:str,__rep=False):
    temp:any
    try:
        ruta=os.getcwd()
        ruta=f"{ruta}/datos/"
        ides=os.listdir(ruta)
        general=[]
        est=post.APIGP()
        for x in ides:
            temp=int(x)
            if temp in est:
                general.append({"station_code":est.get(temp)[0],"name":est.get(temp)[1],"address_label":est.get(temp)[2],"latitude":est.get(temp)[3],"longitude":est.get(temp)[4],
                                "values":__leectorUnico(f"{ruta}/{x}",rango,day),})
    except Exception as e:
        time.sleep(0.5)
        if __rep==False:
            print(e)
            try:
                print(temp)
                reMon.redo(temp)
            except:
                errors=open(f"{os.getcwd()}/req_logs.txt","a")
                txt=datetime.now().strftime("%d-%m-%Y %H:%M")
                errors.write(f"{txt}: {e}, presGPRS; \n")
                time.sleep(0.1)
            general=precipitacionAll(rango,day,True)
        else:
            if os.path.exists(f"{os.getcwd()}/req_logs.txt"):
                errors=open(f"{os.getcwd()}/req_logs.txt","a")
                txt=datetime.now().strftime("%d-%m-%Y %H:%M")
                errors.write(f"{txt}: {e}, GPRS-{x}; \n")
                print("exist")
                errors.close()
            else:
                errors=open(f"{os.getcwd()}/req_logs.txt","w")
                txt=datetime.now().strftime("%d-%m-%Y %H:%M")
                print("creating...")
                errors.write(f"{txt}: {e}, GPRS; \n")
                errors.close()
    return general
def presAllRad(rango,day:str,__rep=False):
    temp:any
    try:
        ruta=os.getcwd()
        ruta=f"{ruta}/datosSQL/"
        ides=os.listdir(ruta)
        general=[]
        est=post.APIRAD()
        for x in ides:
            temp=int(x)
            if temp in est:
                general.append({"station_code":est.get(temp)[0],"name":est.get(temp)[1],"address_label":est.get(temp)[2],"latitude":est.get(temp)[3],"longitude":est.get(temp)[4],
                                "values":__leectorUnico(f"{ruta}/{x}",rango,day),})
    except Exception as e:
        time.sleep(1)
        if __rep==False:
            print(e)
            try:
                print(temp)
                reMy.redo(temp)
            except:
                errors=open(f"{os.getcwd()}/req_logs.txt","a")
                txt=datetime.now().strftime("%d-%m-%Y %H:%M")
                errors.write(f"{txt}: {e}, presRadio; \n")
                time.sleep(0.1)
            general=presAllRad(rango,day,True)
        else:
            print(e)
            if os.path.exists(f"{os.getcwd()}/req_logs.txt"):
                errors=open(f"{os.getcwd()}/req_logs.txt","a")
                txt=datetime.now().strftime("%d-%m-%Y %H:%M")
                errors.write(f"{txt}: {e}, Radio; \n")
                errors.close()
            else:
                errors=open(f"{os.getcwd()}/req_logs.txt","w")
                txt=datetime.now().strftime("%d-%m-%Y %H:%M")
                errors.write(f"{txt}: {e}, Radio;\n")
                print("creating...")
                errors.close()
    return general
########################################################################################################################################################
########################################################################ISOYETAS########################################################################
