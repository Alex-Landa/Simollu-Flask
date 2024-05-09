import zipfile
import os
from datetime import datetime,date
__mes={1:"Enero",2:"Febrero",3:"Marzo",4:"Abril",5:"Mayo",6:"Junio",7:"Julio",8:"Agosto",9:"Septiembre",10:"Octubre",11:"Noviembre",12:"Diciembre"}
def __verify(route):
    today=date.today()
    iszip=f"{route}/historical/{__mes.get(today.month)}-{today.year}.zip"
    if os.path.exists(iszip):
        os.remove(iszip)
def __magia(unZip:str,Route:str,date:datetime,cant:int):
    time=[]
    value=[]
    locker=date.strftime("%Y-%m-%d")
    vrai=False
    with zipfile.ZipFile(unZip, mode="r") as archive:
        print(archive.namelist())
        for filename in archive.namelist():
            print(filename)
            aux=filename.split("/")
            aux=aux[len(aux)-1]
            
            if not vrai and aux == f"{locker}_historico.csv":
                vrai=True
            if vrai:
                x=archive.read(filename).split(b"\n")
                x.pop(0)
                x.pop()
                for y in x:
                    temp=y.split(b";")
                    time.append(str(temp[0],'utf-8'))
                    value.append(str(temp[1].strip(b"\r"),'utf-8'))
                    cant-=1
                    if cant<=0:
                        vrai=False
                        break
    return time,value,cant
                
#dateStart:datetime,dateEnd:datetime
def router(route:str,cant:int,dateIni:datetime):
    __verify(route)
    time=[]
    Vals=[]
    tp1=[]
    tp2=[]
    month=int(dateIni.strftime("%m"))
    while cant>0:
        year=dateIni.strftime("%Y")
        unzip=f"{route}/historical/{__mes.get(month)}-{year}.zip"
        if os.path.exists(unzip):
            tp1,tp2,cant= __magia(unzip,route,dateIni,cant)
            time=time+tp1
            Vals=Vals+tp2
        else:
            break
        month+=1
        if month>12:
            month=1
    time.reverse()
    Vals.reverse()
    return time,Vals,cant
# print(ruta)
#print(router(f"{ruta}/datos/1098",9,2023))