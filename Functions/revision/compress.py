import zipfile
import os
from datetime import datetime , timedelta

__mes={1:"Enero",2:"Febrero",3:"Marzo",4:"Abril",5:"Mayo",6:"Junio",7:"Julio",8:"Agosto",9:"Septiembre",10:"Octubre",11:"Noviembre",12:"Diciembre"}

def __zipper(ruta,mesActual,today):
    stations=os.listdir(ruta)
    for st in stations:
            nRuta=ruta+"/"+st
            csv=os.listdir(nRuta)
            if "actual.csv" in csv:
                csv.remove("actual.csv")
            nameZip=__mes.get(mesActual)+"-"+today.strftime("%Y")
            if os.path.exists(f"{nRuta}/historical/{nameZip}.zip"):
                os.remove(f"{nRuta}/historical/{nameZip}.zip")
            ziper=zipfile.ZipFile(f"{nRuta}/historical/{nameZip}.zip","w")    
            for y in csv:
                mes=y.split("-")
                try:    
                    if mesActual==int(mes[1]):
                        ziper.write(f"{nRuta}/{y}",compress_type = zipfile.ZIP_DEFLATED)
                        if today.strftime("%d")=="1":
                            if (int(mes[0])<30 and int(mes[1])!=2)or(int(mes[0])<28 and int(mes[1])==2):
                                os.remove(f"{nRuta}/{y}")
                        else:
                            os.remove(f"{nRuta}/{y}")
                except:
                    pass
            ziper.close()

def __zipperAll(ruta,mesActual,Anio):
    stations=os.listdir(ruta)
    for st in stations:
        zipear=1
        nRuta=ruta+"/"+st
        ###meter un while
        if not os.path.exists(nRuta+"/historical"):
            os.makedirs(nRuta+"/historical")
        while zipear!=0:
            zipear=0
            csv=os.listdir(nRuta)
            if "actual.csv" in csv:
                csv.remove("actual.csv")
                csv.remove("historical")
            supresor=csv[len(csv)-1].split("_")
            supresor=supresor[0].split("-")
            if mesActual == supresor[1] and Anio == supresor[0]:
                csv.pop(len(csv)-1)
                supresor=csv[len(csv)-1].split("_")
                supresor=supresor[0].split("-")
            nameZip=__mes.get(int(supresor[1]))+"-"+supresor[0]
            if os.path.exists(f"{nRuta}/historical/{nameZip}.zip"):
                    os.remove(f"{nRuta}/historical/{nameZip}.zip")
            ziper=zipfile.ZipFile(f"{nRuta}/historical/{nameZip}.zip","w") 
            for y in csv:
                    recon=y.split("_")
                    recon=recon[0].split("-")
                    try:    
                        if mesActual != recon[1] or Anio != recon[0]:
                            if supresor[1]== recon[1] and supresor[0]==recon[0]:
                                ziper.write(f"{nRuta}/{y}",compress_type = zipfile.ZIP_DEFLATED)
                                os.remove(f"{nRuta}/{y}")
                            else:
                                zipear+=1
                        else:
                            continue
                    except Exception as e:
                        print(e)
                        pass  
def Compress(meses:int=0,seteo="datos", Force=False):
    try:
        print("entra")
        today=datetime.now()
        if int(today.strftime("%d"))==1 or int(today.strftime("%d"))==2 or Force:
            ruta = os.getcwd()
            ruta=ruta+"/"+seteo
            if meses<=0 and meses>12:
                Anio=int(today.strftime("%Y"))
                mesActual=int(today.strftime("%m"))-1
                if mesActual==0:
                    mesActual=12
                    Anio-=1
                __zipper(ruta,mesActual,today)
            elif meses==-1:
                __zipperAll(ruta,today.strftime("%m"),today.strftime("%Y"))
            else:
                mesActual=meses
                Anio=int(today.strftime("%Y"))
                __zipper(ruta,mesActual,today)
        Compress(meses=meses,seteo="datosSQL",Force=Force)            
    except Exception as e:
        if os.path.exists(f"{os.getcwd()}/req_logs.txt"):
                errors=open(f"{os.getcwd()}/req_logs.txt","a")
                txt=datetime.now().strftime("%d-%m-%Y %H:%M")
                errors.write(f"{txt}: {e}, compress; \n")
                errors.close()
        else:
                errors=open(f"{os.getcwd()}/req_logs.txt","w")
                txt=datetime.now().strftime("%d-%m-%Y %H:%M")
                errors.write(f"{txt}: {e}, compress;\n")
                print("creating...")
                errors.close()
# Compress(meses=11,Force=True)
#C:\Users\sacmex6704\Documents\sevicio\simollu-flask\datos\1001\01-01-2023_historico.csv