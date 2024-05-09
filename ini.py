from flask import Flask, jsonify,send_file
from markupsafe import escape
from flask import url_for as body
from flask import request #para metodos https
from flask_cors import CORS
import threading
import os
#-------------------------------------------------------------------------------------------------------
import schedule
import time
import asyncio
import concurrent.futures
from datetime import datetime
#-------------------------------------------------------------------------------------------------------
import Functions.mongoServe.isoGprsCsv as isoGPRS
import Functions.mongoServe.isoRadCsv as isoRad
import Functions.mongoServe.verificacion as verificar
import Functions.mongoServe.mongoData as genDatos
import Functions.APIprocess as getDatos
import Functions.myServe.sqlData as genMyDatos
import Functions.myServe.verificacion as myVerify
import Functions.revision.tuto as tutorial
import Functions.revision.compress as Zipper
import Functions.Graphics.Precipitation as press
import Functions.Graphics.Gumbell as Gumbel
import Functions.isoyetas.pdfGenerator as pdf
import Functions.mongoServe.isoHistCsv as hist
import Functions.promedioPesado as pp
# import Functions.myServe.Conexiones.mySql as sql
###############################R######################
import Functions.isoyetas.rExecution as R
######################################################
#########################SWAGGER######################
from flask_swagger_ui import get_swaggerui_blueprint


#-------------------------------------------------------------------------------------------------------
# import Functions.isoyetas.rExecution as R
__ruta=os.getcwd()
# Allow=os.getenv("AllowIP")
app = Flask(__name__)

####Si se quieren restringir las ip, cambiar el asterisco por la variable comentada allow
CORS(app,resources={"/*":{"origins":"*"}},methods=["GET","POST"])
app.secret_key=os.getenv("secret_Key")
#events=asyncio.get_event_loop()
executorMin=concurrent.futures.ThreadPoolExecutor(max_workers=2)
executorHor=concurrent.futures.ThreadPoolExecutor(max_workers=2)
####################################################################SWAGGER############################################################
SWAGGER_URL="/swagger"
API_URL=f"/static/swagger.json"
swagger_ui_blueprint = get_swaggerui_blueprint(
    SWAGGER_URL,
    API_URL,
    config={
        'app_name': 'Access API'
    }
)
app.register_blueprint(swagger_ui_blueprint, url_prefix=SWAGGER_URL)
####################################################################SWAGGER############################################################
###----------------------------------------------------------------------inicio de rutas------------------------------------------------------------------------------------###
# @app.before_request
# def check_ip():
#     client_ip = request.remote_addr
#     if client_ip not in Allow:
#         print(client_ip)
#         return jsonify({"error": "Acceso denegado"}), 404
##________________________________________________________________________________________________________________________
@app.route("/monitor/gprs",methods=['GET'])
def monGP():
    try:
        return getDatos.allBlocks()
    except:
        return None
####################################################Promedio
@app.route("/AVG/gprs",methods=['GET'])
def avgGP():
    try:
        return {"average":pp.avgGprs()}
    except:
        return None   
@app.route("/AVG/Radio",methods=['GET'])
def avgRad():
    try:
        return {"average":pp.avgRadio()}
    except:
        return None  
######__________________________________________________Monitoreo__________________________________________________________________

@app.route("/station/<int:id>/<int:rango>",methods=['GET'])
def getData(id,rango):
    return getDatos.precipitacion(id,rango)


@app.route("/stationRad/<int:id>/<int:rango>",methods=['GET'])
def getDataRad(id,rango):
    return getDatos.precipitacionRadio(id,rango)

#######################################################MAPAS##########################################
@app.route("/station/all/<int:rango>",methods=['GET'])
async def getDataAll(rango):
    #await asyncio.sleep(0.2) 
    try:
        return getDatos.precipitacionAll(rango,"actual")
    except:
        return None

@app.route("/stationRad/all/<int:rango>",methods=['GET'])
async def getDataRadAll(rango):
    try:
        await asyncio.sleep(0.15) 
        return getDatos.presAllRad(rango,"actual")
    except:
        return None
#######################################################MAPAS##########################################
# @app.route("/test",methods=["post"])
# def test():
#     req=request.get_json()
#     res={"res":sql.executer(req['test'])}
#     return res
#######################################################GETTERS HISTORICOS##########################################
@app.route("/station/historical/<fecha>",methods=['GET'])    
def gethistoricalMap(fecha):
    # try:
        day=datetime.strptime(fecha,"%Y-%m-%d")
        day=day.strftime("%Y-%m-%d")
        print(day)
        return getDatos.precipitacionAll(6,f"{day}_historico")
    # except:
    #     return jsonify({"error": "El archivo no existe"}), 404
    
@app.route("/stationRad/historical/<fecha>",methods=['GET'])    
def gethistoricalRadMap(fecha):
    try:
        day=datetime.strptime(fecha,"%Y-%m-%d")
        day=day.strftime("%Y-%m-%d")
        return getDatos.presAllRad(6,f"{day}_historico")
    except:
        return jsonify({"error": "El archivo no existe"}), 404
    
@app.route("/station/pdf/historical",methods=['POST'])    
def gethistoricalPdf():
        req=request.get_json()
    # try:
        day=datetime.strptime(req['day'],"%Y-%m-%d")
        day=day.strftime("%Y-%m-%d")
        #return {"si":pp.avgGprs(f"{day}_historico")}
        return send_file(hist.Generation("GPRS",day,"GP",pp.avgGprs(f"{day}_historico")),as_attachment=False, download_name='isoyetaGP.pdf') 
    # except:
    #     return jsonify({"error": "El archivo no existe"}), 404

@app.route("/stationRad/pdf/historical",methods=['POST'])    
def gethistoricalRadPdf():
        req=request.get_json()
    # try:
        day=datetime.strptime(req['day'],"%Y-%m-%d")
        day=day.strftime("%Y-%m-%d")
        #return {"si":pp.avgRadio(f"{day}_historico")}
        return send_file(hist.Generation("RADIO",day,"Rad",pp.avgRadio(f"{day}_historico")),as_attachment=False, download_name='isoyetaRad.pdf') 
    # except:
    #     return jsonify({"error": "El archivo no existe"}), 404
#######################################################GETTERS HISTORICOS##########################################

@app.route("/station/redo/<range>",methods=['POST'])
async def debuggear(range):
    try:
        temp=int(range)
    except:
        temp=-1
    try:
        loop=asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_in_executor(executorHor,verificar.redo(temp))
        await asyncio.sleep(20)
        loop.close()     
        return "rehechos los actuales :)"
    except:
        return "ya se están rehaciendo >:("

@app.route("/stationRad/redo/all",methods=['POST'])
async def debuggearRad():
    try:
        loop=asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_in_executor(executorHor, myVerify.redo())
        await asyncio.sleep(20)
        loop.close()
        return "rehechos los actuales :)"
    except:
        return "ya se están rehaciendo >:("
##--------------------------------------------------------------------------historicos---------------------------------------------------------------------------------###
@app.route("/hystorical/GPRS",methods=['POST'])####se hacen los CSV de los dias indicados
async def gpHist():
    try:
        req=request.get_json()
        loop=asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_in_executor(executorHor, verificar.redoHist(req["days"]))
        await asyncio.sleep(5)
        loop.close()
        return f"se han rehecho {req['days']} dias"
    except:
        pass
@app.route("/hystorical/RADIO",methods=['POST'])####se hacen los CSV de los dias indicados
async def rdHist():
    try:
        req=request.get_json()
        loop=asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_in_executor(executorHor, myVerify.redo_Historical(req["days"]))
        await asyncio.sleep(5)
        loop.close()
        return f"se han rehecho {req['days']} dias"
    except:
        pass

 #########################################Help########################################################
@app.route("/guide",methods=['GET'])
def tuto():
    return "simoflask"
 #########################################Help########################################################
###############################################################################ISOTEST#######################################################################
@app.route("/pdf/GPRS",methods=['GET'])
def PDFGPRS():
        pdf=f"{__ruta}/Functions/isoyetas/isoyetaGP.pdf"
        return send_file(pdf,as_attachment=False, download_name='isoyetaGP.pdf')


@app.route("/pdf/RADIO",methods=['GET'])
def PDFRADIO():
    try:
        pdf=f"{__ruta}/Functions/isoyetas/isoyetaRad.pdf"  
        return send_file(pdf,as_attachment=True, download_name='isoyetaRad.pdf')
    except:
        return jsonify({"error": "El archivo no existe"}), 404
###############################################################################ISOTEST#######################################################################
###----------------------------------------------------------------------Graficos------------------------------------------------------------------------------------###
@app.route("/graphics/pres/RADIO",methods=['POST'])####grafica de precipitación:con body->
def pressRadio():
    try:
        req=request.get_json()
        return press.Historic(id=req['StationCode'],ini=str(req['Ini']),fin=str(req['End']),tipo="RADIO")
    except Exception as e:
        print(e)
        return jsonify({"error": "Falta de datos"}), 404
@app.route("/graphics/pres/GPRS",methods=['POST'])####grafica de precipitación:con body->
def pressGPRS():
    try:
        req=request.get_json()
        return press.Historic(id=req['StationCode'],ini=str(req['Ini']),fin=str(req['End']),tipo="GPRS")
    except Exception as e:
        print(e)
        return jsonify({"error": "Falta de datos"}), 404
@app.route("/graphics/gumbell/GPRS",methods=['POST'])####grafica de precipitación:con body->
def GumbellGPRS():
    try:
        req=request.get_json()
        return Gumbel.Generator(id=req['StationCode'],ini=str(req['Ini']),fin=str(req['End']),tipo="GPRS")
    except Exception as e:
        print(e)
        return jsonify({"error": "Falta de datos"}), 404

@app.route("/graphics/gumbell/RADIO",methods=['POST'])####grafica de precipitación:con body->
def GumbellRadio():
    try:
        req=request.get_json()
        return Gumbel.Generator(id=req['StationCode'],ini=str(req['Ini']),fin=str(req['End']),tipo="RADIO")
    except Exception as e:
        print(e)
        return jsonify({"error": "Falta de datos"}), 404
###----------------------------------------------------------------------Graficos------------------------------------------------------------------------------------###
###----------------------------------------------------------------------fin de rutas------------------------------------------------------------------------------------###

###----------------------------------------------------------------------inicio de cronos------------------------------------------------------------------------------------###
def cosas():
    print(genDatos.iniciar())
  
def cosasSql():
    print(genMyDatos.iniciar())

def revision():
    print(verificar.redo())

def revisionSQL():
    print(myVerify.redo())
def csvIsoG():
    isoGPRS.Generation()
def csvIsoR():
    isoRad.csvGen()
def compress():
    Zipper.Compress()
def generatePDF():
    pdf.create("GPRS","isoyetaGP","GP",pp.avgGprs())
    pdf.create("RADIO","isoyetaRad","Rad",pp.avgRadio())
def minutero():
    loop=asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    print("#---------------------------------------------------------#")
    loop.run_in_executor(executorMin,cosas)
    loop.run_in_executor(executorMin,cosasSql)
    loop.run_in_executor(executorMin,csvIsoG)
    loop.run_in_executor(executorMin,csvIsoR)
    loop.run_in_executor(executorMin,generatePDF)
    try:
        loop.run_in_executor(executorMin,R.exec)
    except:
        None
    print("#---------------------------------------------------------#")
    loop.close()
def horas():
    try:
        loop=asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        print("#---------------------------------------------------------#")
        loop.run_in_executor(executorHor,revision)
        loop.run_in_executor(executorHor,revisionSQL)
        print("#---------------------------------------------------------#")
        loop.run_in_executor()
        loop.close()
    except Exception as e:
        print(e)
        

schedule.every(60).seconds.do(minutero)
schedule.every(181).seconds.do(horas)
#schedule.every(1).day.do(compress)

def ejecutar_tareas_programadas():
    while cosas:
        schedule.run_pending()
        time.sleep(1)
# Iniciar el hilo para ejecutar tareas programadas en segundo plano
task_thread = threading.Thread(target=ejecutar_tareas_programadas)
task_thread.start()
def main():
    app.run(host='0.0.0.0',port=5001,debug=False)
        
if __name__ == '__main__':
    main()
#Demo_tester.2023#$
################################################################################################
##NOTA IMPORTANTE
## Si se ejecuta usando python, las conecciones pueden fallar ya que el os.getenv no llega
######################################SWAGGER###################################################    