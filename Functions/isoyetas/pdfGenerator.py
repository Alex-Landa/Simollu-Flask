
from fpdf import FPDF
from datetime import datetime
import os
__metodo=["Thiensen","idw","kriging"]
__main=os.getcwd()
def create(types:str,name:str,acronim:str,day=datetime.now(),avg=0.0,esp=""):
# Ruta al archivo PDF original y al nuevo archivo modificado
    route=""
    bodypdf=FPDF(orientation="P", unit="mm",format="A4")
    for x in range(3):####metodos de Isoyetas
        if esp=='' or esp=="":
            img=f"{__main}/Functions/isoyetas/{__metodo[x]}{acronim}.png"
            bodypdf=__Generation(bodypdf,types,str(x+1),img,day,avg)
            route=f"{__main}/Functions/isoyetas/{name}.pdf"
        else:
            today=day.strftime("%Y-%m-%d")
            img=f"{__main}/Functions/isoyetas/{esp}/{today}{__metodo[x]}.png"
            bodypdf=__Generation(bodypdf,types,str(x+1),img,day,avg)
            route=f"{__main}/Functions/isoyetas/historico/{name}.pdf"
    bodypdf.output(route)


def __Generation(pdf:FPDF,types:str,pg,txt:str,day:datetime,pp):
    try:
        hoy=datetime.now()
        today=day.strftime("%Y-%m-%d")
        timestamp=hoy.strftime("%Y-%m-%d %H:%M")
        pdf.add_page() 
        pdf.image(f"{__main}/resources/images/LogoN2.png",10,10,w=90,h=17)

        pdf.set_font('Arial',"b",9)
        pdf.text(95,16,"Sistema de Aguas de la Ciudad de México")
        pdf.text(x=40,y=260,txt="Dr. Rafael Carmona Paredes") ###Director de Sacmex
        pdf.text(x=120,y=260,txt="Ing. Miguel Carmona Suárez") ###Director de Drenaje

        pdf.set_font('Arial',"",9)
        pdf.text(x=95,y=20,txt="Coordinación General")
        pdf.text(x=20,y=38,txt=f"Fecha: {today}")
        pdf.text(x=145,y=38,txt=f"Promedio pesado: {round(pp,3)}")
        pdf.text(x=140,y=290,txt=f"Fecha de impresión: {timestamp}")
        pdf.text(x=60,y=255,txt="Vo.Bo")
        pdf.text(x=135,y=255,txt="Vo.Bo")
        pdf.image(x=5,y=50,name=txt,w=200,h=200)###mapa de iso

        pdf.set_font('Arial',"b",18)
        pdf.text(x=65,y=48,txt=f"INFORME ISOYETAS {types}")

        pdf.set_font('Arial',"i",7)
        pdf.text(x=25,y=264,txt="Coordinador General del Sistema de Aguas de la Ciudad de México") ###Director de Sacmex
        pdf.text(x=126,y=264,txt="Director General de Drenaje") ###Director de Drenaje

        pdf.set_font('Arial',"i",9)
        pdf.text(x=95,y=290,txt=f"Página {pg}") ###
        return pdf
    except Exception as e:
        print(e)
        print()
# create("RADIO","isoyetaRad","Rad")