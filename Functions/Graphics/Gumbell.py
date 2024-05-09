import numpy as np
from scipy.stats import gumbel_r
import Functions.Graphics.Precipitation as press
def __verify(valores:list):
    cantidad=0
    for x in valores:
        cantidad=cantidad+float(x)
    return cantidad
def __acumgraf(valores:list):
    cantidad=0
    lista=[0]
    for x in valores:
        cantidad=cantidad+float(x)
        lista.append(cantidad)
    return lista
def __gumbelGen(rain:list,date):
# Parámetros de la distribución de Gumbel
    datos_lluvia = rain
    mu, beta = gumbel_r.fit(datos_lluvia)   
# Generar datos para la gráfica
    x = np.linspace(gumbel_r.ppf(0.001, mu, beta), gumbel_r.ppf(0.999, mu, beta), len(rain))
    pdf = gumbel_r.pdf(x, mu, beta) #### Graficar la función de densidad de probabilidad (pdf)
    cdf = gumbel_r.cdf(x, mu, beta) #### Graficar la función de distribución acumulativa (CDF)
    return {"PDF":pdf.tolist(),"CDF":cdf.tolist(),"Acum":__acumgraf(rain),"start":date,"graf":True}

def Generator(id:int,ini:str,fin:str,tipo:str):
    resp=press.Historic(id=id,ini=ini,fin=fin,tipo=tipo)
    dateStart=resp.get("date")
    values=resp.get("value")
    if(__verify(values)==0):
        print("caso vacio")
        return {"PDF":[],"CDF":[],"Acum":[],"graf":False}
    else:
        print("caso con cosas")
        return __gumbelGen(values,dateStart[len(dateStart)-1])
