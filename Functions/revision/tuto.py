def main():
    head="<h1> Pagina para explicaciones </h1> \n"
    lista="<ul> Rango de datos: \
    <li> 6 = todos los registros</li>\
    <li> 1 = los registros en 1 hora</li>\
    <li> 30 = los registros en 30 min</li>\
    <li> 10 = los registros en 10 min</li>\
    </ul> \n"
    points="<ul> end-points: GET \
    <li> /station/*id de la estacion*/*rango de datos*</li>\
    <li> /station/all/*rango de datos*</li>\
    <li> /station/redo/all</li>\
    <li> -----------------------------------</li>\
    <li> /stationRad/*id de la estacion*/*rango de datos*</li>\
    <li> /stationRad/all/*rango de datos*</li>\
    <li> /stationRad/redo/all</li>\
    </ul> \n"
    historic="<ul> end-points de historicos: POST \
    <li> /hystorical/GPRS</li>\
    <li> /hystorical/RADIO</li>\
    </ul> \n  \
       <h4> Ambos llevan un body tipo json, ejemplo: \n {\n'days':n\n}\n donde 'n' son los dias que quieres rehacer\n</h4> "

    return head +lista+points+historic