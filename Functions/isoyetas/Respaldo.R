## Borrar todo lo que haya en memoria:
rm(list = ls())
## Se cargan las siguientes librerias:
library(sp)
library(rgdal)
library(gstat)
library(maptools)
library(surveillance)
EXECUTION_TIME_LIMIT <- 300
##Se ajusta el nivel del raster, aumenta el tiempo de procesamiento al volverse mas preciso (mas nitido o pixeleado por la separacion entre puntos)
precision <- 0.002
##Margen que se le da al plano a partir de las estaciones que se encuentran en los limites territoriales
#margin <- 0.005
margin <- 0.15
margin_th<-0.05
## Niveles de precipitacion:
#levels <- c(5,10,15,25,35,45,55)
levels <- seq(from = 5, to = 300, by = 5)
levels_fix <- seq(from = 5, to = 300, by = 5)
## Declaramos directorio de Trabajo
#setwd("C:/Users/sacmex5121/Documents/Isohyets/")
#setwd("E:/RESP/Sistemas/simollu/SRC/scripts/2021-06-14/Radio")
#setwd("/opt/Isoyetas/Radio")
setwd("C:/Users/sacmex6704/Documents/sevicio/simollu-flask/Functions/isoyetas")


cdmx <- readOGR(".", "alcaldias")
sp.cdmx <- list("sp.polygons", cdmx,col="#c9c9c9",first=F, cex=.5)

"#### Funcion de conversion de nivel de precipitacion a color
http://www.zonums.com/gmaps/kml_color/ ###########"
lev2color <- function(level, opacity="", reverse=FALSE, fix=FALSE) {
  color<-""
  
  if (level >=55) {
    color <- c("ec","6b", "fa")
  } 
  if (level < 55 && level >=50) {
    
    color <- c("ec","6b", "fa")
  } else
    if (level < 50 && level >=45) {
      color <- c("fa","6b", "6b")
    } else 
      if (level < 45 && level >=40) {
        color <- c("fa","6b", "6b")
      } else
        if (level < 40 && level >= 35) {
          color <- c("fa","6b", "6b")
        } else
          if (level < 35 & level >= 30) {
            
            color <- c("fa","6b", "6b")
          } else
            if (level < 30 & level >= 25) {
              color <- c("ff","b3", "80")
            } else
              if (level < 25 & level >= 20) {
                color <- c("ff","b3", "80")
                
              } else
                if (level < 20 & level >= 15) {
                  color <- c("ff","b3", "80")
                  
                } else
                  if (level < 15 & level >= 10) {
                    color <- c("ff","ff", "80")
                    
                  } else
                    if (level < 10 & level > 5) {
                      color <- c("ff","ff", "80")
                    } else
                      if (level <= 5 & level > 0) {
                        color <- c("ca","ff", "c2")
                      } else
                        if (level <= 0) {
                          color <- c("ff","ff", "ff")
                        }
  if(fix)
  {
    
    if (level <= 5 & level > 0) {
      color <- c("f2","ff", "f0")
    } else
      if (level <= 0) {
        color <- c("ff","ff", "ff")
      }
  }
  if(reverse){
    color<-paste(opacity,color[3],color[2],color[1],sep="")
  }else
  {
    color<-paste(color[1],color[2],color[3], opacity,sep="")
  }
  
  return (color)
}

"#### Funcion de conversion de las lineas de contorno a archivo con formato KML utilizando lineas 
https://developers.google.com/kml/documentation/kmlreference ###########"
kmlLine2 <- function(obj = NULL, kmlfile = NULL, name = "R Line", 
                     description = "", col = NULL, visibility = 1, lwd = 3, 
                     kmlname = "", kmldescription = "", labels=NULL) {
  if (is.null(obj)) 
    return(list(header = c("<?xml version=\"1.0\" encoding=\"UTF-8\"?>", 
                           "<kml xmlns=\"http://earth.google.com/kml/2.2\">", 
                           "<Document>", paste("<name>", kmlname, "</name>", 
                                               sep = ""), paste("<description><![CDATA[", 
                                                                kmldescription, "]]></description>", sep = "")), 
                footer = c("</Document>", "</kml>")))
  
  if (class(obj) != "Lines" && class(obj) != "SpatialLinesDataFrame") 
    stop("obj must be of class 'Lines' or 'SpatialLinesDataFrame' [package 'sp']")
  
  if (class(obj) == "SpatialLinesDataFrame") {
    if (length(obj@lines) > 1L) 
      warning(paste("Only the first Lines object with the ID '", 
                    obj@lines[[1]]@ID, "' is taken from 'obj'", 
                    sep = ""))
    obj <- obj@lines[[1]]
  }
  
  kml <- kmlStyle <- ""
  
  #Comprobamos si la longitud del slot de Lineas es mayor a 0, en caso contrario devuelve vacio para que Google maps arroje el error de documento invalido
  if(length(obj@Lines)>0){
    kmlHeader <- c("<?xml version=\"1.0\" encoding=\"UTF-8\"?>", 
                   "<kml xmlns=\"http://earth.google.com/kml/2.2\">", 
                   "<Document>", paste("<name>", kmlname, "</name>", 
                                       sep = ""), paste("<description><![CDATA[", kmldescription, 
                                                        "]]></description>", sep = ""))
  }
  else
  {
    kmlHeader<-c("",sep="")
  }
  
  kmlFooter <- c("</Document>", "</kml>")
  
  
  if (length(obj@Lines) > 0){
    for (i in length(obj@Lines):1) {
      
      kmlStyle <- append(kmlStyle, paste("<Style id=\"", obj@ID, i,"\">", sep = ""))
      kmlStyle <- append(kmlStyle, "<LineStyle>")
      kmlStyle <- append(kmlStyle, paste("<width>", lwd, "</width>", 
                                         sep = ""))
      kmlStyle <- append(kmlStyle, paste("<color>", lev2color(labels[i], "af", TRUE),"</color>", sep = ""))
      kmlStyle <- append(kmlStyle, "</LineStyle>")
      kmlStyle <- append(kmlStyle, "</Style>")
      
      kml <- append(kml, "<Placemark>")
      kml <- append(kml, paste("<name>",name, "</name>", sep = ""))
      kml <- append(kml, paste("<description><![CDATA[", labels[i]," mm", 
                               "]]></description>", sep = ""))
      
      kml <- append(kml, paste("<styleUrl>#", obj@ID,i, "</styleUrl>", 
                               sep = ""))
      
      kml <- append(kml, paste("<visibility>", as.integer(visibility), 
                               "</visibility>", sep = ""))
      ##    kml <- append(kml, "<MultiGeometry>")
      
      
      kml <- append(kml, "<LineString>")
      kml <- append(kml, "<tessellate>1</tessellate>")
      kml <- append(kml, "<coordinates>")
      
      kml <- append(kml, paste(coordinates(obj@Lines[[i]])[, 
                                                           1], coordinates(obj@Lines[[i]])[, 2], sep = ","))
      
      kml <- append(kml, "</coordinates>")
      kml <- append(kml, "</LineString>")
      
      ##    kml <- append(kml, "</MultiGeometry>")
      kml <- append(kml, "</Placemark>")
    }
  }
  if (!is.null(kmlfile)) 
    cat(paste(c(kmlHeader, kmlStyle, kml, kmlFooter), 
              sep = "", collapse = "\n"), "\n", file = kmlfile, sep = "")
  else list(style = kmlStyle, content = kml)
}

#### Funcion de conversion de las lineas de contorno a archivo con formato KML utilizando poligonos/areas ###########
kmlPolygon2 <- function(obj = NULL, r1=NULL, r2=NULL, kmlfile = NULL, name = "R Line", 
                        description = "", col = NULL, visibility = 1, lwd = 1, 
                        kmlname = "", kmldescription = "", labels=NULL) {
  desborde=FALSE
  # print(col[,1])
  if (is.null(obj)) 
    return(list(header = c("<?xml version=\"1.0\" encoding=\"UTF-8\"?>", 
                           "<kml xmlns=\"http://earth.google.com/kml/2.2\">", 
                           "<Document>", paste("<name>", kmlname, "</name>", 
                                               sep = ""), paste("<description><![CDATA[", 
                                                                kmldescription, "]]></description>", sep = "")), 
                footer = c("</Document>", "</kml>")))
  
  if (class(obj) != "Lines" && class(obj) != "SpatialLinesDataFrame") 
    stop("obj must be of class 'Lines' or 'SpatialLinesDataFrame' [package 'sp']")
  
  if (class(obj) == "SpatialLinesDataFrame") {
    if (length(obj@lines) > 1L) 
      warning(paste("Only the first Lines object with the ID '", 
                    obj@lines[[1]]@ID, "' is taken from 'obj'", 
                    sep = ""))
    obj <- obj@lines[[1]]
  }
  
  
  
  kml <- kmlStyle <- ""
  #Comprobamos si la longitud del slot de Lineas es mayor a 0, en caso contrario devuelve vacio para que Google maps arroje el error de documento invalido
  if(length(obj@Lines)>0){
    kmlHeader <- c("<?xml version=\"1.0\" encoding=\"UTF-8\"?>", 
                   "<kml xmlns=\"http://earth.google.com/kml/2.2\">", 
                   "<Document>", paste("<name>", kmlname, "</name>", 
                                       sep = ""), paste("<description><![CDATA[", kmldescription, 
                                                        "]]></description>", sep = ""))
  }
  else
  {
    kmlHeader<-c("",sep="")
  }
  kmlFooter <- c("</Document>", "</kml>")
  
  
  if (length(obj@Lines) > 0){
    for (i in 1:length(obj@Lines)) {
      kmlStyle <- append(kmlStyle, paste("<Style id=\"",  obj@ID, i,
                                         "\">", sep = ""))
      kmlStyle <- append(kmlStyle, "<LineStyle>")
      kmlStyle <- append(kmlStyle, paste("<width>", lwd, "</width>", 
                                         sep = ""))
      kmlStyle <- append(kmlStyle, paste("<color>af000000</color>", sep = ""))
      
      kmlStyle <- append(kmlStyle, "</LineStyle>")
      
      kmlStyle <- append(kmlStyle, "<PolyStyle>")
      kmlStyle <- append(kmlStyle, paste("<color>", lev2color(labels[i],"af", TRUE),"</color>", sep = ""))
      kmlStyle <- append(kmlStyle, "</PolyStyle>")
      
      
      
      kmlStyle <- append(kmlStyle, "</Style>")
      
      
      kml <- append(kml, "<Placemark>")
      kml <- append(kml, paste("<name>",name, "</name>", sep = ""))
      kml <- append(kml, paste("<description><![CDATA[", labels[i]," mm", 
                               "]]></description>", sep = ""))
      
      kml <- append(kml, paste("<styleUrl>#", obj@ID, i, "</styleUrl>", 
                               sep = ""))
      
      
      
      kml <- append(kml, paste("<visibility>", as.integer(visibility), 
                               "</visibility>", sep = ""))
      ##    kml <- append(kml, "<MultiGeometry>")
      
      
      kml <- append(kml, "<Polygon>")
      #kml <- append(kml, "<altitudeMode>relativeToGround</altitudeMode>")
      #kml <- append(kml, paste("<gx:drawOrder>",i,"</gx:drawOrder>",sep=""))
      kml <- append(kml, "<outerBoundaryIs>")
      
      kml <- append(kml, "<LinearRing>")
      #kml <- append(kml, "<tessellate>1</tessellate>")
      
      kml <- append(kml, "<coordinates>")
      
      kml <- append(kml, paste(coordinates(obj@Lines[[i]])[, 
                                                           1], coordinates(obj@Lines[[i]])[, 2],labels[i], sep = ","))
      if(kmlfile=="kr_polygon.kml" || kmlfile=="idw_polygon.kml")
      {
        cierrax<-NULL
        cierray<-NULL
        if(lines_obj@Lines[[i]]@coords[length(lines_obj@Lines[[i]]@coords[,1]),][1]!=lines_obj@Lines[[i]]@coords[1,][1]&&lines_obj@Lines[[i]]@coords[length(lines_obj@Lines[[i]]@coords[,1]),][2]!=lines_obj@Lines[[i]]@coords[1,][2])
        {
          print(paste("Entrando ",kmlfile, sep=","))
          print(i)
          
          #xl izquierda
          if(lines_obj@Lines[[i]]@coords[length(lines_obj@Lines[[i]]@coords[,1]),][1]==r1[1] || lines_obj@Lines[[i]]@coords[1,][1]==r1[1])
          {
            cierrax<-r1[1]
          }
          
          #yl arriba
          if(lines_obj@Lines[[i]]@coords[length(lines_obj@Lines[[i]]@coords[,1]),][2]==r2[2] || lines_obj@Lines[[i]]@coords[1,][2]== r2[2])
          {
            cierray<-r2[2]
          }
          
          #xl derecha
          if(lines_obj@Lines[[i]]@coords[length(lines_obj@Lines[[i]]@coords[,1]),][1]==r1[2] || lines_obj@Lines[[i]]@coords[1,][1]==r1[2])
          {
            cierrax<-r1[2]
          }
          
          #yl abajo
          if(lines_obj@Lines[[i]]@coords[length(lines_obj@Lines[[i]]@coords[,1]),][2]== r2[1]|| lines_obj@Lines[[i]]@coords[1,][2]==r2[1])
          {
            cierray<-r2[1]
            
          }
          
          if(is.null(cierray))
          {
            if(lines_obj@Lines[[i]]@coords[1,][2]>lines_obj@Lines[[i]]@coords[length(lines_obj@Lines[[i]]@coords[,1]),][2])
            {
              cierrax<-r1[2]
              cierray<-r2[2]
            }else if(lines_obj@Lines[[i]]@coords[1,][2]<lines_obj@Lines[[i]]@coords[length(lines_obj@Lines[[i]]@coords[,1]),][2])
            {
              
              cierrax<-r1[1]
              cierray<-r2[1]
            }
          }
          if(!is.null(cierray) && !is.null(cierrax))
          {
            kml<-append(kml, paste(cierrax,cierray,labels[i],sep=","))
          }
          desborde=TRUE
        }
      }
      kml <- append(kml, "</coordinates>")
      kml <- append(kml, "</LinearRing>")
      kml <- append(kml, "</outerBoundaryIs>")
      kml <- append(kml, "</Polygon>")
      
      ##    kml <- append(kml, "</MultiGeometry>")
      kml <- append(kml, "</Placemark>")
    }
  }
  if (!is.null(kmlfile)) 
    cat(paste(c(kmlHeader, kmlStyle, kml, kmlFooter), 
              sep = "", collapse = "\n"), "\n", file = kmlfile, sep = "")
  else list(style = kmlStyle, content = kml)
  
  return(desborde)
}


tryCatch(
  expr = {
    require(R.utils)
    withTimeout({
              
              
              #limite <- readOGR(".", "Perimetro_Alcaldias")
              
              #Se leen los datos desde el csv (cod_estacion, value_acum, lng, lat *Puede contener las columnas valor_utm_x, valor_utm_y en caso de que se requieran las coordenadas en UTM*)
              d <- read.csv('isoRAD.csv', sep = ";")
              
              #Se omiten los nulos en caso de que existan en los datos leidos
              e <- na.omit(d)
              
              #Se aÃÂÃÂÃÂÃÂ±ade arregelo de coordenadas al objeto e en base a longitud y latitud
              coordinates(e) <- ~ lng + lat
              
              #bubble(e, zcol='value_acum', fill=FALSE, do.sqrt=FALSE, maxsize=2)
              r1 <- range(e@coords[, 1])
              r2 <- range(e@coords[, 2])
              
              r1_th <- range(e@coords[, 1])
              r2_th <- range(e@coords[, 2])
              
              #Medidas del plano en el que se calculara
              r1[1] <- r1[1] - margin
              r1[2] <- r1[2] + margin
              r2[1] <- r2[1] - margin
              r2[2] <- r2[2] + margin
              
              r1_th[1] <- r1_th[1] - margin_th
              r1_th[2] <- r1_th[2] + margin_th
              r2_th[1] <- r2_th[1] - margin_th
              r2_th[2] <- r2_th[2] + margin_th
              
              
              grd <-
                expand.grid(
                  x = seq(from = r1[1], to = r1[2], by = precision),
                  y = seq(from = r2[1], to = r2[2], by = precision)
                )
              grd_th <-
                expand.grid(
                  x = seq(from = r1[1], to = r1_th[2], by = precision),
                  y = seq(from = r2[1], to = r2_th[2], by = precision)
                )
              
              coordinates(grd) <- ~ x + y
              gridded(grd) <- TRUE
              
              coordinates(grd_th) <- ~ x + y
              gridded(grd_th) <- TRUE
              
              #bubble.plt <- bubble(e, "value_acum", col="black", pch=21, main="Precipitacion de pruebas",  sp.layout=list(list("sp.polygons", col="grey",fill="transparent", cdmx), list("sp.points", col="black", pch="+",cex=1.2, subset(e, e$value_acum==0))))
              ########################################VARIOGRAMAS#####################################
              ## make gstat object:
              g <- gstat(id="value_acum", formula=value_acum ~ 1, data=e)
              
              ## another approach:
              # create directional variograms at 0, 45, 90, 135 degrees from north (y-axis)
              v <- variogram(g, alpha=c(0,45,90,135))
              
              #anis=estimateAnisotropy(TheData, "value_acum")
              #TheVariogramModel <- vgm(sill= 0.10, model=c("Lin","Mat"), range=0.05, anis=c(anis$ratio,anis$direction))
              #TheVariogramModel <- vgm( model=c("Lin","Mat"), range=1, anis=c(anis$ratio,anis$direction))
              #TheVariogramModel <- vgm( model="Lin", range=1)
              #TheVariogramModel <- fit.variogram(v, model=vgm(nugget=1, model="Sph", sill=10, range=2, anis=c(30,0.5)))
              ###############################Aceptables############################
              #TheVariogramModel <- vgm(sill= 0.10, model=c("Lin"), range=1)
              #TheVariogramModel <- vgm(sill= 0.10, model=c("Exp"), range=1)
              #TheVariogramModel <- vgm(sill= 0.10, model=c("Sph"), range=1)
              
              TheVariogramModel <- vgm(sill= 0.10, model=c("Ste"), range=1)
              
              
              
              ## 0 and 45 deg. look good. lets fit a linear variogram model:
              ## an un-bounded variogram suggests additional source of anisotropy... oh well.
              #v.fit <- fit.variogram(v, model=vgm(nugget=1, model="Sph", sill=10, range=2, anis=c(30,0.5))
              #)# , anis=c(0, 0.5)))
              v.fit  <- fit.variogram(v, model=TheVariogramModel)
              if(v.fit$range<0){
                print("ENTRO A LA CONDICION DE CORRECCION DE RANGO 1")
                v.fit <- fit.variogram(v, vgm(sill= 0.10, model=c("Sph"), range=1))# , anis=c(0, 0.5)))
              }
              
              if(v.fit$range<0){
                print("ENTRO A LA CONDICION DE CORRECCION DE RANGO 2")
                v.fit <- fit.variogram(v, vgm(sill= 0.10, model=c("Exc"), range=1))# , anis=c(0, 0.5)))
              }
              
              if(v.fit$range<0){
                print("ENTRO A LA CONDICION DE CORRECCION DE RANGO 3")
                v.fit <- fit.variogram(v, vgm(sill= 0.10, model=c("Pow"), range=1))# , anis=c(0, 0.5)))
              }
              
              if(v.fit$range<0){
                print("ENTRO A LA CONDICION DE CORRECCION DE RANGO 4")
                v.fit <- fit.variogram(v, vgm(sill= 0.10, model=c("Cir"), range=1))# , anis=c(0, 0.5)))
              }
              
              ## update the gstat object:
              g <- gstat(g, id="value_acum", model=v.fit) 
              
              ############################ CALCULAR PREDICCIONES, LINEAS DE CONTORNO Y CREAR KMLS ################################
              #Secuencia de coordenadas
              x_th <- seq(r1_th[1],r1_th[2], precision)
              y_th <- seq(r2_th[1],r2_th[2], precision)
              #Secuencia de coordenadas
              x <- seq(r1[1],r1[2], precision)
              y <- seq(r2[1],r2[2], precision)
              #---------------------------------------------------------------------------------------------------
              # Se redondea el valor acumulado para mostrarlo solamente en el pdf
              e@data$value_acum_format <- format(e@data$value_acum, digits = 2)
              
              pts.s <- list("sp.points", e, col="#444444", font=0.2,  pch=20,cex = 0.03)
              pts.station_id <- layout.labels(e, labels = list(labels="station_id",cex = 0.32, col="black"))
              pts.value_acum <- layout.labels(e, labels = list(font=0.6, labels="value_acum_format",cex = 0.28,pos=1,col="black",offset = 0.5, digits=2, corner=c(1,1)))
              at <- seq(0,max(e$value_acum)+5,5)
              
              ###Generacion de vector de colores para pdf#######
              col.sacmex = c("0"="#ffffff")
              col.sacmex.fix= c("0"="#ffffff")
              
              for (level in levels)
              {
                col.sacmex<-c(col.sacmex,level=paste("#",lev2color(level=level,reverse = FALSE),sep=""))
                
              }
              
              for (level in levels_fix)
              {
                col.sacmex.fix<-c(col.sacmex.fix,level=paste("#",lev2color(level=level,reverse = FALSE, fix=TRUE),sep=""))
              }
              
              # Thiessen Prediction
              thiessen.d <- krige(value_acum ~ 1, e, grd_th, nmax = 1)
              
              
              #Calculo de lineas de contorno para Thiessen
              cl <- contourLines(x_th, y_th, z=array(thiessen.d@data[,1], dim=c(length(x_th),length(y_th))), levels=levels)
              
              line_list = list()
              label_list = list()
              
              for (clitem in cl) {
                # utm1 <- data.frame(x=clitem$x,y=clitem$y)
                #  coordinates(utm1) <- ~x+y
                # proj4string(utm1) <- CRS("+proj=utm +zone=14 +datum=WGS84 +units=m +ellps=WGS84")
                # utm2 <- spTransform(utm1,CRS("+proj=longlat +datum=WGS84"))
                #coords <- cbind(utm2@coords[,1],utm2@coords[,2])
                coords <- cbind(clitem$x, clitem$y)
                l <- Line(coords)
                line_list <- c(line_list, l)
                label_list <- c(label_list, clitem$level)
              }
              #Objeto final contenedor de las lineas de contorno para Thiessen
              lines_obj <- Lines(line_list, "Isohyets")
              
              #Llamada de funciones para creacion de archivo KML Thiessen (lineas y poligonos)
              kmlLine2(lines_obj, "th_line.kml", name="SACMEX Thiessen Lineas", description="Isohyet test", kmlname="Isohyets KML", kmldescription="Isohyet test KML", labels=label_list)
              desborde_th=kmlPolygon2(lines_obj, r1, r2, "th_polygon.kml", name="SACMEX Thiessen Areas", description="Isohyet test", kmlname="Isohyets KML", kmldescription="Isohyet test KML", labels=label_list)
              
              p1<-spplot(thiessen.d, "var1.pred", asp=1,ylab="Precipitacion Acumulada [mm]", contour=TRUE, at=at ,col.regions=col.sacmex[seq(1,length(at)-1,1)],sp.layout = list(pts.s,sp.cdmx,pts.value_acum,pts.station_id) ,main="METODO THIESSEN",pretty=TRUE)
              
              p1fix<-spplot(thiessen.d, "var1.pred", asp=1,ylab="Precipitacion Acumulada [mm]", contour=TRUE, at=at ,col.regions=col.sacmex.fix[seq(1,length(at)-1,1)],sp.layout = list(pts.s,sp.cdmx,pts.value_acum,pts.station_id) ,main="METODO THIESSEN",pretty=TRUE)
              
              #---------------------------------------------------------------------------------------------------
              
              #Inverso de la distancia (idw) Prediction
              
              idw.d = idw(value_acum ~ 1, e, grd)
              
              
              #Calculo de lineas de contorno para IDW
              cl <- contourLines(x, y, z=array(idw.d@data[,1], dim=c(length(x),length(y))), levels=levels)
              
              line_list = list()
              label_list = list()
              
              for (clitem in cl) {
                # utm1 <- data.frame(x=clitem$x,y=clitem$y)
                #  coordinates(utm1) <- ~x+y
                # proj4string(utm1) <- CRS("+proj=utm +zone=14 +datum=WGS84 +units=m +ellps=WGS84")
                # utm2 <- spTransform(utm1,CRS("+proj=longlat +datum=WGS84"))
                #coords <- cbind(utm2@coords[,1],utm2@coords[,2])
                coords <- cbind(clitem$x, clitem$y)
                l <- Line(coords)
                line_list <- c(line_list, l)
                label_list <- c(label_list, clitem$level)
              }
              #Objeto final contenedor de las lineas de contorno para IDW
              lines_obj <- Lines(line_list, "Isohyets")
              
              #Llamada de funciones para creacion de archivo KML IDW (lineas y poligonos)
              kmlLine2(lines_obj, "idw_line.kml", name="SACMEX IDW Lineas", description="Isohyet test", kmlname="Isohyets KML", kmldescription="Isohyet test KML", labels=label_list)
              desborde_idw=kmlPolygon2(lines_obj, r1, r2, "idw_polygon.kml", name="SACMEX IDW Areas", description="Isohyet test", kmlname="Isohyets KML", kmldescription="Isohyet test KML", labels=label_list)
              
              p2 <- spplot(idw.d, "var1.pred", asp=1,cuts=19,ylab="Precipitacion Acumulada [mm]", at=at ,col.regions=col.sacmex[seq(1,length(at)-1,1)],sp.layout = list(pts.s,sp.cdmx,pts.value_acum,pts.station_id),main="METODO IDW", pretty=TRUE, col='black')
              
              p2fix <- spplot(idw.d, "var1.pred", asp=1,cuts=19,ylab="Precipitacion Acumulada [mm]", at=at ,col.regions=col.sacmex.fix[seq(1,length(at)-1,1)],sp.layout = list(pts.s,sp.cdmx,pts.value_acum,pts.station_id),main="METODO IDW", pretty=TRUE, col='black')
              
              #---------------------------------------------------------------------------------------------------
              #Kriging Prediction
              #summary(e)
              #kriging_result= autoKrige(value_acum ~ 1, e, grd)
              #plot(kriging_result)
              
              #p3 <- spplot(kriging_result$krige_output, "var1.pred", asp=1,cuts=40,ylab="Precipitación Acumulada [mm]", at=at ,col.regions=col.sacmex[seq(1,length(at)-1,1)],sp.layout =list(pts.s,sp.cdmx,pts.value_acum,pts.station_id),main="METODO KRIGING", pretty=TRUE, col='black', edge.col = "black")
              
              kr.d <- predict(g, model=v.fit, newdata=grd)
              #kr.d=st_as_sf(kr.d)
              #st_write(kr.d, "kr.shp")
              
              #vgm()
              #Calculo de lineas de contorno para Kriging
              #cl <- contourLines(x, y, z=array(kriging_result$krige_output@data[,1], dim=c(length(x),length(y))), levels=levels)
              cl <- contourLines(x, y, z=array(kr.d@data[,1], dim=c(length(x),length(y))), levels=levels)
              
              line_list = list()
              label_list = list()
              
              for (clitem in cl) {
                # utm1 <- data.frame(x=clitem$x,y=clitem$y)
                #  coordinates(utm1) <- ~x+y
                # proj4string(utm1) <- CRS("+proj=utm +zone=14 +datum=WGS84 +units=m +ellps=WGS84")
                # utm2 <- spTransform(utm1,CRS("+proj=longlat +datum=WGS84"))
                #coords <- cbind(utm2@coords[,1],utm2@coords[,2])
                coords <- cbind(clitem$x, clitem$y)
                l <- Line(coords)
                line_list <- c(line_list, l)
                label_list <- c(label_list, clitem$level)
              }
              #Objeto final contenedor de las lineas de contorno para Kriging
              lines_obj <- Lines(line_list, "Isohyets")
              
              #Llamada de funciones para creacion de archivo KML Kriging (lineas y poligonos)
              kmlLine2(lines_obj, "kr_line.kml", name="SACMEX Kriging Lineas", description="Isohyet test", kmlname="Isohyets KML", kmldescription="Isohyet test KML", labels=label_list)
              desborde_kr=kmlPolygon2(lines_obj, r1, r2, "kr_polygon.kml", name="SACMEX Kriging Areas", description="Isohyet test", kmlname="Isohyets KML", kmldescription="Isohyet test KML", labels=label_list)
              
              
              p3 <- spplot(kr.d, "value_acum.pred", asp=1,cuts=40,ylab="Precipitacion Acumulada [mm]", at=at ,col.regions=col.sacmex[seq(1,length(at)-1,1)],sp.layout =list(pts.s,sp.cdmx,pts.value_acum,pts.station_id),main="METODO KRIGING", pretty=TRUE, col='black', edge.col = "black")
              
              p3fix <- spplot(kr.d, "value_acum.pred", asp=1,cuts=40,ylab="Precipitacion Acumulada [mm]", at=at ,col.regions=col.sacmex.fix[seq(1,length(at)-1,1)],sp.layout =list(pts.s,sp.cdmx,pts.value_acum,pts.station_id),main="METODO KRIGING", pretty=TRUE, col='black', edge.col = "black")
              
              
              
              #Convertir PDF las isoyetas
              
              pdf(file = "isoyetasRad.pdf",   # The directory you want to save the file in
                  width = 5.77, # The width of the plot in inches
                  height = 9.16) # The height of the plot in inches
              
              print(p1)
              print(p2)
              print(p3)
              
              dev.off()
              
              
              pdf(file = "isoyetasRadfix.pdf",   # The directory you want to save the file in
                  width = 5.77, # The width of the plot in inches
                  height = 9.16) # The height of the plot in inches
              
              print(p1fix)
              print(p2fix)
              print(p3fix)
              
              dev.off()
              
    },timeout =EXECUTION_TIME_LIMIT, onTimeout = "warning")
  },
  TimeoutException = function(ex) {
    graphics.off() # close devices before printing
    cat("Now sending PDF graphics to the printer:\n")
    system("lpr Rplots.pdf")
    cat("bye bye...\n")
    print(e)
    stop("Error: Tiempo limite de ejecución alcanzado")
  }
  
)

