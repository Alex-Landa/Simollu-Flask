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
