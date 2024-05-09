import subprocess 
import os
import time

#import Functions.isoyetas.pdfGenerator as pdf
__ruta= os.getcwd()

def exec():
    time.sleep(0.5)
    try:
        subprocess.run(['Rscript', f'{__ruta}/Functions/isoyetas/Isoyetas_GPRS.R'], check=False)
        subprocess.run(['Rscript', f'{__ruta}/Functions/isoyetas/Isoyetas_RADIO.R'], check=False)
    except subprocess.CalledProcessError as e:
        print(f"Error al ejecutar el script de R: {e}")