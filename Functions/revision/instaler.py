import subprocess
import os
route=os.getcwd()
route="C:/Users/sacmex6704/Documents/sevicio/simollu-flask/resources/auxData/reqs.txt"
library=open(route,'r')
for x in library:
    subprocess.run(f"python -m pip install {x}")