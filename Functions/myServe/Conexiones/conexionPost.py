import psycopg2
import os
class __BaseDatos:
    def __init__(self):
        self.__connection = psycopg2.connect(
            host = os.getenv("Postgres_IP"),
            user = os.getenv("Postgres_User"),
            password = os.getenv("Postgres_pass"),
            database = os.getenv("Postgres_DB"),
            port=os.getenv("Postgres_Port")
            )
        print("Conexión Exitosa")
    def qwerysAll(self,consulta):
        self.cursor = self.__connection.cursor()
        try:
            self.sql = consulta
            self.cursor.execute(self.sql)
            self.rows = self.cursor.fetchall()
            self.cursor.close()
            return self.rows
        except psycopg2.InterfaceError:
            self.__connection=None
            self.__connection = psycopg2.connect(
                host = os.getenv("Postgres_IP"),
                user = os.getenv("Postgres_User"),
                password = os.getenv("Postgres_pass"),
                database = os.getenv("Postgres_DB"),
                port=os.getenv("Postgres_Port")
            )
            print("Conexión Exitosa")
        except Exception as e:
            self.cursor.close()
            print(f"error de consulta {e}")
__db=__BaseDatos()
##___________________________________________________________________________________
def active():
    id=[]
    for x in  __db.qwerysAll("select station_code FROM stations_rads where active=True"):
        id.append(x[0])
    return id

#________________________________getters___________________________________________
