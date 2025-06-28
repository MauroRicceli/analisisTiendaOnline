import pandas as pd
import numpy as np
import sys, os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db.connector import mongo_connector

class analysis_db:
    def __init__(self):
        self.conexion = ""
        self.db = self.conexion.get_db("tienda")
        
    
    def obtener_datos(self, collection_name):
        collection = self.conexion.get_collection(self.db, collection_name)
        data = list(collection.find())

        return data
    
#EXAMPLES
#aux = analysis_db()
#df = pd.DataFrame(pd.json_normalize(aux.obtener_datos("ventas")))
#sin_tarjeta_credito = df[df["orden_compra.metodo_pago"] != "Tarjeta Crédito"]

#print(df.head())
#print(df.groupby("datos_cliente.nombre_cliente")["orden_compra.precio_total"].sum())
#print(df.sort_values(by="orden_compra.id_orden", ascending=True))
#print(sin_tarjeta_credito.head())

#Análisis exploratorio de datos (EDA)
#•	Cantidad de clientes por ubicación
    def cant_clientes_por_ubicacion(self):
        df = pd.DataFrame(pd.json_normalize(self.obtener_datos("ventas")))
        print(df["datos_cliente.domicilio.provincia"].value_counts())

#•	Categorías de productos más vendidas
    def categorias_mas_vendidas(self):
        df = pd.DataFrame(pd.json_normalize(self.obtener_datos("ventas"), record_path=["orden_compra", "productos"], meta=[["orden_compra", "id_orden"]], errors='ignore'))
        #print(df.head())
        dfExploded = df.explode("categorias").reset_index(drop=True) #CREO UNA FILA POR CADA VALUE EN EL ARRAY CATEGORIAS PARA PODER VISUALIZAR MEJOR LOS DATOS Y HACER EL COUNT.
        print(dfExploded["categorias"].value_counts())

#•	Ticket promedio de compra
    def ticket_promedio_compra(self):
        df = pd.DataFrame(pd.json_normalize(self.obtener_datos("ventas")))
        print(df["orden_compra.precio_total"].mean())



aux = analysis_db()
#aux.cant_clientes_por_ubicacion()
#aux.categorias_mas_vendidas()
aux.ticket_promedio_compra()

