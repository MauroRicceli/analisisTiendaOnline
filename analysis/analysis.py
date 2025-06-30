import pandas as pd
import numpy as np
import sys, os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db.connector import mongo_connector

class analysis_db:
    def __init__(self):
        self.conexion = 
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

#•	Frecuencia de compra por cliente
    def frec_compra_cliente(self):
        df = pd.DataFrame(pd.json_normalize(self.obtener_datos("ventas")))

        #como hay datos mal cargados en distintos niveles los junto todos en un unico dataframe concatenados
        df1 = df[['orden_compra.fecha_pedido.$date','orden_compra.id_orden','datos_cliente.nombre_cliente','datos_cliente.email']]
        df1 = df1.rename(columns={'orden_compra.fecha_pedido.$date':'orden_compra.fecha_pedido'})
        df2 = df[['orden_compra.fecha_pedido','orden_compra.id_orden','datos_cliente.nombre_cliente','datos_cliente.email']]

        dfAux = pd.concat([df2,df1])
        #LIMPIAR FECHAS EN NULL POSIBLES
        dfAux.dropna(inplace=True)
        #como algunas fechas tienen zona horaria y otras no, las pongo todas sin zona horaria.

        dfAux['orden_compra.fecha_pedido'] = pd.to_datetime(dfAux['orden_compra.fecha_pedido'],utc=True).dt.tz_localize(None)


        #organizo los datos por fecha ascendente y por emails de cliente
        dfDias = dfAux.sort_values(by=['orden_compra.fecha_pedido','datos_cliente.email'])

        #creo una nueva tabla donde almaceno la diferencia de dias entre las fechas de compra
        dfDias['frec_compra'] = dfDias.groupby(by='datos_cliente.email')['orden_compra.fecha_pedido'].diff().dt.days

        #la primera fecha de cada cliente se quedara en nulo, asi que lo pongo en 0.
        dfDias.fillna(0, inplace=True)

        #saco el promedio de compra en dias de cada cliente segun la diferencia de dias calculada anteriormente
        dfDias['frec_compra'] = dfDias.groupby('datos_cliente.nombre_cliente')['frec_compra'].transform('mean')

        #ejemplo
        print(dfDias[dfDias['datos_cliente.nombre_cliente'] == 'Bianca Mason'])

#•	Total de ventas por mes
    def total_ventas_mes(self):
        df = pd.DataFrame(pd.json_normalize(self.obtener_datos("ventas")))

        #como hay datos mal cargados en distintos niveles los junto todos en un unico dataframe concatenados
        df1 = df[['orden_compra.fecha_pedido.$date','orden_compra.id_orden']]
        df1 = df1.rename(columns={'orden_compra.fecha_pedido.$date':'orden_compra.fecha_pedido'})
        df2 = df[['orden_compra.fecha_pedido','orden_compra.id_orden']]

        dfAux = pd.concat([df2,df1])
        #LIMPIAR FECHAS EN NULL POSIBLES
        dfAux.dropna(inplace=True)

        #como algunas fechas tienen zona horaria y otras no, las pongo todas sin zona horaria.
        dfAux['orden_compra.fecha_pedido'] = pd.to_datetime(dfAux['orden_compra.fecha_pedido'],utc=True).dt.tz_localize(None)

        #extraigo el mes y el año de la fecha para un mejor calculo
        dfAux['año_compra'] = pd.to_datetime(dfAux['orden_compra.fecha_pedido']).dt.year
        dfAux['mes_compra'] = pd.to_datetime(dfAux['orden_compra.fecha_pedido']).dt.month

        #cantidad de ventas totales por mes.
        print(dfAux.groupby(by=['año_compra','mes_compra'])['mes_compra'].value_counts())

#•	Ingreso promedio por cliente
    def ingreso_prom_por_cliente(self):
        df = pd.DataFrame(pd.json_normalize(self.obtener_datos("ventas")))

        #obtengo las columnas interesantes
        df = df[['datos_cliente.email', 'orden_compra.precio_total', 'orden_compra.id_orden']]

        #obtengo el gasto total de cada cliente
        df['gasto_total'] = df.groupby(by='datos_cliente.email')['orden_compra.precio_total'].transform('sum')

        #saco el promedio del gasto de cada cliente.
        print(df['gasto_total'].mean())

#•	Retención de clientes (cuántos vuelven a comprar)
    def retencion_clientes(self):
        df = pd.DataFrame(pd.json_normalize(self.obtener_datos("ventas")))
        df = df[['datos_cliente.email', 'orden_compra.id_orden']]

        #almaceno la cantidad de compras de cada cliente
        df['cant_compras'] = df.groupby(by='datos_cliente.email')['datos_cliente.email'].transform('count')
        
        #obtengo aquellos clientes que volvieron a comprar al menos una vez (osea dos compras).
        df = df[df['cant_compras'] >= 2]
        
        #elimino la duplicacion de clientes
        df.drop_duplicates(subset='datos_cliente.email', inplace=True)

        #cantidad de filas en el dataframe significan cantidad de clientes que volvieron a comprar (sin duplicados.)
        print(df['datos_cliente.email'].count())

#•	Productos más vendidos
    def mejores_productos(self):
        df = pd.DataFrame(pd.json_normalize(self.obtener_datos("ventas"), record_path=["orden_compra", "productos"], meta=[["orden_compra", "id_orden"]], errors='ignore'))

        #obtengo la cantidad de cada producto que se vendió hasta este momento, teniendo en cuenta la cantidad de cada uno en la orden de compra
        df['cant_ventas_prod'] = df.groupby(['identificador_producto'])['cantidad'].transform('sum')

        #elimino las duplicaciones para mejor analisis
        df.drop_duplicates(subset="nombre_producto", inplace=True)

        #ordeno de mayor a menor por cantidad de ventas de cada producto, como solo hay 5 productos no puse un limite en le head.
        print(df.sort_values(by='cant_ventas_prod', ascending=False).head())


aux = analysis_db()
#aux.cant_clientes_por_ubicacion()
#aux.categorias_mas_vendidas()
#aux.ticket_promedio_compra()
#aux.frec_compra_cliente()
#aux.total_ventas_mes()
#aux.ingreso_prom_por_cliente()
#aux.retencion_clientes()
aux.mejores_productos()

