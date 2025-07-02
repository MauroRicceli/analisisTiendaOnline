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
        df = df["datos_cliente.domicilio.provincia"].value_counts()
        print(df)

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
        df1 = df[['orden_compra.fecha_pedido.$date','orden_compra.id_orden','orden_compra.precio_total']]
        df1 = df1.rename(columns={'orden_compra.fecha_pedido.$date':'orden_compra.fecha_pedido'})
        df2 = df[['orden_compra.fecha_pedido','orden_compra.id_orden', 'orden_compra.precio_total']]

        dfAux = pd.concat([df2,df1])
        #LIMPIAR FECHAS EN NULL POSIBLES
        dfAux.dropna(inplace=True)

        #como algunas fechas tienen zona horaria y otras no, las pongo todas sin zona horaria.
        dfAux['orden_compra.fecha_pedido'] = pd.to_datetime(dfAux['orden_compra.fecha_pedido'],utc=True).dt.tz_localize(None)

        #extraigo el mes y el año de la fecha para un mejor calculo
        dfAux['año_compra'] = pd.to_datetime(dfAux['orden_compra.fecha_pedido']).dt.year
        dfAux['mes_compra'] = pd.to_datetime(dfAux['orden_compra.fecha_pedido']).dt.month

        dfAux = dfAux[['año_compra','mes_compra','orden_compra.precio_total']]

        #cantidad de ventas totales por mes.
        dfAux = dfAux.groupby(by=['año_compra','mes_compra'])['orden_compra.precio_total'].agg('sum').reset_index()

        dfAux = dfAux.sort_values(by=['año_compra','mes_compra'], ascending=False)

        return dfAux

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

#•	Productos más vendidos totales
    def mejores_productos(self):
        """
        Muestra los productos mas vendidos.

        """

        df = pd.DataFrame(pd.json_normalize(self.obtener_datos("ventas"), record_path=["orden_compra", "productos"], meta=[["orden_compra", "id_orden"]], errors='ignore'))

        #obtengo la cantidad de cada producto que se vendió hasta este momento, teniendo en cuenta la cantidad de cada uno en la orden de compra
        df['cant_ventas_prod'] = df.groupby(['identificador_producto'])['cantidad'].transform('sum')

        #elimino las duplicaciones para mejor analisis
        df.drop_duplicates(subset="nombre_producto", inplace=True)

        df = df[['nombre_producto', 'identificador_producto', 'categorias', 'cant_ventas_prod', 'precio_unitario']]

        #ordeno de mayor a menor por cantidad de ventas de cada producto
        df = df.sort_values(by='cant_ventas_prod', ascending=False)

        return df

#•	Por ubicación
    def segmentacion_clientes_ubicacion(self):
        df = pd.DataFrame(pd.json_normalize(self.obtener_datos("ventas")))

        #tomo las columnas que considero necesarias para una buena segmentación
        df = df[['datos_cliente.nombre_cliente', 'datos_cliente.email', 'datos_cliente.domicilio.pais', 'datos_cliente.domicilio.provincia', 'datos_cliente.domicilio.localidad']]

        #elimino las duplicaciones de clientes ya que no me interesan para este analisis
        df.drop_duplicates(subset='datos_cliente.nombre_cliente', inplace=True)

        #ordeno por pais - prov - localidad y calculo la cantidad por ubicacion de cada combinacion
        df = df.groupby(['datos_cliente.domicilio.pais','datos_cliente.domicilio.provincia', 'datos_cliente.domicilio.localidad']).size()
        return df


#•	Por volumen de compras (cuanto gasta en promedio)
    def segmentacion_clientes_volCompras(self):
        df = pd.DataFrame(pd.json_normalize(self.obtener_datos("ventas")))

        #dejo las columnas necesarias para el analisis
        df = df[['datos_cliente.nombre_cliente', 'datos_cliente.email','orden_compra.precio_total']]
        
        #obtengo el gasto promedio de las compras de cada cliente y lo guardo en una nueva tabla
        df['gasto_promedio'] = df.groupby(['datos_cliente.nombre_cliente', 'datos_cliente.email'])['orden_compra.precio_total'].transform('mean').round(decimals=2)

        #ya no me interesa precio total asi que dejo la columna
        df = df[['datos_cliente.nombre_cliente', 'datos_cliente.email', 'gasto_promedio']]

        #elimino la duplicacion de clientes ya que hay repeticion de datos de gasto promedio por cada entrada de ese cliente
        df.drop_duplicates(subset='datos_cliente.email', inplace=True)

        #muestro de mayor a menor por gasto promedio
        df = df.sort_values(by='gasto_promedio', ascending=False)

        return df

#•	Por categorías preferidas
    def segmentacion_clientes_categoriasPreferidas(self):
        #consigo el dataframe de los productos con su orden de compra para mergear con los datos del cliente que hizo esa compra
        dfProducto = pd.DataFrame(pd.json_normalize(self.obtener_datos("ventas"), record_path=['orden_compra','productos'], meta=[['orden_compra','id_orden']]))
        #obtengo las categorias fuera del array como una fila cada uno
        dfProducto = dfProducto.explode('categorias',ignore_index=True)

        #consigo el datafrme con los datos del cliente y el id de orden
        dfClientes = pd.DataFrame(pd.json_normalize(self.obtener_datos("ventas")))
        dfClientes = dfClientes[['datos_cliente.nombre_cliente', 'datos_cliente.email', 'orden_compra.id_orden']]
        
        #uno ambos dataframe por el id_orden para tener las categorias asociadas al cliente que la compro
        dfClientesXProd = pd.merge(dfClientes,dfProducto, on='orden_compra.id_orden')
        
        #limpio columnas no deseadas
        dfClientesXProd = dfClientesXProd[['datos_cliente.nombre_cliente', 'datos_cliente.email', 'nombre_producto', 'categorias']]

        #obtengo las compras de cada categoria por cliente.
        print(dfClientesXProd.groupby(['datos_cliente.nombre_cliente', 'datos_cliente.email'])['categorias'].value_counts())

    #informacion para una visualización
    def gasto_promedio_por_ubicacion(self):
        df = pd.DataFrame(pd.json_normalize(self.obtener_datos("ventas")))

        #tomo las columnas que considero necesarias para una buena segmentación
        df = df[['datos_cliente.email', 'datos_cliente.domicilio.pais', 'datos_cliente.domicilio.provincia', 'datos_cliente.domicilio.localidad']]

        df.drop_duplicates(subset='datos_cliente.email',inplace=True)

        #uso otro metodo para obtener información util
        dfGastoPromedioCliente = self.segmentacion_clientes_volCompras()

        dfGastoPromedioCliente = dfGastoPromedioCliente[['datos_cliente.email','gasto_promedio']]

        #mergeo las tablas por email
        dfGastoPromPorUbicacion = pd.merge(df, dfGastoPromedioCliente, on='datos_cliente.email')

        #dejo la informacion interesante
        dfGastoPromPorUbicacion = dfGastoPromPorUbicacion[['datos_cliente.domicilio.pais','datos_cliente.domicilio.provincia', 'datos_cliente.domicilio.localidad', 'gasto_promedio']]
        
        #obtengo el gasto promedio por ubicacion (no por cliente unitario)
        dfGastoPromPorUbicacion['gasto_promedio'] = dfGastoPromPorUbicacion.groupby(['datos_cliente.domicilio.pais','datos_cliente.domicilio.provincia','datos_cliente.domicilio.localidad'])['gasto_promedio'].transform('mean').round(2)

        #elimino duplicacion de datos
        dfGastoPromPorUbicacion.drop_duplicates(subset=['datos_cliente.domicilio.pais','datos_cliente.domicilio.provincia','datos_cliente.domicilio.localidad'], inplace=True)

        return dfGastoPromPorUbicacion

#•	Dashboard con mapa de clientes
    def exportar_mapa_clientes(self):
        csv1 = self.segmentacion_clientes_ubicacion()
        csv2 = self.gasto_promedio_por_ubicacion()

        csv1.to_csv("datos_ubi_clientes.csv")
        csv2.to_csv("datos_gasto_promedio_por_ubi.csv", decimal=",")

#•	Dashboard con evolución mensual de ventas
    def exportar_evol_mensual_ventas(self):
        csv1 = self.total_ventas_mes()

        csv1.to_csv("evolucion_mensual_ventas.csv", decimal=",")

#•	Gráfico de barras de productos más vendidos (totales y por mes)
    def exportar_prod_mas_vendidos(self):
        csv1 = self.mejores_productos()
        csv1.to_csv("mejores_productos_historico.csv")

        csv2 = self.obtener_prod_mas_vendidos_por_mes()
        csv2.to_csv("prod_mas_vendidos_por_mes.csv")

    def obtener_prod_mas_vendidos_por_mes(self):
        """
        Muestra los productos mas vendidos por mes.

        """

        dfP = pd.DataFrame(pd.json_normalize(self.obtener_datos("ventas"), record_path=["orden_compra", "productos"], meta=[["orden_compra", "id_orden"]], errors='ignore'))

        dfP = dfP[['orden_compra.id_orden','nombre_producto', 'cantidad','identificador_producto', 'categorias','precio_unitario']]

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

        dfAux = dfAux[['año_compra','mes_compra', 'orden_compra.id_orden']]

        #uno los dataframes por el id orden
        df_ventas_prod_mes = pd.merge(dfP, dfAux, on='orden_compra.id_orden')

        df_ventas_prod_mes = df_ventas_prod_mes[['nombre_producto', 'identificador_producto', 'categorias','cantidad','precio_unitario','año_compra','mes_compra']]

        #hago la suma de la cantidad de productos vendidos por cada mes/año
        df_ventas_prod_mes['cantidad_ventas_mes'] = df_ventas_prod_mes.groupby(['nombre_producto','año_compra','mes_compra'])['cantidad'].transform('sum')

        df_ventas_prod_mes = df_ventas_prod_mes[['nombre_producto', 'identificador_producto', 'categorias','precio_unitario','año_compra','mes_compra', 'cantidad_ventas_mes']]

        #elimino duplicidades
        df_ventas_prod_mes.drop_duplicates(subset=['identificador_producto','año_compra', 'mes_compra', 'cantidad_ventas_mes'], inplace=True)
        
        return df_ventas_prod_mes


aux = analysis_db()
#aux.cant_clientes_por_ubicacion()
#aux.categorias_mas_vendidas()
#aux.ticket_promedio_compra()
#aux.frec_compra_cliente()
#aux.total_ventas_mes()
#aux.ingreso_prom_por_cliente()
#aux.retencion_clientes()
#aux.mejores_productos()
#aux.segmentacion_clientes_ubicacion()
#aux.segmentacion_clientes_volCompras()
#aux.segmentacion_clientes_categoriasPreferidas()
#aux.gasto_promedio_por_ubicacion()
#aux.obtener_prod_mas_vendidos_por_mes()

##aux.exportar_mapa_clientes()
##aux.exportar_evol_mensual_ventas()
aux.exportar_prod_mas_vendidos()