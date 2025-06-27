from datetime import datetime
from faker import Faker
from connector import mongo_connector

fake = Faker('es_AR')

#FUNCIONA CORRECTAMENTE PERO NO ES POSIBLE GENERAR TANTOS DATOS FALSOS COMO SE DESEA. SE HACE INSERTS MANUALES POR MONGO COMPASS CREANDO ESTOS DATOS MASIVOS POR CHATGPT.

def generar_datos():
    return {
        "datos_cliente": {
            "nombre_cliente": fake.name(),
            "numero_telefono": fake.phone_number(),
            "email": fake.email(),
            "domicilio": {
                "direccion_envio": fake.street_address(),
                "localidad": fake.city(),
                "provincia": fake.province(),
                "pais": "Argentina"
            }
        },
        "orden_compra": {
            "productos": [
            {
                "nombre_producto": fake.job(),
                "identificador_producto": fake.random_number(digits=12, fix_len=True),
                "cantidad": fake.random_int(min=1, max=8),
                "precio_unitario": fake.pricetag(),
                "categorias": ['veraniego']
            }],
            "fecha_pedido": datetime.combine(fake.date_this_decade(),datetime.min.time()),
            "precio_total": 104800,
            "metodo_pago": "tarjeta",
            "id_orden": fake.random_number(digits=12, fix_len=True),
        }
    }

def generar_datos_masivos(): 
    for i in range(fake.random_int(min= 1, max=2)):
        data = []
        aux = generar_datos()
        data.append(aux)
        return data
    
#
#db = aux.get_db("tienda")
#coll1 = aux.get_collection(db, "ventas")
#aux.insertMany_into_collection(coll1, generar_datos_masivos())