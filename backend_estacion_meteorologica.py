import paho.mqtt.client as mqtt
import mysql.connector
from mysql.connector import Error
import json
import time

# ---------- CONFIGURACIÓN MQTT ----------
MQTT_CLIENT_ID = "python-backend-estacion"
MQTT_BROKER = "broker.mqttdashboard.com"
MQTT_TOPIC = "estacion-meteorologica-ESP32"

# ---------- CONFIGURACIÓN MYSQL ----------
DB_HOST = "localhost"
DB_USER = "estacion"
DB_PASSWORD = "1234abc!"
DB_NAME = "estacion_meteorologica"

# ---------- FUNCIONES ----------
def conectar_db():
    """Crea y devuelve la conexión a la base de datos."""
    try:
        conn = mysql.connector.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME
        )
        return conn
    except Error as e:
        print("Error al conectar a MySQL:", e)
        return None

def guardar_en_db(temp, hum, pres):
    """Guarda los datos recibidos en la tabla mediciones."""
    conn = conectar_db()
    if conn is None:
        print("No se pudo conectar a la base de datos.")
        return
    try:
        cursor = conn.cursor()
        sql = "INSERT INTO mediciones (temperatura, humedad, presion) VALUES (%s, %s, %s)"
        cursor.execute(sql, (temp, hum, pres))
        conn.commit()
        print(f"Guardado en DB: Temp={temp}, Hum={hum}, Pres={pres}")
    except Error as e:
        print("Error al insertar en la base de datos:", e)
    finally:
        cursor.close()
        conn.close()

# ---------- CALLBACKS MQTT ----------
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Conectado al broker MQTT!")
        client.connected_flag = True
        client.subscribe(MQTT_TOPIC)
    else:
        print("Fallo al conectar, código de error:", rc)
        client.connected_flag = False

def on_disconnect(client, userdata, rc):
    print("Se perdió la conexión con el broker MQTT. Reintentando...")
    client.connected_flag = False

def on_message(client, userdata, msg):
    try:
        payload = msg.payload.decode('utf-8')
        print("Mensaje recibido:", payload)
        data = json.loads(payload)
        temp = data['temperatura']
        hum = data['humedad']
        pres = data['presion']
        guardar_en_db(temp, hum, pres)
    except Exception as e:
        print("Error al procesar mensaje MQTT:", e)

# ---------- CONFIGURAR CLIENTE MQTT ----------
mqtt.Client.connected_flag = False  # atributo personalizado
client = mqtt.Client(MQTT_CLIENT_ID)
client.on_connect = on_connect
client.on_disconnect = on_disconnect
client.on_message = on_message

# ---------- CONEXIÓN Y LOOP ----------
while True:
    if not client.connected_flag:
        try:
            client.connect(MQTT_BROKER)
            client.loop_start()  # loop en segundo plano
            # Espera breve hasta confirmar conexión
            timeout = time.time() + 5
            while not client.connected_flag and time.time() < timeout:
                time.sleep(0.1)
        except Exception as e:
            print("Error al conectar al broker MQTT:", e)
            time.sleep(5)  # espera antes de reintentar
    time.sleep(1)
