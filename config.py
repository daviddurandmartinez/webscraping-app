from decouple import config
import time
from datetime import datetime,timedelta

def get_sql_server_config():
    # Esto asegura que si la variable no existe, se muestre un error.
    try:
        return {
            "DRIVER": config("DRIVER"),
            "SERVER": config("SERVER"),
            "USER": config("USER"),    
            "PASSWORD": config("PASSWORD"),
            "DATABASE": config("DATABASE")
        }
    except Exception as e:
        print(f"Error {e}")
        return None
    
# Cargar la configuración principal
SQL_SERVER_CONFIG = get_sql_server_config()

# OBTENER FECHA Y HORA ACTUAL
# FECHA_HORA = time.strftime('%Y-%m-%d %H:%M:%S')
FECHA_HORA = datetime.now()
FECHA_ACTUAL = datetime.now().date()
FECHA_ANTERIOR = datetime.now().date() - timedelta(days=1)
HORA_ACTUAL = time.strftime('%H:%M')

# DECLARACION DE VARIABLES URL (FUENTE: GESTION)
URL_GESTION = {
    'empresas': 'https://gestion.pe/economia/empresas/',
    'finanzas': 'https://gestion.pe/tu-dinero/finanzas-personales/',
    'ultimas_noticias': 'https://gestion.pe/ultimas-noticias/'
}

# DECLARACION DE VARIABLES URL (FUENTE: RPP)
URL_RPP = {
    'ultimas_noticias': 'https://rpp.pe/ultimas-noticias',
    'politica': 'https://rpp.pe/politica',
    'actualidad':'https://rpp.pe/actualidad'
}

# DECLARACION DE VARIABLES URL (CLIMA)
URL_CLIMA = {
    'Lima': 'https://weather.com/es-PE/tiempo/hoy/l/188551fb506b09449342a6f282da07372e19f8e28fbe99767fd4852534aee1d1',
    'San Miguel': 'https://weather.com/es-PE/tiempo/hoy/l/8643595df876f056f8e57b483b5ac354a22a65217dca6c0ba0a387cae9502099',
    'Jesus maria': 'https://weather.com/es-PE/tiempo/hoy/l/8643595df876f056f8e57b483b5ac354211f463ae8fa8635157dba9510c42802',
    'Surco': 'https://weather.com/es-PE/tiempo/hoy/l/3ce7764494c0962ea550913060a8985d3b1f3702a0cf32dad6438bf63b139aa5',
    'San Juan de Miraflores': 'https://weather.com/es-PE/tiempo/hoy/l/d204472727c597cce9c3d53352bab637503d6c1a1788ce30b1f6bbe14c7c7665',
    'Los Olivos': 'https://weather.com/es-PE/tiempo/hoy/l/b8ed58d30d6b2ee4a065fc61aee26f3695e56a8300a635467dba66c79b787cb8',
    'Miraflores': 'https://weather.com/es-PE/tiempo/hoy/l/49f68c035258f9099ea090ccc87ad27e22a4579e28c66a8832e1f0755a6c8b61',
    'San Martin de Porres': 'https://weather.com/es-PE/tiempo/hoy/l/e8f40bcb2cef2fbfe4a578402986ddced0480bcb8f02a2741041eeef8fea6499'
}

# DECLARAMOS LISTA CON PALABRAS CLAVES DE FILTRADO
PALABRAS_CLAVE = ['huelga', 'paro','AFP','sueldo','feriado','sueldo','delincuencia','robo','temblor','lima','apuestas','aumento','Clima','robo','casino','guerra','legales']

# Definir constantes de la aplicación (usadas por app.py y database_connector.py)
# Si SQL_SERVER_CONFIG falla o está incompleto, asignamos valores de error.
if SQL_SERVER_CONFIG and all(SQL_SERVER_CONFIG.values()):
    # Valores específicos proporcionados por el usuario
    TARGET_TABLE = "origen.ocurrencias" # Nombre de la tabla de destino en SQL Server
    KEY_COLUMN = "fecha,hora,distrito,tipo,fuente" # Columna(s) clave para la comparación
else:
    TARGET_TABLE = "ERROR_TABLE_CHECK_CONFIG"
    KEY_COLUMN = "ERROR_ID_CHECK_CONFIG"