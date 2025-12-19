import pandas as pd
import pyodbc

# Configuración de la conexión a SQL Server
conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};'
                      'SERVER=172.16.0.137;'
                      'DATABASE=global;'
                      'UID=produccion;'
                      'PWD=samcorp$246')

# Consulta SQL
query = "SELECT * FROM base.maestra_empresa_casino"

# Cargar los datos en un DataFrame de Pandas
df = pd.read_sql(query, conn)

# Cerrar la conexión
conn.close()

# Ver las primeras filas del DataFrame
print(df.head())