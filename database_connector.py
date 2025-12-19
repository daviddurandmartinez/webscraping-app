from sqlalchemy import create_engine, text
from config import SQL_SERVER_CONFIG, TARGET_TABLE, KEY_COLUMN
import urllib
import pandas as pd

def create_sqlalchemy_engine():
    if not SQL_SERVER_CONFIG or not all(SQL_SERVER_CONFIG.values()):
        return None
    
    driver_name = SQL_SERVER_CONFIG["DRIVER"].strip('{}')
    username = SQL_SERVER_CONFIG["USER"]
    server = SQL_SERVER_CONFIG["SERVER"]
    database = SQL_SERVER_CONFIG["DATABASE"]
    password = urllib.parse.quote_plus(SQL_SERVER_CONFIG["PASSWORD"])
    
    conn_str = (f'mssql+pyodbc://{username}:{password}@{server}/{database}?driver={driver_name}')
    
    try:
        engine = create_engine(conn_str, pool_recycle=3600, pool_pre_ping=True)
        return engine
    except Exception as e:
        print(f"Error creando el motor: {e}")
        return None

def generate_merge_query(df: pd.DataFrame, table_name: str, staging_table: str, key_columns_str: str) -> str:

    keys = [k.strip() for k in key_columns_str.split(',')]
    on_clause = " AND ".join([f"TARGET.[{k}] = SOURCE.[{k}]" for k in keys])
    
    update_cols = [col for col in df.columns if col not in keys]
    set_clauses = ", ".join([f"TARGET.[{col}] = SOURCE.[{col}]" for col in update_cols])
    
    columns_list = ", ".join([f"[{col}]" for col in df.columns])
    values_list = ", ".join([f"SOURCE.[{col}]" for col in df.columns])
    
    # IMPORTANTE: Se añade el esquema 'origen' explícitamente a la tabla SOURCE
    merge_sql = f"""
    MERGE INTO {table_name} AS TARGET
    USING origen.{staging_table} AS SOURCE 
    ON ({on_clause})
    WHEN MATCHED THEN
        UPDATE SET {set_clauses}
    WHEN NOT MATCHED BY TARGET THEN
        INSERT ({columns_list}) VALUES ({values_list});
    """
    return merge_sql

def run_upsert_process(df_temporal: pd.DataFrame, engine):
    if df_temporal.empty:
        return False, "El DataFrame está vacío."
        
    # NUEVO: Forzar conversión a Datetime Esto soluciona el error 22007 al enviar objetos fecha nativos
    df_temporal['fecha'] = pd.to_datetime(df_temporal['fecha'], errors='coerce').dt.date 
    df_temporal['actualizacion'] = pd.to_datetime(df_temporal['actualizacion'], errors='coerce') 
    # Limpiar posibles errores de conversión (filas con fechas nulas)
    df_temporal = df_temporal.dropna(subset=['fecha', 'actualizacion']) 

    staging_table_name = "stg_ocurrencias" 
    try:
        with engine.begin() as connection:
            # 1. Cargar a staging
            df_temporal.to_sql(
                name=staging_table_name, 
                con=connection, 
                if_exists='replace', 
                index=False,
                schema="origen"
            ) 
            
            # 2. Ejecutar MERGE
            merge_query = generate_merge_query(df_temporal, TARGET_TABLE, staging_table_name, KEY_COLUMN) 
            connection.execute(text(merge_query))
            
            # 3. Limpiar
            connection.execute(text(f"DROP TABLE origen.{staging_table_name}"))
            
            return True, "Datos sincronizados exitosamente con SQL Server."
    except Exception as e:
        return False, f"Error durante el proceso: {e}"