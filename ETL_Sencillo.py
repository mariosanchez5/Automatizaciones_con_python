#El objetivo de este script es realizar un proceso ETL (Extract, Transform, Load) sencillo
# que extrae datos de una base de datos Redshift, los transforma y limpia, y luego los carga de vuelta a Redshift.
# -*- coding: utf-8 -*-

from sqlite3 import OperationalError
import pandas as pd  # type: ignore
import psycopg2  # type: ignore
import numpy as np
from psycopg2.extras import execute_values
from datetime import timedelta

# ===== Credenciales (usar variables de entorno en la práctica) =====
Credenciales_redshift = {
    'host': 'your-redshift-cluster.amazonaws.com',
    'database': 'your_database',
    'port': 5439,
    'user': 'your_user',
    'password': 'your_password'
}

# ===== Funciones de Conexión y Consulta =====
def connect_db(cfg):
    """Establece conexión con la base de datos Redshift."""
    try:
        conn = psycopg2.connect(**cfg)
        print("✓ Conectado a Redshift")
        return conn
    except OperationalError as e:
        raise SystemExit(f"❌ No se pudo conectar: {e.pgerror or e}")

def query_df(conn, sql):
    """Ejecuta una consulta SQL y retorna un DataFrame de Pandas."""
    try:
        return pd.read_sql(sql, conn)
    except Exception as e:
        print(f"⚠️ Error en la consulta: {e}")
        return pd.DataFrame()

# ===== Definición de Consultas (Queries de ejemplo) =====
query1 = """
SELECT 
    liq.tipo_tx,
    CASE 
        WHEN liq.emisor IN ('999999-BCO_X') THEN 'Internacional'
        ELSE 'Nacional'
    END AS nacionalidad_tx,
    a.tarjeta_presente, 
    a.marca, 
    TO_CHAR(a.fecha_tx, 'yyyy-mm-dd') AS fecha_tx,  
    ROUND(SUM(a.monto_total)) AS monto_venta,  
    COUNT(a.tx_codigo) AS cant_trx,  
    SUM(a.col_1) AS costo_1,  
    SUM(a.col_2) AS costo_2,  
    SUM(a.col_3) AS costo_3,  
    SUM(a.col_4) AS costo_4,  
    SUM(a.col_5) AS costo_5,  
    SUM(a.col_6) AS costo_6,  
    SUM(a.col_7) AS costo_7,
    SUM(a.col_8) AS costo_8,  
    SUM(a.col_9) AS costo_9,  
    SUM(a.col_10) AS costo_10
FROM schema_x.tabla_liquidaciones liq
LEFT JOIN schema_x.costos_marcas a ON a.id = liq.id
WHERE a.fecha_tx >= '2025-01-01'::timestamp  
GROUP BY liq.tipo_tx, nacionalidad_tx, a.tarjeta_presente, a.marca, fecha_tx
"""

query2 = """
SELECT 
    nacionalidad_tx, 
    tarjeta_presente,
    tx_codigo_mandante,  
    TO_CHAR(tx_fecha_hora, 'yyyy-mm-dd') AS fecha_tx,  
    ROUND(SUM(tx_monto)/1000) AS monto_venta,
    COUNT(tx_codigo) AS cant_tx,
    ROUND(SUM(col_1 + col_2)) AS costo_1,
    ROUND(SUM(col_3)) AS costo_2,  
    ROUND(SUM(col_4)) AS costo_3,  
    ROUND(SUM(col_5)) AS costo_4,  
    ROUND(SUM(col_6)) AS costo_5,  
    ROUND(SUM(col_7)) AS costo_6
FROM schema_x.costos_rechazadas crv  
WHERE crv.tx_fecha_hora >= '2025-01-01'::timestamp  
GROUP BY nacionalidad_tx, tx_codigo_mandante, fecha_tx, tarjeta_presente
"""

query3 = """
SELECT 
    nacionalidad_tx,
    tarjeta_presente, 
    tx_codigo_mandante,  
    TO_CHAR(fecha_venta, 'yyyy-mm-dd') AS fecha_tx,
    ROUND(SUM(tx_monto)/1000) AS monto_venta,
    COUNT(tx_codigo) AS cant_tx,
    ROUND(SUM(col_1/1000), 2) AS costo_1,  
    ROUND(SUM(col_2/1000), 2) AS costo_2,
    ROUND(SUM(col_3/1000), 2) AS costo_3,  
    ROUND(SUM(col_4/1000), 2) AS costo_4,
    ROUND(SUM(col_5/1000), 2) AS costo_5,  
    ROUND(SUM(col_6/1000), 2) AS costo_6
FROM schema_x.tabla_checkin ac  
WHERE fecha_venta >= '2025-01-01'::timestamp
GROUP BY nacionalidad_tx, tx_codigo_mandante, fecha_tx, tarjeta_presente
"""

# ===== Descarga de Datos desde Redshift =====
print("🚚 Iniciando descarga de datos desde Redshift...")
conn = connect_db(Credenciales_redshift)
df1 = query_df(conn, query1)
df2 = query_df(conn, query2)
df3 = query_df(conn, query3)
conn.close()
print("✓ Datos descargados.")

# ===== Limpieza y Normalización de Datos =====
print("🧹 Procesando y limpiando datos...")
df1 = df1.rename(columns={"marca": "tx_codigo_mandante"})
df1["tipo_tx"] = "VENTA"
df2["tipo_tx"] = "RECHAZOS"
df3["tipo_tx"] = "CHECKIN"

for df in [df1, df2, df3]:
    df["cant_trx"] = df.get("cant_trx", df.get("cant_tx", 0))

required_columns = [
    "tipo_tx", "nacionalidad_tx", "tarjeta_presente", "tx_codigo_mandante", "fecha_tx",
    "monto_venta", "cant_trx",
    "costo_1", "costo_2", "costo_3", "costo_4", "costo_5",
    "costo_6", "costo_7", "costo_8", "costo_9", "costo_10"
]

def completar_columnas(df):
    for col in required_columns:
        if col not in df.columns:
            df[col] = 0
    return df[required_columns]

df1 = completar_columnas(df1)
df2 = completar_columnas(df2)
df3 = completar_columnas(df3)

df1["fuente"] = "ADQ"
df2["fuente"] = "RECHAZOS"
df3["fuente"] = "CHECKIN"

df_final = pd.concat([df1, df2, df3], ignore_index=True)

# ===== Lógica de Nuevas Columnas de Fechas =====
df_final["fecha_tx"] = pd.to_datetime(df_final["fecha_tx"])

# Calcular billing_date (próximo domingo)
weekday = df_final["fecha_tx"].dt.weekday
days_to_sunday = (6 - weekday + 7) % 7
df_final["billing_date"] = df_final["fecha_tx"] + pd.to_timedelta(days_to_sunday, unit="D")

df_final["fecha_contable"] = df_final["fecha_tx"]
df_final["fecha_visa"] = df_final["fecha_tx"].dt.month

# ===== Cálculo adicional de billing_date_2 =====
def calcular_billing_date_2(fecha_tx):
    fecha_tx = pd.to_datetime(fecha_tx)
    weekday = fecha_tx.weekday()
    if weekday >= 4:
        dias_hasta_jueves = (3 - weekday + 7) % 7
    else:
        dias_hasta_jueves = (3 - weekday)
    jueves_objetivo = fecha_tx + timedelta(days=dias_hasta_jueves)
    domingo_post_jueves = jueves_objetivo + timedelta(days=3)
    return domingo_post_jueves.normalize()

df_final["billing_date_2"] = df_final["fecha_tx"].apply(calcular_billing_date_2)

# ===== Carga de Datos a Redshift =====
conn, cur = None, None
try:
    conn = connect_db(Credenciales_redshift)
    cur = conn.cursor()
    
    print("🗑️  Limpiando la tabla destino schema_x.tabla_destino...")
    cur.execute("DELETE FROM schema_x.tabla_destino;")
    print("✓ Tabla limpiada.")
    
    df_ins = df_final.astype(object).where(pd.notnull(df_final), None)
    
    cols = ", ".join(df_ins.columns)
    insert_sql = f"INSERT INTO schema_x.tabla_destino ({cols}) VALUES %s"
    
    print(f"📦 Insertando {len(df_ins)} registros...")
    execute_values(
        cur,
        insert_sql,
        df_ins.to_records(index=False).tolist(),
        page_size=1000
    )
    
    conn.commit()
    print("✅ ¡Éxito! Datos insertados correctamente en tabla destino.")

except Exception as e:
    print(f"❌ Error durante la carga a Redshift: {e}")
    if conn:
        conn.rollback()
finally:
    if cur:
        cur.close()
    if conn:
        conn.close()
    print("🚪 Conexión a Redshift cerrada.")
