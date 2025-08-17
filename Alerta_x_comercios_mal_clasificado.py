#El objetivo de este script es revisar la clasificación de comercios en una base de datos Redshift,
# identificar aquellos que están mal clasificados y enviar un correo de alerta con los detalles.
# -*- coding: utf-8 -*-
# ═════════════════════════════════════════════════════════════════════════════
# 1) IMPORTS Y CONFIGURACIÓN INICIAL
# ═════════════════════════════════════════════════════════════════════════════
import smtplib
import psycopg2
import pandas as pd
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Dict

# -- Credenciales DB (usar variables de entorno en un entorno real)
CREDENTIALS_DB = {
    'host': 'your-redshift-cluster.amazonaws.com',
    'database': 'your_database',
    'port': 5439,
    'user': 'your_user',
    'password': 'your_password'
}

# -- Configuración Correo (usar variables de entorno en un entorno real)
DATA_MAIL = {
    'sender_email': 'alert_bot@example.com',
    'sender_password': 'your_password',
    'recipient': ['recipient1@example.com', 'recipient2@example.com'],
    'subject': "🚨 Alerta: Comercios Mal Clasificados Detectados"
}

# ═════════════════════════════════════════════════════════════════════════════
# 2) CONSULTA SQL PARA LA VALIDACIÓN
# ═════════════════════════════════════════════════════════════════════════════

QUERY_VALIDACION = """
    SELECT
        sc.rut_comercio,
        sc.clasificacion_final,
        sc.ceco_final,
        sc.segmentacion_final,
        sc.segmentacion_final_general
    FROM
        schema_clientes.segmentacion sc
    LEFT JOIN
        schema_auxiliar.lista_comercios lc 
        ON sc.rut_comercio = lc.rut_comercio
    WHERE
        lc.rut_comercio IS NOT NULL
    AND
        sc.clasificacion_final <> 'Clasificación Correcta Esperada'
    GROUP BY
        sc.rut_comercio,
        sc.clasificacion_final,
        sc.ceco_final,
        sc.segmentacion_final,
        sc.segmentacion_final_general
    ORDER BY
        sc.rut_comercio;
"""

# ═════════════════════════════════════════════════════════════════════════════
# 3) FUNCIONES DE UTILIDAD
# ═════════════════════════════════════════════════════════════════════════════

def connect_db(credentials: Dict) -> psycopg2.extensions.connection:
    """Establece conexión con la base de datos Redshift."""
    try:
        conn = psycopg2.connect(**credentials)
        print("✓ Conexión exitosa a la base de datos.")
        return conn
    except psycopg2.OperationalError as e:
        raise SystemExit(f"❌ Error fatal al conectar a la base de datos: {e}")

def close_db_connection(connection: psycopg2.extensions.connection):
    """Cierra la conexión a la base de datos."""
    if connection:
        connection.close()
        print("✓ Conexión a la base de datos cerrada.")

def send_alert_email(df_incorrectos: pd.DataFrame):
    """
    Envía un correo de alerta mostrando los comercios incorrectos
    y luego la configuración correcta esperada.
    """
    msg = MIMEMultipart("related")
    msg["From"] = DATA_MAIL["sender_email"]
    msg["To"] = ", ".join(DATA_MAIL["recipient"])
    msg["Subject"] = DATA_MAIL["subject"]

    # --- TABLA 1: Comercios con datos incorrectos ---
    df_reporte_incorrectos = df_incorrectos.rename(columns={
        'rut_comercio': 'RUT Comercio',
        'clasificacion_final': 'Clasificación Final',
        'ceco_final': 'CECO Final',
        'segmentacion_final': 'Segmentación Final',
        'segmentacion_final_general': 'Segmentación General'
    })

    # --- TABLA 2: Configuración esperada ---
    datos_correctos = {
        'Clasificación Final': ['Clasificación Correcta Esperada'],
        'CECO Final': [00000],
        'Segmentación Final': ['SEGMENTO X'],
        'Segmentación General': ['GENERAL']
    }
    df_reporte_correctos = pd.DataFrame(datos_correctos)

    # --- Cuerpo del Correo ---
    cuerpo_html = f"""
    <p>Estimados,</p>
    <p>Se han detectado comercios que presentan una clasificación incorrecta.</p>
    <hr>
    <h3>Casos Detectados con Clasificación Incorrecta ❌</h3>
    {df_reporte_incorrectos.to_html(index=False, border=0)}
    <br>
    <hr>
    <h3>Configuración Correcta Esperada ✅</h3>
    {df_reporte_correctos.to_html(index=False, border=0)}
    <br>
    <hr>
    <p>Se recomienda revisar y corregir la clasificación de los comercios listados.</p>
    <p>Saludos,<br>Bot de Revisión Automática.</p>
    """
    
    msg.attach(MIMEText(cuerpo_html, "html", "utf-8"))
    
    try:
        with smtplib.SMTP("smtp.office365.com", 587) as srv:
            srv.starttls()
            srv.login(DATA_MAIL["sender_email"], DATA_MAIL["sender_password"])
            srv.sendmail(DATA_MAIL["sender_email"], DATA_MAIL["recipient"], msg.as_string())
        print(f"✉️ Correo de alerta enviado exitosamente a: {', '.join(DATA_MAIL['recipient'])}")
    except Exception as e:
        print(f"❌ Error al enviar el correo: {e}")

# ═════════════════════════════════════════════════════════════════════════════
# 4) LÓGICA PRINCIPAL
# ═════════════════════════════════════════════════════════════════════════════

def main():
    """Función principal que orquesta todo el proceso."""
    print("🚀 Iniciando revisión de clasificación de comercios...")
    conn = None
    try:
        conn = connect_db(CREDENTIALS_DB)
        
        print("⚙️  Ejecutando consulta en la base de datos...")
        df_incorrectos = pd.read_sql(QUERY_VALIDACION, conn)
        
        if not df_incorrectos.empty:
            print(f"🚨 ¡Alerta! Se encontraron {len(df_incorrectos)} comercios mal clasificados.")
            send_alert_email(df_incorrectos)
        else:
            print("✅ ¡Perfecto! No se encontraron comercios mal clasificados.")

    except Exception as e:
        print(f"❌ Ocurrió un error inesperado durante la ejecución: {e}")
    finally:
        if conn:
            close_db_connection(conn)
        print("🏁 Proceso finalizado.")

if __name__ == "__main__":
    main()
