# El objetivo de este script es revisar descuadres entre una fuente de datos creada y fuentes originales,
# identificar discrepancias y enviar un correo de alerta con los detalles.
# -*- coding: utf-8 -*-
# ═════════════════════════════════════════════════════════════════════════════
# 1) IMPORTS
# ═════════════════════════════════════════════════════════════════════════════
import smtplib
import psycopg2
import pandas as pd
from datetime import datetime
from typing import Dict, List
from psycopg2 import OperationalError
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

# ═════════════════════════════════════════════════════════════════════════════
# 2) CONFIGURACIÓN GENERAL
# ═════════════════════════════════════════════════════════════════════════════
HOY = datetime.now().strftime("%Y-%m-%d")

# Credenciales para la conexión a la base de datos (anonimizadas)
DB_CONFIG = {
    'host': 'your-redshift-cluster.amazonaws.com',
    'database': 'your_database',
    'port': 5439,
    'user': 'your_user',
    'password': 'your_password'
}

# Configuración del correo de alerta (anonimizada)
MAIL_CONFIG: Dict[str, object] = {
    'sender_email':    'noreply@yourdomain.com',
    'sender_password': 'your_smtp_password',
    'recipient':       ['user1@yourdomain.com', 'user2@yourdomain.com'],
    'recipient_cc':    [],
    'subject':         f'ALERTA: Discrepancias detectadas - {HOY}',
    'cuerpo_intro':    "<p>Estimados,</p><p>Se detectaron diferencias en las validaciones automáticas. A continuación, se detallan las inconsistencias:</p>",
    'cuerpo_cierre':   ("<br><p>Favor revisar la causa de estas diferencias.<br>Saludos,<br>Equipo de Monitoreo.</p><br>"
                        "Nota: Este correo fue generado automáticamente, favor no responder."),
}

# Query SQL de validación (placeholder, ajusta según tu caso)
SQL_VALIDACION = """
WITH fuente_creada AS (
    SELECT
        fuente,
        to_char(fecha_tx, 'YYYY-MM') AS periodo,
        sum(monto_venta) AS venta,
        sum(cant_trx) AS trx
    FROM your_schema.your_consolidated_table
    WHERE fecha_tx >= '2025-01-01'::timestamp
    GROUP BY periodo, fuente
),
fuentes_originales AS (
    -- Ejemplo de fuente 1
    SELECT
        'SOURCE_A' AS fuente,
        to_char(fecha_evento, 'yyyy-mm') AS periodo,
        round(sum(monto)/1000) AS venta,
        count(id_tx) AS trx
    FROM your_schema.source_a
    WHERE fecha_evento >= '2025-01-01'::timestamp
    GROUP BY to_char(fecha_evento, 'yyyy-mm')

    UNION ALL

    -- Ejemplo de fuente 2
    SELECT
        'SOURCE_B' AS fuente,
        to_char(fecha_evento, 'yyyy-mm') AS periodo,
        round(sum(monto)/1000) AS venta,
        count(id_tx) AS trx
    FROM your_schema.source_b
    WHERE fecha_evento >= '2025-01-01'::timestamp
    GROUP BY periodo
)
-- Comparación final
SELECT
    COALESCE(c.periodo, o.periodo) AS periodo,
    COALESCE(c.fuente, o.fuente) AS fuente,
    COALESCE(c.venta, 0) AS venta_creada,
    COALESCE(c.trx, 0) AS trx_creada,
    COALESCE(o.venta, 0) AS venta_original,
    COALESCE(o.trx, 0) AS trx_original,
    (COALESCE(o.venta, 0) - COALESCE(c.venta, 0)) AS diferencia_venta,
    (COALESCE(o.trx, 0) - COALESCE(c.trx, 0)) AS diferencia_trx
FROM fuente_creada c
FULL OUTER JOIN fuentes_originales o 
    ON c.periodo = o.periodo AND c.fuente = o.fuente
WHERE (COALESCE(o.trx, 0) - COALESCE(c.trx, 0)) <> 0
ORDER BY periodo, fuente;
"""

# ═════════════════════════════════════════════════════════════════════════════
# 3) UTILIDADES – DB & CORREO
# ═════════════════════════════════════════════════════════════════════════════

def connect_db(cfg):
    """Conecta a Amazon Redshift y devuelve (conn, cursor)."""
    try:
        conn = psycopg2.connect(**cfg)
        print("✓ Conectado a Redshift")
        return conn, conn.cursor()
    except OperationalError as e:
        raise SystemExit(f"❌ No se pudo conectar: {e.pgerror or e}")

def query_df(conn, sql) -> pd.DataFrame:
    """Ejecuta una consulta SQL y la devuelve como DataFrame."""
    return pd.read_sql(sql, conn)

def close_db(conn, cur):
    if cur:  cur.close()
    if conn: conn.close(); print("✓ Conexión a Redshift cerrada.")

def dataframe_to_html(df: pd.DataFrame) -> str:
    """Convierte un DataFrame de pandas a una tabla HTML con estilos."""
    header_style = "background:#002B49; color:#fff; padding:8px; border:1px solid #ddd; text-align:left;"
    cell_style = "padding:8px; border:1px solid #ddd; text-align:left;"
    
    html = '<table style="border-collapse:collapse; width:100%; font-family:Arial,sans-serif;">'
    html += '<thead><tr>'
    for col in df.columns:
        html += f'<th style="{header_style}">{col}</th>'
    html += '</tr></thead><tbody>'
    for i, row in df.iterrows():
        bg = "#f9f9f9" if i % 2 == 0 else "#ffffff"
        html += f'<tr style="background:{bg}">'
        for col in df.columns:
            html += f'<td style="{cell_style}">{row[col]}</td>'
        html += '</tr>'
    html += '</tbody></table>'
    return html

def build_and_send_mail(df_alertas: pd.DataFrame):
    """Construye y envía el correo de alerta."""
    msg = MIMEMultipart("related")
    msg["From"] = MAIL_CONFIG["sender_email"]
    msg["To"] = ", ".join(MAIL_CONFIG["recipient"])
    if MAIL_CONFIG["recipient_cc"]:
        msg["Cc"] = ", ".join(MAIL_CONFIG["recipient_cc"])
    msg["Subject"] = MAIL_CONFIG["subject"]

    # Cuerpo del correo en HTML
    tabla_html = dataframe_to_html(df_alertas)
    cuerpo_html = MAIL_CONFIG["cuerpo_intro"] + tabla_html + MAIL_CONFIG["cuerpo_cierre"]
    msg.attach(MIMEText(cuerpo_html, "html", "utf-8"))

    recip_all = MAIL_CONFIG["recipient"] + MAIL_CONFIG["recipient_cc"]
    try:
        with smtplib.SMTP("smtp.office365.com", 587) as srv:
            srv.starttls()
            srv.login(MAIL_CONFIG["sender_email"], MAIL_CONFIG["sender_password"])
            srv.sendmail(MAIL_CONFIG["sender_email"], recip_all, msg.as_string())
        print(f"✉️  Correo de alerta enviado a: {', '.join(recip_all)}")
    except Exception as e:
        print(f"❌ Error al enviar el correo: {e}")

# ═════════════════════════════════════════════════════════════════════════════
# 4) FUNCIÓN PRINCIPAL
# ═════════════════════════════════════════════════════════════════════════════
def main():
    """Función principal que orquesta la validación y el envío de alertas."""
    conn, cur = None, None
    try:
        conn, cur = connect_db(DB_CONFIG)
        print("⚙️  Ejecutando consulta de validación...")
        df_alertas = query_df(conn, SQL_VALIDACION)

        if not df_alertas.empty:
            print(f"⚠️  ¡Alerta! Se encontraron {len(df_alertas)} registros con diferencias.")
            build_and_send_mail(df_alertas)
        else:
            print("✅ No se encontraron discrepancias. Todo OK.")
    except Exception as e:
        print(f"❌ Ocurrió un error inesperado en el proceso: {e}")
    finally:
        if conn:
            close_db(conn, cur)

# ═════════════════════════════════════════════════════════════════════════════
# 5) PUNTO DE ENTRADA
# ═════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    main()
