#El objetivo de este script es revisar el campo "objetivo" de transacciones en una base de datos Redshift,
# identificar aquellos registros con valores nulos o menores a 1, y enviar un correo de alerta con los detalles.
# -*- coding: utf-8 -*-
# ═════════════════════════════════════════════════════════════════════════════
# 1) IMPORTS Y CONFIGURACIÓN INICIAL
# ═════════════════════════════════════════════════════════════════════════════
import smtplib
import psycopg2
import pandas as pd
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Dict, List

# -- Configuración de fecha dinámica
hoy = datetime.now()
nombres_meses_es = [
    "Enero","Febrero","Marzo","Abril","Mayo","Junio",
    "Julio","Agosto","Septiembre","Octubre","Noviembre","Diciembre"
]
CONFIG_MES = {'nombre': nombres_meses_es[hoy.month - 1], 'numero': hoy.month, 'ano': hoy.year}

# -- Para evaluar un mes específico completo, comentar la línea de arriba
# CONFIG_MES = {'nombre': 'Junio', 'numero': 6, 'ano': 2025}


# -- Credenciales DB (anonimizadas)
CREDENTIALS_DB = {
    'host': 'xxxx-redshift-cluster.amazonaws.com',
    'database': 'db_demo', 'port': 5439,
    'user': 'usuario_demo', 'password': '********'
}

# -- Configuración Correo (anonimizada)
DATA_MAIL = {
    'sender_email': 'alertas@demo.com', 'sender_password': '********',
    'recipient': ['persona1@demo.com', 'persona2@demo.com'],
    'subject': f"🚨 Alerta: Objetivo Inválido Detectado - {CONFIG_MES['nombre']} {CONFIG_MES['ano']}"
}

# ═════════════════════════════════════════════════════════════════════════════
# 2) CONSTANTES PARA LA CONSULTA
# ═════════════════════════════════════════════════════════════════════════════

# Lista de identificadores a validar (anonimizados)
IDS_A_VALIDAR = [
    '11111111-1','22222222-2','33333333-3','44444444-4','55555555-5'
]

# Tipos de transacción a ser excluidos (anonimizados)
TIPOS_TX_A_EXCLUIR = [
    'ANULACION_TIPO_A','ANULACION_TIPO_B','REVERSA_TIPO_C',
    'CONTRACARGO_TIPO_A','CONTRACARGO_TIPO_B'
]

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

def send_alert_email(df_nulos: pd.DataFrame, df_menores: pd.DataFrame):
    """Envía un correo de alerta con los datos encontrados, separados por categoría."""
    msg = MIMEMultipart("related")
    msg["From"] = DATA_MAIL["sender_email"]
    msg["To"] = ", ".join(DATA_MAIL["recipient"])
    msg["Subject"] = DATA_MAIL["subject"]
    
    # Inicia el cuerpo del correo
    cuerpo_html = f"""
    <p>Estimados,</p>
    <p>Se detectaron problemas con el <b>Objetivo</b> para transacciones en el periodo de <b>{CONFIG_MES['nombre']} {CONFIG_MES['ano']}</b>.</p>
    <p>A continuación, se detallan los casos encontrados:</p>
    """
    
    # --- SECCIÓN 1: Objetivo NULO ---
    if not df_nulos.empty:
        df_reporte_nulos = df_nulos.rename(columns={
            'id_comercio': 'ID Comercio', 'periodo': 'Periodo',
            'objetivo': 'Objetivo', 'cantidad_tx': 'Cantidad de TX'
        })
        cuerpo_html += """
        <hr>
        <h3>Casos con Objetivo Nulo</h3>
        <p>Estos registros no tienen un valor asignado en el campo <code>objetivo</code>.</p>
        """
        cuerpo_html += df_reporte_nulos.to_html(index=False, border=0, na_rep='NULO')
        cuerpo_html += "<br>"

    # --- SECCIÓN 2: Objetivo MENOR A 1 ---
    if not df_menores.empty:
        df_reporte_menores = df_menores.rename(columns={
            'id_comercio': 'ID Comercio', 'periodo': 'Periodo',
            'objetivo': 'Objetivo', 'cantidad_tx': 'Cantidad de TX'
        })
        cuerpo_html += """
        <hr>
        <h3>Casos con Objetivo Menor a 1</h3>
        <p>Estos registros tienen un valor incorrecto en <code>objetivo</code> (menor a 1).</p>
        """
        cuerpo_html += df_reporte_menores.to_html(index=False, border=0)

    # Cierre del correo
    cuerpo_html += """
    <hr>
    <p>Saludos,<br>Bot de Revisión Automática.</p>
    """
    
    msg.attach(MIMEText(cuerpo_html, "html", "utf-8"))
    
    try:
        with smtplib.SMTP("smtp.demo.com", 587) as srv:
            srv.starttls()
            srv.login(DATA_MAIL["sender_email"], DATA_MAIL["sender_password"])
            srv.sendmail(DATA_MAIL["sender_email"], DATA_MAIL["recipient"], msg.as_string())
        print(f"✉️ Correo de alerta enviado exitosamente a: {', '.join(DATA_MAIL['recipient'])}")
    except Exception as e:
        print(f"❌ Error al enviar el correo: {e}")

# ═════════════════════════════════════════════════════════════════════════════
# 4) LÓGICA PRINCIPAL
# ═════════════════════════════════════════════════════════════════════════════

def construir_query_revision(mes: int, ano: int, ids: List[str], tx_excluidas: List[str]) -> str:
    """Construye la consulta SQL dinámicamente."""
    start_date = f"{ano}-{mes:02d}-01"
    hoy = datetime.now()
    if ano == hoy.year and mes == hoy.month:
        end_date_obj = hoy + timedelta(days=1)
        end_date = end_date_obj.strftime('%Y-%m-%d')
    else:
        next_month, next_year = (mes % 12) + 1, ano + (mes // 12)
        end_date = f"{next_year}-{next_month:02d}-01"

    ids_sql = ", ".join([f"'{r}'" for r in ids])
    tx_sql = ", ".join([f"'{tx}'" for tx in tx_excluidas])

    query = f"""
        SELECT 
            al.id_comercio,
            TO_CHAR(al.fecha_tx, 'yyyy-mm-dd') AS periodo,
            SUM(rc.objetivo) AS objetivo,
            COUNT(al.codigo_tx) AS cantidad_tx
        FROM schema_demo.transacciones al
        LEFT JOIN schema_demo.objetivos rc ON (al.id = rc.id)
        WHERE al.id_comercio IN ({ids_sql})
          AND al.tipo_tx NOT IN ({tx_sql})
          AND al.fecha_tx >= '{start_date}'::timestamp
          AND al.fecha_tx < '{end_date}'::timestamp
          AND (rc.objetivo IS NULL OR rc.objetivo < 1)
        GROUP BY al.id_comercio, periodo
        ORDER BY periodo;
    """
    return query

def main():
    """Función principal que orquesta todo el proceso."""
    print(f"🚀 Iniciando revisión de Objetivo para {CONFIG_MES['nombre']} de {CONFIG_MES['ano']}...")
    conn = None
    try:
        conn = connect_db(CREDENTIALS_DB)
        
        query = construir_query_revision(
            mes=CONFIG_MES['numero'],
            ano=CONFIG_MES['ano'],
            ids=IDS_A_VALIDAR,
            tx_excluidas=TIPOS_TX_A_EXCLUIR
        )
        
        print("⚙️  Ejecutando consulta en la base de datos...")
        df_resultados = pd.read_sql(query, conn)
        
        if not df_resultados.empty:
            print(f"🚨 ¡Alerta! Se encontraron {len(df_resultados)} registros con Objetivo inválido.")
            
            # Separar el DataFrame en dos según la condición
            df_nulos = df_resultados[df_resultados['objetivo'].isnull()].copy()
            df_menores_a_uno = df_resultados[df_resultados['objetivo'].notnull()].copy()
            
            # Imprimir resumen
            if not df_nulos.empty:
                print(f"  - {len(df_nulos)} casos con Objetivo NULO.")
            if not df_menores_a_uno.empty:
                print(f"  - {len(df_menores_a_uno)} casos con Objetivo MENOR A 1.")

            # Enviar reporte por correo
            send_alert_email(df_nulos, df_menores_a_uno)
        else:
            print("✅ ¡Perfecto! No se encontraron registros con Objetivo inválido.")

    except Exception as e:
        print(f"❌ Ocurrió un error inesperado durante la ejecución: {e}")
    finally:
        if conn:
            close_db_connection(conn)
        print("🏁 Proceso finalizado.")

if __name__ == "__main__":
    main()
