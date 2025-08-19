#El objetivo de este script es realizar validaciones de tarifas en una base de datos Redshift,
#identificar discrepancias entre tarifas aplicadas y tarifas teóricas, y enviar un correo de alerta con los detalles.
#Ocupando herramientas como S3, Glue, Redshift y Pandas.
#!/usr/bin/env python3
"""
# Validador de Tarifas – Script de Alerta
• Lee la BO desde S3 (toma el CSV más reciente del prefijo dado).
• Ejecuta dos consultas en Redshift: Día (última fecha) y MTD (mes hasta la fecha).
• Cruza, calcula diferencias y clasifica errores (excluye NO Error 6: MCC 4511 – MD particular).
• Envía por correo un resumen HTML tabular (con líneas) con columnas MTD
  y adjunta **dos CSV**: detalle diario y detalle acumulado MTD.

Dependencias: boto3, pandas, numpy, psycopg2-binary
"""
import io
import os
import sys
import boto3
import smtplib
import traceback
import numpy as np
import pandas as pd
import psycopg2
from typing import Tuple, Optional, List
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders

# ===============================
# ⚙️ CONFIGURACIÓN EN LÍNEA
# ===============================
# Redshift
DB_HOST = "your-redshift-cluster.region.redshift.amazonaws.com"
DB_NAME = "your_database_name"
DB_USER = "your_db_user"
DB_PASSWORD = "YOUR_SECURE_PASSWORD"
DB_PORT = 5439

# S3 (prefijo de entrada con los CSV de BO)
S3_INPUT = "s3://your-company-datalake/path/to/input/files/"

# Correo (Office365 o similar)
SMTP_HOST = "smtp.your-email-provider.com"
SMTP_PORT = 587
MAIL_SENDER = "automation-sender@yourcompany.com"
MAIL_PASSWORD = "YOUR_EMAIL_APP_PASSWORD"
MAIL_RECIPIENTS = [
    "recipient.one@example.com",
    "recipient.two@example.com",
    "team.alias@example.com",
]

# ===============================
# 🔧 UTILIDADES S3
# ===============================
s3 = boto3.client("s3")

def parse_s3_url(url: str) -> Tuple[str, str]:
    if not url.startswith("s3://"):
        raise ValueError("URL S3 inválida. Debe empezar con s3://")
    path = url[5:]
    parts = path.split("/", 1)
    bucket = parts[0]
    prefix = parts[1] if len(parts) > 1 else ""
    if prefix and not prefix.endswith("/"):
        prefix += "/"
    return bucket, prefix


def get_latest_object(bucket: str, prefix: str, suffixes: Tuple[str, ...] = (".csv", ".CSV")) -> Optional[str]:
    """Retorna la key más reciente (LastModified) que termine en .csv dentro del prefijo."""
    paginator = s3.get_paginator("list_objects_v2")
    latest_key, latest_time = None, None
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.lower().endswith(tuple(s.lower() for s in suffixes)):
                continue
            lm = obj["LastModified"]
            if latest_time is None or lm > latest_time:
                latest_time = lm
                latest_key = key
    return latest_key


def read_csv_from_s3(bucket: str, key: str) -> pd.DataFrame:
    """Lee CSV de S3 a pandas, probando UTF-8-SIG y UTF-8."""
    obj = s3.get_object(Bucket=bucket, Key=key)
    raw = obj["Body"].read()
    for enc in ("utf-8-sig", "utf-8"):
        try:
            return pd.read_csv(io.BytesIO(raw), encoding=enc)
        except UnicodeDecodeError:
            continue
    return pd.read_csv(io.BytesIO(raw))

# ===============================
# 🛢️ CONEXIÓN REDSHIFT
# ===============================
def connect_redshift() -> psycopg2.extensions.connection:
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            port=DB_PORT,
        )
        print("✓ Conexión a Redshift OK")
        return conn
    except psycopg2.OperationalError as e:
        raise SystemExit(f"❌ Error al conectar a Redshift: {e}")


RED_SHIFT_QUERY = """
-- =========================================================================================
-- NOTA DE ANONIMIZACIÓN: Los nombres de esquemas, tablas y campos han sido reemplazados
-- por nombres genéricos para proteger la confidencialidad del negocio.
-- La lógica y estructura de la consulta se mantienen intactas.
-- =========================================================================================
WITH filtered_settlements AS (
    SELECT *
    FROM analytics_schema.settlements_table
    WHERE CAST(settlement_date AS DATE) = (
        SELECT MAX(CAST(settlement_date AS DATE)) FROM analytics_schema.settlements_table
    )
    AND (
        merchant_id IN ('11111111-1', '22222222-2', '33333333-3')
        OR (merchant_id = '99999999-9' AND store_id IN ('100001', '100002', '100003'))
    )
),
enriched_transactions AS (
    SELECT st.*, f.transaction_id, f.card_present_flag, f.mcc_code AS original_mcc, f.transaction_origin, f.fee_percentage
    FROM filtered_settlements AS st
    LEFT JOIN analytics_schema.transactions_fact_table AS f ON st.transaction_code = f.transaction_code
),
corrected_mcc_map AS (
    SELECT transaction_id, COALESCE(MAX(NULLIF(mcc_code, '0')), '0') AS mcc_code_fix
    FROM analytics_schema.transactions_fact_table
    WHERE transaction_id IN (SELECT DISTINCT transaction_id FROM enriched_transactions WHERE transaction_id IS NOT NULL)
    GROUP BY transaction_id
),
corrected_origin_map AS (
    SELECT transaction_id, COALESCE(MAX(NULLIF(transaction_origin, '-')), '-') AS origin_fix
    FROM analytics_schema.transactions_fact_table
    WHERE transaction_id IN (SELECT DISTINCT transaction_id FROM enriched_transactions WHERE transaction_id IS NOT NULL)
    GROUP BY transaction_id
),
theoretical_fee_map AS (
    SELECT transaction_id, MAX(fee_percentage) AS theoretical_fee_max
    FROM analytics_schema.transactions_fact_table
    WHERE transaction_id IN (SELECT DISTINCT transaction_id FROM enriched_transactions WHERE transaction_id IS NOT NULL)
    GROUP BY transaction_id
),
initial_calculation AS (
    SELECT
        et.*,
        cm.mcc_code_fix,
        CASE
            WHEN et.transaction_type LIKE 'ANULACION%' AND et.variable_fee_rate <> 0
            THEN tfm.theoretical_fee_max
            ELSE NULL
        END AS theoretical_fee_lookup,
        CASE
            WHEN et.card_brand = 'AMEX' THEN CASE WHEN et.issuer_name = '999999-BCO_GENERICO' THEN 'Internacional' ELSE 'Nacional' END
            ELSE et.transaction_origin
        END AS origin_plan_a
    FROM enriched_transactions AS et
    LEFT JOIN corrected_mcc_map AS cm ON et.transaction_id = cm.transaction_id
    LEFT JOIN theoretical_fee_map AS tfm ON et.transaction_id = tfm.transaction_id
)
SELECT
    ic.card_present_flag,
    CASE WHEN ic.transaction_type LIKE 'ANULACION%' THEN ic.mcc_code_fix ELSE ic.original_mcc END AS mcc_code_corrected,
    COUNT(DISTINCT(ic.transaction_code)) AS trx_count,
    SUM(ic.gross_amount) AS sales_volume,
    SUM(ROUND(ic.commission_amount / 1.19)) AS total_fee,
    ic.applied_exchange_rate,
    ic.merchant_id,
    TO_CHAR(ic.transaction_date, 'YYYY-MM-DD') AS trx_date,
    ic.card_brand,
    CASE WHEN ic.origin_plan_a = '-' THEN com.origin_fix ELSE ic.origin_plan_a END AS transaction_origin,
    CASE WHEN (CASE WHEN ic.origin_plan_a = '-' THEN com.origin_fix ELSE ic.origin_plan_a END) = 'Internacional' THEN 'INTERNACIONAL' ELSE ic.product_category END AS category,
    ic.transaction_type,
    ic.is_installment,
    (ic.variable_fee_rate * 100) AS applied_var_fee,
    ic.fixed_fee_rate AS applied_fixed_fee,
    ic.theoretical_fee_lookup,
    LEFT(ic.merchant_id, LENGTH(ic.merchant_id) - 2)
        || '-' || (CASE WHEN ic.transaction_type LIKE 'ANULACION%' THEN ic.mcc_code_fix ELSE ic.original_mcc END)
        || '-' || ic.card_brand
        || '-' || (CASE WHEN (CASE WHEN ic.origin_plan_a = '-' THEN com.origin_fix ELSE ic.origin_plan_a END) = 'Internacional' THEN 'INTERNACIONAL' ELSE ic.product_category END) AS join_key
FROM initial_calculation AS ic
LEFT JOIN corrected_origin_map AS com ON ic.transaction_id = com.transaction_id
GROUP BY 1, 2, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16
LIMIT 1000000;
"""

RED_SHIFT_QUERY_MTD = """
-- =========================================================================================
-- NOTA DE ANONIMIZACIÓN: Los nombres de esquemas, tablas y campos han sido reemplazados
-- por nombres genéricos para proteger la confidencialidad del negocio.
-- La lógica y estructura de la consulta se mantienen intactas.
-- =========================================================================================
WITH max_date AS (
    SELECT MAX(CAST(settlement_date AS DATE)) AS ld FROM analytics_schema.settlements_table
),
filtered_settlements AS (
    SELECT st.*
    FROM analytics_schema.settlements_table st
    JOIN max_date ON 1=1
    WHERE CAST(st.settlement_date AS DATE) BETWEEN DATE_TRUNC('month', ld)::date AND ld
    AND (
        merchant_id IN ('11111111-1', '22222222-2', '33333333-3')
        OR (merchant_id = '99999999-9' AND store_id IN ('100001', '100002', '100003'))
    )
),
enriched_transactions AS (
    SELECT st.*, f.transaction_id, f.card_present_flag, f.mcc_code AS original_mcc, f.transaction_origin, f.fee_percentage
    FROM filtered_settlements AS st
    LEFT JOIN analytics_schema.transactions_fact_table AS f ON st.transaction_code = f.transaction_code
),
corrected_mcc_map AS (
    SELECT transaction_id, COALESCE(MAX(NULLIF(mcc_code, '0')), '0') AS mcc_code_fix
    FROM analytics_schema.transactions_fact_table
    WHERE transaction_id IN (SELECT DISTINCT transaction_id FROM enriched_transactions WHERE transaction_id IS NOT NULL)
    GROUP BY transaction_id
),
corrected_origin_map AS (
    SELECT transaction_id, COALESCE(MAX(NULLIF(transaction_origin, '-')), '-') AS origin_fix
    FROM analytics_schema.transactions_fact_table
    WHERE transaction_id IN (SELECT DISTINCT transaction_id FROM enriched_transactions WHERE transaction_id IS NOT NULL)
    GROUP BY transaction_id
),
theoretical_fee_map AS (
    SELECT transaction_id, MAX(fee_percentage) AS theoretical_fee_max
    FROM analytics_schema.transactions_fact_table
    WHERE transaction_id IN (SELECT DISTINCT transaction_id FROM enriched_transactions WHERE transaction_id IS NOT NULL)
    GROUP BY transaction_id
),
initial_calculation AS (
    SELECT
        et.*,
        cm.mcc_code_fix,
        CASE
            WHEN et.transaction_type LIKE 'ANULACION%' AND et.variable_fee_rate <> 0
            THEN tfm.theoretical_fee_max
            ELSE NULL
        END AS theoretical_fee_lookup,
        CASE
            WHEN et.card_brand = 'AMEX' THEN CASE WHEN et.issuer_name = '999999-BCO_GENERICO' THEN 'Internacional' ELSE 'Nacional' END
            ELSE et.transaction_origin
        END AS origin_plan_a
    FROM enriched_transactions AS et
    LEFT JOIN corrected_mcc_map AS cm ON et.transaction_id = cm.transaction_id
    LEFT JOIN theoretical_fee_map AS tfm ON et.transaction_id = tfm.transaction_id
)
SELECT
    ic.card_present_flag,
    CASE WHEN ic.transaction_type LIKE 'ANULACION%' THEN ic.mcc_code_fix ELSE ic.original_mcc END AS mcc_code_corrected,
    COUNT(DISTINCT(ic.transaction_code)) AS trx_count,
    SUM(ic.gross_amount) AS sales_volume,
    SUM(ROUND(ic.commission_amount / 1.19)) AS total_fee,
    ic.applied_exchange_rate,
    ic.merchant_id,
    TO_CHAR(ic.transaction_date, 'YYYY-MM-DD') AS trx_date,
    ic.card_brand,
    CASE WHEN ic.origin_plan_a = '-' THEN com.origin_fix ELSE ic.origin_plan_a END AS transaction_origin,
    CASE WHEN (CASE WHEN ic.origin_plan_a = '-' THEN com.origin_fix ELSE ic.origin_plan_a END) = 'Internacional' THEN 'INTERNACIONAL' ELSE ic.product_category END AS category,
    ic.transaction_type,
    ic.is_installment,
    (ic.variable_fee_rate * 100) AS applied_var_fee,
    ic.fixed_fee_rate AS applied_fixed_fee,
    ic.theoretical_fee_lookup,
    LEFT(ic.merchant_id, LENGTH(ic.merchant_id) - 2)
        || '-' || (CASE WHEN ic.transaction_type LIKE 'ANULACION%' THEN ic.mcc_code_fix ELSE ic.original_mcc END)
        || '-' || ic.card_brand
        || '-' || (CASE WHEN (CASE WHEN ic.origin_plan_a = '-' THEN com.origin_fix ELSE ic.origin_plan_a END) = 'Internacional' THEN 'INTERNACIONAL' ELSE ic.product_category END) AS join_key
FROM initial_calculation AS ic
LEFT JOIN corrected_origin_map AS com ON ic.transaction_id = com.transaction_id
GROUP BY 1, 2, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16
LIMIT 1000000;
"""

# ===============================
# ✉️ CORREO (HTML)
# ===============================
def format_miles(val) -> str:
    try:
        return ("{:,.0f}".format(float(val))).replace(",", ".")
    except Exception:
        return str(val)


def build_html_report(resumen: pd.DataFrame, fecha_min: Optional[pd.Timestamp], fecha_max: Optional[pd.Timestamp]) -> str:
    fecha_rango = "N/D"
    if pd.notna(fecha_min) and pd.notna(fecha_max):
        fecha_rango = f"{fecha_min.strftime('%Y-%m-%d')} a {fecha_max.strftime('%Y-%m-%d')}"

    resumen_html = resumen.to_html(index=False, border=0)
    resumen_html = resumen_html.replace(
        '<table border="0" class="dataframe">',
        '<table border="1" cellpadding="6" cellspacing="0" style="border-collapse:collapse;border:1px solid #cccccc;">'
    )
    resumen_html = resumen_html.replace(
        '<th>',
        '<th style="border:1px solid #cccccc;padding:6px 8px;background:#f6f8fa;text-align:center;">'
    )
    resumen_html = resumen_html.replace(
        '<td>',
        '<td style="border:1px solid #cccccc;padding:6px 8px;">'
    )

    html = f"""
    <html>
    <body style="font-family: Arial, sans-serif;">
        <h2>Resumen de errores de Tarifa ({fecha_rango})</h2>
        <p>Se adjuntan <b>dos archivos CSV</b>: detalle <b>diario</b> y detalle <b>acumulado MTD</b>.</p>
        <h3>Resumen</h3>
        {resumen_html}
        <br/>
        <p style="color:#888">Mensaje generado automáticamente.</p>
    </body>
    </html>
    """
    return html


def send_email(subject: str, html_body: str, recipients: List[str], attachments: Optional[List[tuple]] = None) -> None:
    if not recipients:
        print("[WARN] No hay destinatarios definidos, omitiendo envío de correo.")
        return

    msg = MIMEMultipart("mixed")
    msg["From"] = MAIL_SENDER
    msg["To"] = ", ".join(recipients)
    msg["Subject"] = subject

    alt = MIMEMultipart("alternative")
    alt.attach(MIMEText(html_body, "html", "utf-8"))
    msg.attach(alt)

    if attachments:
        for filename, content_bytes in attachments:
            part = MIMEBase("application", "octet-stream")
            part.set_payload(content_bytes)
            encoders.encode_base64(part)
            part.add_header("Content-Disposition", f'attachment; filename="{filename}"')
            msg.attach(part)

    with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as srv:
        srv.starttls()
        srv.login(MAIL_SENDER, MAIL_PASSWORD)
        srv.sendmail(MAIL_SENDER, recipients, msg.as_string())
    print(f"✉️ Correo enviado a: {', '.join(recipients)}")

# ===============================
# 🧮 LÓGICA DE NEGOCIO
# ===============================
def preparar_bo(df_bo: pd.DataFrame) -> pd.DataFrame:
    required = {
        'merchant_identifier','mcc_code','card_brand','product_category',
        'start_date','end_date','fee_card_present','fee_card_not_present'
    }
    missing = required - set(df_bo.columns)
    if missing:
        raise ValueError(f"BO sin columnas requeridas: {missing}")

    df = df_bo.copy()
    df['merchant_identifier'] = df['merchant_identifier'].astype(str)
    df['mcc_code'] = df['mcc_code'].astype(str)
    df['card_brand'] = df['card_brand'].astype(str)
    df['product_category'] = df['product_category'].astype(str)
    df['join_key'] = df['merchant_identifier'] + '-' + df['mcc_code'] + '-' + df['card_brand'] + '-' + df['product_category']

    fecha_objetivo = pd.Timestamp('2050-12-31')
    df['end_date'] = df['end_date'].replace(['inf','infinity','Inf','Infinity'], fecha_objetivo)

    df['start_date'] = pd.to_datetime(df['start_date'], errors='coerce')
    df['end_date'] = pd.to_datetime(df['end_date'], errors='coerce')
    return df


def procesar(df_liq: pd.DataFrame, df_bo: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    df_liq = df_liq.copy()
    df_liq['trx_date'] = pd.to_datetime(df_liq['trx_date'], errors='coerce')

    df_merged = pd.merge(df_liq, df_bo, on='join_key', how='left')
    df_combinado = df_merged[(df_merged['trx_date'] >= df_merged['start_date']) & (df_merged['trx_date'] <= df_merged['end_date'])].copy()

    md_col = 'theoretical_fee_lookup'
    if md_col not in df_combinado.columns:
        df_combinado[md_col] = np.nan

    df_combinado['fee_comparison'] = np.where(
        ((df_combinado['card_present_flag'] == 'Si') & (df_combinado['applied_var_fee'] == df_combinado['fee_card_present'])) |
        ((df_combinado['card_present_flag'] == 'No') & (df_combinado['applied_var_fee'] == df_combinado['fee_card_not_present'])),
        True, False
    )

    df_combinado.loc[
        (df_combinado['fee_comparison'] == False) &
        (pd.notna(df_combinado[md_col])) &
        (df_combinado['applied_var_fee'] == df_combinado[md_col]),
        'fee_comparison'
    ] = True

    df_combinado['fee_difference'] = np.where(
        df_combinado['card_present_flag'] == 'Si',
        df_combinado['applied_var_fee'] - df_combinado['fee_card_present'],
        df_combinado['applied_var_fee'] - df_combinado['fee_card_not_present']
    )

    cond_base_false = ~(
        ((df_combinado['card_present_flag'] == 'Si') & (df_combinado['applied_var_fee'] == df_combinado['fee_card_present'])) |
        ((df_combinado['card_present_flag'] == 'No') & (df_combinado['applied_var_fee'] == df_combinado['fee_card_not_present']))
    )
    cond_teorico = pd.notna(df_combinado[md_col])
    df_combinado.loc[cond_base_false & cond_teorico, 'fee_difference'] = (
        df_combinado['applied_var_fee'] - df_combinado[md_col]
    )

    sel_cols = [
        'merchant_id','mcc_code_corrected','card_brand','category','transaction_origin','transaction_type',
        'card_present_flag','is_installment','trx_date','start_date','end_date','sales_volume','trx_count','total_fee',
        'applied_var_fee','applied_fixed_fee','fee_card_present','fee_card_not_present', md_col,'fee_comparison','fee_difference'
    ]
    sel_cols = [c for c in sel_cols if c in df_combinado.columns]

    df_final = df_combinado[df_combinado['fee_comparison'] == False][sel_cols].rename(columns={
        'applied_var_fee':'applied_fee_var',
        'applied_fixed_fee':'applied_fee_fixed',
        'fee_card_present':'theoretical_fee_cp',
        'fee_card_not_present':'theoretical_fee_cnp',
        md_col:'theoretical_fee_lookup'
    })

    df_final['theoretical_fee'] = np.where(df_final['card_present_flag'] == 'Si', df_final['theoretical_fee_cp'], df_final['theoretical_fee_cnp'])
    df_final['quantified_error'] = df_final['fee_difference'] * df_final['sales_volume'] / 100.0

    condiciones = [
        df_final['mcc_code_corrected'] == 0,
        (df_final['applied_fee_var'] == 0) & (df_final['is_installment'] == 0),
        (df_final['applied_fee_var'] == 0) & (df_final['is_installment'] == 1),
        (df_final['applied_fee_var'] == 1.0) & (df_final['is_installment'] == 1),
        (df_final['applied_fee_var'] > 1.0) & (df_final['is_installment'] == 1),
    ]
    categorias = [
        'Error 1: MCC en 0',
        'Error 2: Tarifa en 0 sin cuotas',
        'Error 3: Tarifa en 0 para Cuotas',
        'Error 4: Cuotas con tarifa modificada',
        'Error 5: Cuotas con tarifa internacional',
    ]
    df_final['error_classification'] = np.select(condiciones, categorias, default='No clasificados')
    df_final = df_final[df_final['error_classification'] != 'NO Error 6: MCC 4511 – MD particular']

    resumen = df_final.groupby('error_classification', dropna=False).agg(
        affected_transactions=('trx_count','sum'),
        affected_sales=('sales_volume','sum'),
        total_quantified_error=('quantified_error','sum'),
    ).reset_index()

    tot = pd.DataFrame({
        'error_classification':['TOTAL GENERAL'],
        'affected_transactions':[resumen['affected_transactions'].sum()],
        'affected_sales':[resumen['affected_sales'].sum()],
        'total_quantified_error':[resumen['total_quantified_error'].sum()],
    })
    resumen = pd.concat([resumen, tot], ignore_index=True)
    ex_total = resumen[resumen['error_classification'] != 'TOTAL GENERAL']
    total = resumen[resumen['error_classification'] == 'TOTAL GENERAL']
    resumen = pd.concat([ex_total.sort_values('total_quantified_error', ascending=False), total], ignore_index=True)

    resumen_fmt = resumen.copy()
    for c in ['affected_transactions','affected_sales','total_quantified_error']:
        resumen_fmt[c] = resumen_fmt[c].apply(format_miles)

    return resumen_fmt, df_final, resumen

# ===============================
# 🔗 COMBINAR RESÚMENES (DÍA vs MTD)
# ===============================
def combinar_resumenes(res_dia: pd.DataFrame, res_mtd: pd.DataFrame) -> pd.DataFrame:
    d_ex = res_dia[res_dia['error_classification'] != 'TOTAL GENERAL'].copy()
    d_tot = res_dia[res_dia['error_classification'] == 'TOTAL GENERAL'].copy()
    m_ex = res_mtd[res_mtd['error_classification'] != 'TOTAL GENERAL'].copy()
    m_tot = res_mtd[res_mtd['error_classification'] == 'TOTAL GENERAL'].copy()

    merged = pd.merge(
        d_ex,
        m_ex[['error_classification','affected_transactions','affected_sales','total_quantified_error']],
        on='error_classification', how='outer', suffixes=("", "_MTD")
    )

    for c in ['affected_transactions','affected_sales','total_quantified_error',
              'affected_transactions_MTD','affected_sales_MTD','total_quantified_error_MTD']:
        if c in merged.columns:
            merged[c] = merged[c].fillna(0)

    merged = merged.sort_values('total_quantified_error', ascending=False, na_position='last')

    if not d_tot.empty:
        tot_row = {
            'error_classification': 'TOTAL GENERAL',
            'affected_transactions': float(d_tot['affected_transactions'].values[0]),
            'affected_sales': float(d_tot['affected_sales'].values[0]),
            'total_quantified_error': float(d_tot['total_quantified_error'].values[0]),
            'affected_transactions_MTD': float(m_tot['affected_transactions'].values[0]) if not m_tot.empty else 0.0,
            'affected_sales_MTD': float(m_tot['affected_sales'].values[0]) if not m_tot.empty else 0.0,
            'total_quantified_error_MTD': float(m_tot['total_quantified_error'].values[0]) if not m_tot.empty else 0.0,
        }
        merged = pd.concat([merged, pd.DataFrame([tot_row])], ignore_index=True)

    cols = [
        'error_classification',
        'affected_transactions', 'affected_transactions_MTD',
        'affected_sales', 'affected_sales_MTD',
        'total_quantified_error', 'total_quantified_error_MTD',
    ]
    for c in cols:
        if c not in merged.columns:
            merged[c] = np.nan
    merged = merged[cols]

    return merged

# ===============================
# 🏁 MAIN
# ===============================
def main() -> int:
    start_ts = datetime.now()
    print(f"🚀 Inicio: {start_ts}")
    try:
        conn = connect_redshift()
        try:
            df_liq = pd.read_sql(RED_SHIFT_QUERY, conn)
            df_liq_mtd = pd.read_sql(RED_SHIFT_QUERY_MTD, conn)
        finally:
            conn.close()
        print(f"SQL OK – filas día: {len(df_liq)} | filas MTD: {len(df_liq_mtd)}")

        bucket, prefix = parse_s3_url(S3_INPUT)
        key = get_latest_object(bucket, prefix, suffixes=(".csv",".CSV"))
        if not key:
            raise FileNotFoundError(f"No se encontró CSV en s3://{bucket}/{prefix}")
        print(f"BO encontrada: s3://{bucket}/{key}")
        df_bo_raw = read_csv_from_s3(bucket, key)
        df_bo = preparar_bo(df_bo_raw)
        print(f"BO OK – filas: {len(df_bo)}")

        resumen_fmt_dia, df_final_dia, resumen_raw_dia = procesar(df_liq, df_bo)
        resumen_fmt_mtd, df_final_mtd, resumen_raw_mtd = procesar(df_liq_mtd, df_bo)
        print(f"Discrepancias día: {len(df_final_dia)} filas | MTD: {len(df_final_mtd)} filas")

        resumen_comb = combinar_resumenes(resumen_raw_dia, resumen_raw_mtd)
        resumen_comb_fmt = resumen_comb.copy()
        for c in resumen_comb_fmt.columns:
            if c != 'error_classification':
                resumen_comb_fmt[c] = resumen_comb_fmt[c].apply(format_miles)

        if not df_final_mtd.empty:
            fecha_min = pd.to_datetime(df_final_mtd['trx_date']).min()
            fecha_max = pd.to_datetime(df_final_mtd['trx_date']).max()
        elif not df_final_dia.empty:
            fecha_min = fecha_max = pd.to_datetime(df_final_dia['trx_date']).max()
        else:
            fecha_min = fecha_max = None

        html = build_html_report(resumen_comb_fmt, fecha_min, fecha_max)
        subject = f"Validación de Tarifas – Resumen de errores ({datetime.now().strftime('%Y-%m-%d')})"

        csv_name_dia = f"detalles_validacion_tarifas_diario_{datetime.now().strftime('%Y%m%d')}.csv"
        csv_text_dia = df_final_dia.to_csv(index=False)
        csv_bytes_dia = ("\ufeff" + csv_text_dia).encode("utf-8")

        csv_name_mtd = f"detalles_validacion_tarifas_MTD_{datetime.now().strftime('%Y%m%d')}.csv"
        csv_text_mtd = df_final_mtd.to_csv(index=False)
        csv_bytes_mtd = ("\ufeff" + csv_text_mtd).encode("utf-8")

        attachments = [(csv_name_dia, csv_bytes_dia), (csv_name_mtd, csv_bytes_mtd)]
        send_email(subject, html, MAIL_RECIPIENTS, attachments=attachments)

        print(f"⏱️ Fin OK en {datetime.now() - start_ts}")
        return 0

    except Exception:
        print("[ERROR] Falló la ejecución:")
        tb = traceback.format_exc()
        print(tb)
        try:
            html_err = f"<html><body><h3>Fallo en Validación de Tarifas</h3><pre>{tb}</pre></body></html>"
            send_email("[ERROR] Proceso de Validación de Tarifas", html_err, MAIL_RECIPIENTS)
        except Exception as mail_err:
            print(f"No se pudo enviar el correo de error: {mail_err}")
        return 1

if __name__ == "__main__":
    sys.exit(main())