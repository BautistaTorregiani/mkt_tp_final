import pandas as pd

def process_fact_web_session():
    """
    Crea la tabla de hechos para las sesiones web, separando fecha y hora,
    y utilizando claves surrogadas para las dimensiones.
    """
    # --- 1. Carga de Datos ---
    web_session = pd.read_csv("raw/web_session.csv")
    dim_date = pd.read_csv("dw/dim_date.csv")
    dim_customer = pd.read_csv("dw/dim_customer.csv")

    # --- 2. Preparación y Separación de Fecha/Hora ---
    web_session['started_at'] = pd.to_datetime(web_session['started_at'], errors='coerce')
    web_session['ended_at'] = pd.to_datetime(web_session['ended_at'], errors='coerce')

    web_session['started_at_dt'] = web_session['started_at'].dt.date
    web_session['ended_at_dt'] = web_session['ended_at'].dt.date
    web_session['started_at_time'] = web_session['started_at'].dt.time
    web_session['ended_at_time'] = web_session['ended_at'].dt.time
    
    df = web_session

    # --- 3. Reemplazo de Business Keys por SKs ---
    df = df.merge(dim_customer[['customer_sk', 'customer_id']], on='customer_id', how='left')

    # Dimensión de rol para fechas (inicio y fin)
    dim_date_started = dim_date.rename(columns={'date_id': 'started_date_id', 'date': 'started_date'})
    df = df.merge(
        dim_date_started[['started_date_id', 'started_date']],
        left_on='started_at_dt',
        right_on=pd.to_datetime(dim_date_started['started_date']).dt.date,
        how='left'
    )

    dim_date_ended = dim_date.rename(columns={'date_id': 'ended_date_id', 'date': 'ended_date'})
    df = df.merge(
        dim_date_ended[['ended_date_id', 'ended_date']],
        left_on='ended_at_dt',
        right_on=pd.to_datetime(dim_date_ended['ended_date']).dt.date,
        how='left'
    )

    # --- 4. Selección de Columnas Finales (sin session_id) ---
    cols = [
        "started_date_id",
        "started_at_time",
        "ended_date_id",
        "ended_at_time",
        "customer_sk",
        "source",
        "device"
    ]
    fact_web_session = df[cols].copy()

    # --- 5. Surrogate key simple (reinicia en 1) ---
    fact_web_session.insert(0, 'session_sk', range(1, len(fact_web_session) + 1))
    fact_web_session['session_sk'] = fact_web_session['session_sk'].astype(int)

    # --- 6. Exportar ---
    fact_web_session.to_csv("dw/fact_web_session.csv", index=False)

if __name__ == "__main__":
    process_fact_web_session()
