import pandas as pd

def transform_fact_web_session(raw_data, transformed_dims):
    web_session = raw_data['web_session']
    dim_date = transformed_dims['dim_date']
    dim_customer = transformed_dims['dim_customer']

    web_session['started_at'] = pd.to_datetime(web_session['started_at'], errors='coerce')
    web_session['ended_at'] = pd.to_datetime(web_session['ended_at'], errors='coerce')

    web_session['started_at_dt'] = web_session['started_at'].dt.date
    web_session['ended_at_dt'] = web_session['ended_at'].dt.date
    web_session['started_at_time'] = web_session['started_at'].dt.time
    web_session['ended_at_time'] = web_session['ended_at'].dt.time
    
    df = web_session

    #Reemplazo de Claves de Negocio por Claves Surrogadas
    df = df.merge(dim_customer[['customer_sk', 'customer_id']], on='customer_id', how='left')

    # Dimensión para fechas (inicio y fin)
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

    # Asegura que los IDs de fecha sean enteros 
    fact_web_session['started_date_id'] = fact_web_session['started_date_id'].astype('Int64')
    fact_web_session['ended_date_id'] = fact_web_session['ended_date_id'].astype('Int64')

    # Crea la clave surrogada para la tabla de hechos
    fact_web_session.insert(0, 'session_sk', range(1, len(fact_web_session) + 1))

    return fact_web_session