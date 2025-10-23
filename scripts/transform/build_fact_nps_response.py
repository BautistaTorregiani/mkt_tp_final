import pandas as pd

def transform_fact_nps_response(raw_data, transformed_dims):
    nps_response = raw_data['nps_response']
    dim_date = transformed_dims['dim_date']
    dim_customer = transformed_dims['dim_customer']
    dim_channel = transformed_dims['dim_channel']

    nps_response['score'] = pd.to_numeric(nps_response['score'], errors='coerce').fillna(0)
    nps_response['responded_at'] = pd.to_datetime(nps_response['responded_at'], errors='coerce')

    # Separa fecha y hora
    nps_response['responded_at_date'] = nps_response['responded_at'].dt.date
    nps_response['responded_at_time'] = nps_response['responded_at'].dt.time
    
    df = nps_response

    dim_date_renamed = dim_date.rename(columns={'date_id': 'responded_at_date_id', 'date': 'responded_date'})
    df = df.merge(
        dim_date_renamed[['responded_at_date_id', 'responded_date']],
        left_on='responded_at_date',
        right_on=pd.to_datetime(dim_date_renamed['responded_date']).dt.date,
        how='left'
    )
    df = df.merge(dim_customer[['customer_sk', 'customer_id']], on='customer_id', how='left')
    df = df.merge(dim_channel[['channel_sk', 'channel_id']], on='channel_id', how='left')

    cols = [
        "responded_at_date_id",
        "responded_at_time",
        "customer_sk",
        "comment",
        "channel_sk",
        "score"
    ]
    fact_nps_response = df[cols].copy()
    
    # Asegurar que el ID de fecha sea un entero (y maneje nulos)
    fact_nps_response['responded_at_date_id'] = fact_nps_response['responded_at_date_id'].astype('Int64')

    fact_nps_response.insert(0, 'nps_response_sk', range(1, len(fact_nps_response) + 1))

    return fact_nps_response