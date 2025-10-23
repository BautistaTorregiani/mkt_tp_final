import pandas as pd

def process_fact_nps():
    # --- 1. Carga de Datos ---
    nps_response = pd.read_csv("raw/nps_response.csv")
    dim_date = pd.read_csv("dw/dim_date.csv")
    dim_customer = pd.read_csv("dw/dim_customer.csv")
    dim_channel = pd.read_csv("dw/dim_channel.csv")

    # --- 2. Preparación ---
    nps_response['score'] = pd.to_numeric(nps_response['score'], errors='coerce').fillna(0)
    nps_response['responded_at'] = pd.to_datetime(nps_response['responded_at'], errors='coerce')

    # Separar fecha y hora
    nps_response['responded_at_date'] = nps_response['responded_at'].dt.date
    nps_response['responded_at_time'] = nps_response['responded_at'].dt.time

    # --- 3. Reemplazo de claves ---
    df = nps_response
    df = df.merge(
        dim_date[['date_id', 'date']],
        left_on='responded_at_date',
        right_on=pd.to_datetime(dim_date['date']).dt.date,
        how='left'
    )
    df = df.merge(dim_customer[['customer_sk', 'customer_id']], on='customer_id', how='left')
    df = df.merge(dim_channel[['channel_sk', 'channel_id']], on='channel_id', how='left')

    cols = [
        "date_id",
        "responded_at_time",
        "customer_sk",
        "comment",
        "channel_sk",
        "score"
    ]
    fact_nps_response = df[cols].copy()

    fact_nps_response.insert(0, 'nps_response_sk', range(1, len(fact_nps_response) + 1))
    fact_nps_response['nps_response_sk'] = fact_nps_response['nps_response_sk'].astype(int)


    fact_nps_response.to_csv("dw/fact_nps_response.csv", index=False)

if __name__ == "__main__":
    process_fact_nps()
