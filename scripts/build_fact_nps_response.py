import pandas as pd

def process_fact_nps():
   
    nps_response = pd.read_csv("raw/nps_response.csv")
    dim_date = pd.read_csv("dw/dim_date.csv")
    dim_customer = pd.read_csv("dw/dim_customer.csv")
    dim_channel = pd.read_csv("dw/dim_channel.csv")

    nps_response['score'] = pd.to_numeric(nps_response['score'], errors='coerce').fillna(0)
    nps_response['responded_at'] = pd.to_datetime(nps_response['responded_at'], errors='coerce')
    nps_response['responded_at_dt'] = nps_response['responded_at'].dt.date
    nps_response['responded_at_time'] = nps_response['responded_at'].dt.time
    df = nps_response

   
    dim_date_nps = dim_date.rename(columns={'date_id': 'responded_at_date_id', 'date': 'responded_date'})
    df = df.merge(
        dim_date_nps[['responded_at_date_id', 'responded_date']],
        left_on='responded_at_dt',
        right_on=pd.to_datetime(dim_date_nps['responded_date']).dt.date,
        how='left'
    )
    
    df = df.merge(dim_customer[['customer_sk', 'customer_id']], on='customer_id', how='left')
    df = df.merge(dim_channel[['channel_sk', 'channel_id']], on='channel_id', how='left')

    
    cols = [
        "responded_at_date_id",  
        "responded_at_time",     
        "customer_sk",
        "channel_sk",
        "score",
        "comment"               
    ]
    fact_nps_response = df[cols].copy()

    
    fact_nps_response.insert(0, 'nps_sk', range(1, len(fact_nps_response) + 1))
    
   
    fact_nps_response['responded_at_date_id'] = fact_nps_response['responded_at_date_id'].astype('Int64')

   
    fact_nps_response.to_csv("dw/fact_nps_response.csv", index=False)
  

if __name__ == "__main__":
    process_fact_nps()