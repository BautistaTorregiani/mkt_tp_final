import pandas as pd

def process_fact_payment():
    payment = pd.read_csv("raw/payment.csv")
    sales_order = pd.read_csv("raw/sales_order.csv")

    dim_date = pd.read_csv("dw/dim_date.csv")
    dim_customer = pd.read_csv("dw/dim_customer.csv")
    dim_channel = pd.read_csv("dw/dim_channel.csv")
    dim_store = pd.read_csv("dw/dim_store.csv")
    dim_location = pd.read_csv("dw/dim_location.csv")

  
    df = payment.merge(
        sales_order[["order_id", "customer_id", "channel_id", "store_id", "billing_address_id"]],
        on="order_id",
        how="left"
    )

    df['amount'] = pd.to_numeric(df['amount'], errors='coerce').fillna(0)

    df['paid_at'] = pd.to_datetime(df['paid_at'], errors='coerce')
    df['paid_at_dt'] = df['paid_at'].dt.date         
    df['paid_at_time'] = df['paid_at'].dt.time       

  
    dim_date_payment = dim_date.rename(columns={'date_id': 'paid_at_date_id', 'date': 'paid_date'})
    df = df.merge(
        dim_date_payment[['paid_at_date_id', 'paid_date']],
        left_on='paid_at_dt',
        right_on=pd.to_datetime(dim_date_payment['paid_date']).dt.date,
        how='left'
    )
    
    df = df.merge(dim_customer[['customer_sk', 'customer_id']], on='customer_id', how='left')
    df = df.merge(dim_channel[['channel_sk', 'channel_id']], on='channel_id', how='left')
    df = df.merge(dim_store[['store_sk', 'store_id']], on='store_id', how='left')
    df = df.merge(
        dim_location[['location_sk', 'address_id']],
        left_on='billing_address_id',
        right_on='address_id',
        how='left'
    )

    cols = [
        "paid_at_date_id",  
        "paid_at_time",     
        "customer_sk",
        "channel_sk",
        "store_sk",
        "location_sk",      
        "status",
        "method",
        "transaction_ref",
        "amount"
    ]
    fact_payment = df[cols].copy()

    #Creo una surrogate
    fact_payment.insert(0, 'payment_sk', range(1, len(fact_payment) + 1))
    
    #Id de fecha entero
    fact_payment['paid_at_date_id'] = fact_payment['paid_at_date_id'].astype('Int64')

    fact_payment.to_csv("dw/fact_payment.csv", index=False)
    


if __name__ == "__main__":
    process_fact_payment()