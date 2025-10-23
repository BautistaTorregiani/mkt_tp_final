import pandas as pd

def transform_fact_shipment(raw_data, transformed_dims):
    shipment = raw_data['shipment']
    sales_order = raw_data['sales_order']
    
    dim_date = transformed_dims['dim_date']
    dim_customer = transformed_dims['dim_customer']
    dim_channel = transformed_dims['dim_channel']
    dim_store = transformed_dims['dim_store']
    dim_location = transformed_dims['dim_location']

    df = shipment.merge(sales_order, on="order_id", how="left")


    df = df.merge(dim_customer[['customer_sk', 'customer_id']], on='customer_id', how='left')
    df = df.merge(dim_channel[['channel_sk', 'channel_id']], on='channel_id', how='left')
    df = df.merge(dim_store[['store_sk', 'store_id']], on='store_id', how='left')
    df = df.merge(
        dim_location[['location_sk', 'address_id']],
        left_on='shipping_address_id',
        right_on='address_id',
        how='left'
    )

    # Separa fecha y hora para ambas fechas de rol
    df['shipped_at'] = pd.to_datetime(df['shipped_at'], errors='coerce')
    df['shipped_at_dt'] = df['shipped_at'].dt.date
    df['shipped_at_time'] = df['shipped_at'].dt.time

    df['delivered_at'] = pd.to_datetime(df['delivered_at'], errors='coerce')
    df['delivered_at_dt'] = df['delivered_at'].dt.date
    df['delivered_at_time'] = df['delivered_at'].dt.time

    dim_date_shipped = dim_date.rename(columns={'date_id': 'shipped_date_id', 'date': 'shipped_date'})
    df = df.merge(
        dim_date_shipped[['shipped_date_id', 'shipped_date']],
        left_on='shipped_at_dt',
        right_on=pd.to_datetime(dim_date_shipped['shipped_date']).dt.date,
        how='left'
    )

    dim_date_delivered = dim_date.rename(columns={'date_id': 'delivered_date_id', 'date': 'delivered_date'})
    df = df.merge(
        dim_date_delivered[['delivered_date_id', 'delivered_date']],
        left_on='delivered_at_dt',
        right_on=pd.to_datetime(dim_date_delivered['delivered_date']).dt.date,
        how='left'
    )

    cols = [
        "shipped_date_id",
        "shipped_at_time",
        "delivered_date_id",
        "delivered_at_time",
        "customer_sk",
        "channel_sk",
        "store_sk",
        "location_sk",
        "carrier",
        "tracking_number"
    ]
    fact_shipment = df[cols].copy()

    # Asegura que los IDs de fecha sean enteros (y manejen nulos)
    fact_shipment['shipped_date_id'] = fact_shipment['shipped_date_id'].astype('Int64')
    fact_shipment['delivered_date_id'] = fact_shipment['delivered_date_id'].astype('Int64')

    # Crea surrogate key incremental para la tabla de hechos
    fact_shipment.insert(0, 'shipment_sk', range(1, len(fact_shipment) + 1))

    return fact_shipment