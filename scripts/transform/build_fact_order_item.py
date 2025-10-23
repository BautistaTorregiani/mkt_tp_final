import pandas as pd

def transform_fact_order_item(raw_data, transformed_dims):
    item = raw_data['sales_order_item']
    order = raw_data['sales_order']
    
    dim_date = transformed_dims['dim_date']
    dim_channel = transformed_dims['dim_channel']
    dim_customer = transformed_dims['dim_customer']
    dim_store = transformed_dims['dim_store']
    dim_product = transformed_dims['dim_product']
    dim_location = transformed_dims['dim_location']

    df = item.merge(order, on="order_id", how="left")

   
    df['order_date_dt'] = pd.to_datetime(df['order_date']).dt.date
    
    # Asegurar tipos numéricos
    for col in ["quantity", "unit_price", "discount_amount", "line_total"]:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

    # 3. Reemplaza claves de negocio por claves surrogadas
    df = df.merge(dim_date[['date_id', 'date']],
                  left_on='order_date_dt',
                  right_on=pd.to_datetime(dim_date['date']).dt.date,
                  how='left')
    df = df.merge(dim_channel[['channel_sk', 'channel_id']], on='channel_id', how='left')
    df = df.merge(dim_customer[['customer_sk', 'customer_id']], on='customer_id', how='left')
    df = df.merge(dim_store[['store_sk', 'store_id']], on='store_id', how='left')
    df = df.merge(dim_product[['product_sk', 'product_id']], on='product_id', how='left')
    df = df.merge(dim_location[['location_sk', 'address_id']],
                  left_on='shipping_address_id',
                  right_on='address_id',
                  how='left')

    cols = [
        "date_id",       
        "channel_sk",
        "store_sk",
        "customer_sk",
        "location_sk",
        "product_sk",
        "quantity",
        "unit_price",
        "discount_amount",
        "line_total"
    ]
    
    fact_order_item = df[cols].copy()
    
    # Asegura
    for col in ['date_id', 'channel_sk', 'store_sk', 'customer_sk', 'location_sk', 'product_sk']:
        fact_order_item[col] = fact_order_item[col].astype('Int64')

    fact_order_item.insert(0, 'order_item_sk', range(1, len(fact_order_item) + 1))
    
    return fact_order_item