import pandas as pd

def process_fact_order():
    sales_order = pd.read_csv("raw/sales_order.csv")
    dim_date = pd.read_csv("dw/dim_date.csv")
    dim_channel = pd.read_csv("dw/dim_channel.csv")
    dim_customer = pd.read_csv("dw/dim_customer.csv")
    dim_store = pd.read_csv("dw/dim_store.csv")
    dim_location = pd.read_csv("dw/dim_location.csv")

    # Conversión de fechas
    sales_order['order_date_dt'] = pd.to_datetime(sales_order['order_date']).dt.date
    
    # Conversión de columnas numéricas
    for col in ["subtotal", "tax_amount", "shipping_fee", "total_amount"]:
        sales_order[col] = pd.to_numeric(
            sales_order[col].astype(str).str.replace(",", "."),
            errors='coerce'
        ).fillna(0)

    df = sales_order

    # MERGE con dimensiones
    df = df.merge(dim_date[['date_id', 'date']],
                  left_on='order_date_dt',
                  right_on=pd.to_datetime(dim_date['date']).dt.date,
                  how='left')
    df = df.merge(dim_channel[['channel_sk', 'channel_id']], on='channel_id', how='left')
    df = df.merge(dim_customer[['customer_sk', 'customer_id']], on='customer_id', how='left')
    df = df.merge(dim_store[['store_sk', 'store_id']], on='store_id', how='left')
    df = df.merge(dim_location[['location_sk', 'address_id']],
                  left_on='shipping_address_id',
                  right_on='address_id',
                  how='left')
    
    # Selección de columnas finales
    cols = [
        "date_id",
        "channel_sk",
        "store_sk",
        "customer_sk",
        "location_sk",
        "status",
        "subtotal",
        "tax_amount",
        "shipping_fee",
        "total_amount"
    ]
    
    fact_order = df[cols].copy()

    # Agregar surrogate key incremental
    fact_order.insert(0, 'order_sk', range(1, len(fact_order) + 1))

    # Exportar a CSV
    fact_order.to_csv("dw/fact_order.csv", index=False)

if __name__ == "__main__":
    process_fact_order()


