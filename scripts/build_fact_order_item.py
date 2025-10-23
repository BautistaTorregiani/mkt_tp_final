import pandas as pd

def process_fact_order_item():
    # Cargar tablas
    item = pd.read_csv("raw/sales_order_item.csv")
    order = pd.read_csv("raw/sales_order.csv")
    
    dim_date = pd.read_csv("dw/dim_date.csv")
    dim_channel = pd.read_csv("dw/dim_channel.csv")
    dim_customer = pd.read_csv("dw/dim_customer.csv")
    dim_store = pd.read_csv("dw/dim_store.csv")
    dim_product = pd.read_csv("dw/dim_product.csv")
    dim_location = pd.read_csv("dw/dim_location.csv")

    # Unir items con ordenes
    df = item.merge(order, on="order_id", how="left")

    # Convertir fecha
    df['order_date_dt'] = pd.to_datetime(df['order_date']).dt.date
    
    # Asegurar tipos numéricos
    for col in ["quantity", "unit_price", "discount_amount", "line_total"]:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

    # Merges con dimensiones
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

    # Seleccionar solo columnas necesarias (sin las IDs naturales)
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

    # Crear surrogate key incremental
    fact_order_item.insert(0, 'order_item_sk', range(1, len(fact_order_item) + 1))

    # Exportar a CSV
    fact_order_item.to_csv("dw/fact_order_item.csv", index=False)

if __name__ == "__main__":
    process_fact_order_item()
