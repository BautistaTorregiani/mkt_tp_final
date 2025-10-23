import pandas as pd

def process_fact_shipment():
    # Cargar fuentes
    shipment = pd.read_csv("raw/shipment.csv")
    sales_order = pd.read_csv("raw/sales_order.csv")
    
    # Dimensiones DW
    dim_date = pd.read_csv("dw/dim_date.csv")
    dim_customer = pd.read_csv("dw/dim_customer.csv")
    dim_channel = pd.read_csv("dw/dim_channel.csv")
    dim_store = pd.read_csv("dw/dim_store.csv")
    dim_location = pd.read_csv("dw/dim_location.csv")

    # Unir envío con contexto de la orden
    df = shipment.merge(sales_order, on="order_id", how="left")

    # Reemplazo de business keys por SKs
    df = df.merge(dim_customer[['customer_sk', 'customer_id']], on='customer_id', how='left')
    df = df.merge(dim_channel[['channel_sk', 'channel_id']], on='channel_id', how='left')
    df = df.merge(dim_store[['store_sk', 'store_id']], on='store_id', how='left')
    df = df.merge(
        dim_location[['location_sk', 'address_id']],
        left_on='shipping_address_id',
        right_on='address_id',
        how='left'
    )

    # Rol de fechas (shipped / delivered)
    df['shipped_at_dt'] = pd.to_datetime(df['shipped_at'], errors='coerce').dt.date
    dim_date_shipped = dim_date.rename(columns={'date_id': 'shipped_date_id', 'date': 'shipped_date'})
    df = df.merge(
        dim_date_shipped[['shipped_date_id', 'shipped_date']],
        left_on='shipped_at_dt',
        right_on=pd.to_datetime(dim_date_shipped['shipped_date']).dt.date,
        how='left'
    )

    df['delivered_at_dt'] = pd.to_datetime(df['delivered_at'], errors='coerce').dt.date
    dim_date_delivered = dim_date.rename(columns={'date_id': 'delivered_date_id', 'date': 'delivered_date'})
    df = df.merge(
        dim_date_delivered[['delivered_date_id', 'delivered_date']],
        left_on='delivered_at_dt',
        right_on=pd.to_datetime(dim_date_delivered['delivered_date']).dt.date,
        how='left'
    )

    # Columnas finales (sin IDs naturales)
    cols = [
        "shipped_date_id",
        "delivered_date_id",
        "customer_sk",
        "channel_sk",
        "store_sk",
        "location_sk",
        "carrier",
        "tracking_number"
    ]
    fact_shipment = df[cols].copy()

    # Surrogate key simple (reinicia en 1 en cada corrida)
    fact_shipment.insert(0, 'shipment_sk', range(1, len(fact_shipment) + 1))

    # Exportar
    fact_shipment.to_csv("dw/fact_shipment.csv", index=False)

if __name__ == "__main__":
    process_fact_shipment()
