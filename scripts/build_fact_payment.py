import pandas as pd

def process_fact_payment():
    """
    Crea la tabla de hechos para los eventos de pago, enriquecida con
    dimensiones adicionales como tienda y ubicación de facturación.
    """
    # --- 1. Carga de Datos ---
    payment = pd.read_csv("raw/payment.csv")
    sales_order = pd.read_csv("raw/sales_order.csv")

    dim_date = pd.read_csv("dw/dim_date.csv")
    dim_customer = pd.read_csv("dw/dim_customer.csv")
    dim_channel = pd.read_csv("dw/dim_channel.csv")
    dim_store = pd.read_csv("dw/dim_store.csv")
    dim_location = pd.read_csv("dw/dim_location.csv")

    # --- 2. Preparación y Unión de Datos ---
    df = payment.merge(
        sales_order[["order_id", "customer_id", "channel_id", "store_id", "billing_address_id"]],
        on="order_id",
        how="left"
    )

    df['paid_at_dt'] = pd.to_datetime(df['paid_at'], errors='coerce').dt.date
    df['amount'] = pd.to_numeric(df['amount'], errors='coerce').fillna(0)

    # --- 3. Reemplazo de Claves de Negocio por Claves Surrogadas ---
    df = df.merge(
        dim_date[['date_id', 'date']],
        left_on='paid_at_dt',
        right_on=pd.to_datetime(dim_date['date']).dt.date,
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

    # --- 4. Selección de Columnas Finales ---
    cols = [
        "payment_id",
        "date_id",
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

    # --- 5. Surrogate key simple ---
    fact_payment.insert(0, 'payment_sk', range(1, len(fact_payment) + 1))
    fact_payment['payment_sk'] = fact_payment['payment_sk'].astype(int)

    # --- 6. Exportar ---
    fact_payment.to_csv("dw/fact_payment.csv", index=False)

if __name__ == "__main__":
    process_fact_payment()
