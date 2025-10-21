import pandas as pd

shipment = pd.read_csv("raw/shipment.csv")
sales_order = pd.read_csv("raw/sales_order.csv")[["order_id","customer_id","channel_id","store_id","shipping_address_id"]]
address = pd.read_csv("raw/address.csv")[["address_id","province_id","city"]]
province = pd.read_csv("raw/province.csv")[["province_id","name"]].rename(columns={"name":"province_name"})

df = shipment.merge(sales_order, on="order_id", how="left")
df = df.merge(address, left_on="shipping_address_id", right_on="address_id", how="left")
df = df.merge(province, on="province_id", how="left")

cols = [c for c in [
    "shipment_id","order_id",
    "customer_id","channel_id","store_id",
    "province_id","province_name","city",
    "shipping_fee","shipped_date","delivered_date","promised_date",
    "status"
] if c in df.columns]

df[cols].to_csv("dw/fact_shipment.csv", index=False)