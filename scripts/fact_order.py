import pandas as pd

sales_order = pd.read_csv("raw/sales_order.csv")
address = pd.read_csv("raw/address.csv")
province = pd.read_csv("raw/province.csv")
store = pd.read_csv("raw/store.csv")

df = sales_order.merge(address[["address_id","province_id"]],
                       left_on="shipping_address_id", right_on="address_id", how="left")
df = df.merge(province[["province_id","name"]].rename(columns={"name":"province_name"}),
              on="province_id", how="left")
df = df.merge(store[["store_id","name"]].rename(columns={"name":"store_name"}),
              on="store_id", how="left")

cols = [
    "order_id","order_date","channel_id","store_id","store_name","customer_id",
    "province_id","province_name",
    "subtotal","tax_amount","shipping_fee","total_amount","status"
]
df[cols].to_csv("dw/fact_order.csv", index=False)


