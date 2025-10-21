import pandas as pd

sales_order_item = pd.read_csv("raw/sales_order_item.csv")
sales_order = pd.read_csv("raw/sales_order.csv")[[
    "order_id", "order_date", "channel_id", "store_id",
    "customer_id", "shipping_address_id", "status"
]]
address = pd.read_csv("raw/address.csv")[["address_id", "province_id"]]
product = pd.read_csv("raw/product.csv")[["product_id", "name"]].rename(columns={"name": "product_name"})


df = sales_order_item.merge(sales_order, on="order_id", how="left")


df = df.merge(address, left_on="shipping_address_id", right_on="address_id", how="left")

df = df.merge(product, on="product_id", how="left")

columns = [
    "order_item_id", "order_id", "order_date",
    "channel_id", "store_id", "customer_id",
    "province_id", "product_id", "product_name",
    "quantity", "unit_price", "discount_amount", "line_total", "status"
]

df[columns].to_csv("dw/fact_order_item.csv", index=False)