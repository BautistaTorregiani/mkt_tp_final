import pandas as pd

payment = pd.read_csv("raw/payment.csv")
sales_order = pd.read_csv("raw/sales_order.csv")[["order_id","customer_id","channel_id"]]

df = payment.merge(sales_order, on="order_id", how="left")

df.to_csv("dw/fact_payment.csv", index=False)