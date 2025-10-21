import pandas as pd

customer = pd.read_csv("raw/customer.csv")

customer.to_csv("dw/dim_customer.csv", index=False)
