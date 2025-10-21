import pandas as pd

store = pd.read_csv("raw/store.csv")
address = pd.read_csv("raw/address.csv")
province = pd.read_csv("raw/province.csv")

df = store.merge(address, on="address_id", how="left")
df = df.merge(province[["province_id","name"]].rename(columns={"name":"province_name"}), on="province_id", how="left")

df.to_csv("dw/dim_store.csv", index=False)
