# scripts/dim_store.py
import pandas as pd

def process_dim_store():
    store = pd.read_csv("raw/store.csv")
    address = pd.read_csv("raw/address.csv")
    province = pd.read_csv("raw/province.csv")

    df = store.merge(address, on="address_id", how="left")
    
    df = df.merge(
        province.rename(columns={'name': 'province_name', 'code': 'province_code'}),
        on="province_id",
        how="left"
    )
    df.insert(0, 'store_sk', range(1, 1 + len(df)))
    df.rename(columns={'name': 'store_name'}, inplace=True)
    
    dim_store = df[[
        'store_sk',
        'store_id',
        'store_name',
        'line1',
        'line2',
        'city',
        'postal_code',
        'province_name',
        'province_code',
        'country_code',
        'created_at'
    ]]

    dim_store.to_csv("dw/dim_store.csv", index=False)

if __name__ == "__main__":
    process_dim_store()