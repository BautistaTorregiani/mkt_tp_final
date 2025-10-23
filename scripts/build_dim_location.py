import pandas as pd

def process_dim_location():
    address = pd.read_csv("raw/address.csv")
    province = pd.read_csv("raw/province.csv")

    df = address.merge(
        province.rename(columns={'name': 'province_name', 'code': 'province_code'}),
        on="province_id",
        how="left"
    )

    df.insert(0, 'location_sk', range(1, 1 + len(df)))

    
    dim_location = df[[
        'location_sk',
        'address_id', 
        'line1',
        'line2',
        'city',
        'postal_code',
        'province_name',
        'province_code',
        'country_code'
    ]]

    dim_location.to_csv("dw/dim_location.csv", index=False)

if __name__ == "__main__":
    process_dim_location()