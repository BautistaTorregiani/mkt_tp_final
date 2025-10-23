import pandas as pd

def transform_dim_store(raw_data):

    store = raw_data['store']
    address = raw_data['address']
    province = raw_data['province']
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

    print("  -> Dimensión 'dim_store' transformada.")
    
    return dim_store