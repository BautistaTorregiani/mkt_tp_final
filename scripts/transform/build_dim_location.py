# etl/transform/dim_location.py
import pandas as pd

def transform_dim_location(raw_data):

    address = raw_data['address']
    province = raw_data['province']

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

    return dim_location