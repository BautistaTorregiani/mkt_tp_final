import pandas as pd

def transform_dim_channel(raw_data):
    df = raw_data['channel']
    df.insert(0, 'channel_sk', range(1, 1 + len(df)))
    df.rename(columns={'name': 'channel_name', 'code': 'channel_code'}, inplace=True)
    
    dim_channel = df[['channel_sk', 'channel_id', 'channel_name', 'channel_code']]

    return dim_channel