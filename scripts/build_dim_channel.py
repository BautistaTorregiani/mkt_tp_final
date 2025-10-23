import pandas as pd

def process_dim_channel():
    df = pd.read_csv("raw/channel.csv")
    df.insert(0, 'channel_sk', range(1, 1 + len(df)))
    df.rename(columns={'name': 'channel_name', 'code': 'channel_code'}, inplace=True)
    df[['channel_sk', 'channel_id', 'channel_name', 'channel_code']].to_csv("dw/dim_channel.csv", index=False)

if __name__ == "__main__":
    process_dim_channel()