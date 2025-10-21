import pandas as pd

channel = pd.read_csv("raw/channel.csv")
channel[["channel_id","code","name"]].to_csv("dw/dim_channel.csv", index=False)