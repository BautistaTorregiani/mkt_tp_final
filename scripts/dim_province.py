import pandas as pd

province = pd.read_csv("raw/province.csv")

province.to_csv("dw/dim_province.csv", index=False)