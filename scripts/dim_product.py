import pandas as pd

product = pd.read_csv("raw/product.csv")
product_category = pd.read_csv("raw/product_category.csv")

df = product.merge(
    product_category[["category_id", "name"]].rename(columns={"name": "category_name"}),
    on="category_id", how="left"
)

df.to_csv("dw/dim_product.csv", index=False)