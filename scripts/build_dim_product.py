# scripts/dim_product.py
import pandas as pd

def process_dim_product():
    product = pd.read_csv("raw/product.csv")
    category = pd.read_csv("raw/product_category.csv")
    df = product.merge(category.rename(columns={"name": "category_name"}), on="category_id", how="left")
    df.insert(0, 'product_sk', range(1, 1 + len(df)))
    df.rename(columns={'name': 'product_name'}, inplace=True)
    df[['product_sk', 'product_id', 'product_name', 'sku', 'category_name', 'list_price', 'status']].to_csv("dw/dim_product.csv", index=False)

if __name__ == "__main__":
    process_dim_product()