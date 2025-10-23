import pandas as pd

def transform_dim_product(raw_data):
 
    product = raw_data['product']
    product_category = raw_data['product_category']

 
    df = product.merge(
        product_category.rename(columns={"name": "category_name"}),
        on="category_id",
        how="left"
    )
    
    df.insert(0, 'product_sk', range(1, 1 + len(df)))
    df.rename(columns={'name': 'product_name'}, inplace=True)

    dim_product = df[[
        'product_sk', 
        'product_id', 
        'product_name', 
        'sku', 
        'category_name', 
        'list_price', 
        'status'
    ]]

    return dim_product