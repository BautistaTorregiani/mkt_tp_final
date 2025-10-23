import pandas as pd

def process_dim_customer():
    df = pd.read_csv("raw/customer.csv")
    df.insert(0, 'customer_sk', range(1, 1 + len(df)))
    df[['customer_sk', 'customer_id', 'email', 'first_name', 'last_name',"phone",'status',"created_at"]].to_csv("dw/dim_customer.csv", index=False)

if __name__ == "__main__":
    process_dim_customer()