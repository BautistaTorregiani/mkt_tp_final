import pandas as pd

def process_dim_date():
    sales_order = pd.read_csv("raw/sales_order.csv", parse_dates=["order_date"])
    shipment = pd.read_csv("raw/shipment.csv", parse_dates=["shipped_at", "delivered_at"])
    payment = pd.read_csv("raw/payment.csv", parse_dates=["paid_at"])
    web_session = pd.read_csv("raw/web_session.csv", parse_dates=["started_at"])
    nps = pd.read_csv("raw/nps_response.csv", parse_dates=["responded_at"])

    #Unifico las fechas
    all_dates = pd.concat([
        sales_order['order_date'],
        shipment['shipped_at'],
        shipment['delivered_at'],
        payment['paid_at'],
        web_session['started_at'],
        nps['responded_at']
    ]).dropna().dt.date.unique()

    df_dates = pd.DataFrame({'date': all_dates})
    df_dates['date'] = pd.to_datetime(df_dates['date'])

    dim_date = pd.DataFrame()
    dim_date['date_id'] = df_dates['date'].dt.strftime('%Y%m%d').astype(int)
    dim_date['date'] = df_dates['date']
    dim_date['year'] = df_dates['date'].dt.year
    dim_date['month'] = df_dates['date'].dt.month
    dim_date['month_name'] = df_dates['date'].dt.strftime('%B')
    dim_date['day'] = df_dates['date'].dt.day
    dim_date['quarter'] = df_dates['date'].dt.quarter
    dim_date['day_of_week_name'] = df_dates['date'].dt.day_name()

    dim_date.to_csv("dw/dim_date.csv", index=False)

if __name__ == "__main__":
    process_dim_date()