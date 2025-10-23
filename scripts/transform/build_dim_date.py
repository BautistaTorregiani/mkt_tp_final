import pandas as pd

def transform_dim_date(raw_data):
    sales_order = raw_data['sales_order']
    shipment = raw_data['shipment']
    payment = raw_data['payment']
    web_session = raw_data['web_session']
    nps = raw_data['nps_response']


    all_dates = pd.concat([
        pd.to_datetime(sales_order['order_date']),
        pd.to_datetime(shipment['shipped_at']),
        pd.to_datetime(shipment['delivered_at']),
        pd.to_datetime(payment['paid_at']),
        pd.to_datetime(web_session['started_at']),
        pd.to_datetime(nps['responded_at'])
    ]).dropna().dt.date.unique()

    df_dates = pd.DataFrame({'date': pd.to_datetime(all_dates)})

    # Crea la tabla de dimensión
    dim_date = pd.DataFrame()
    dim_date['date_id'] = df_dates['date'].dt.strftime('%Y%m%d').astype(int) 
    dim_date['date'] = df_dates['date']
    dim_date['year'] = df_dates['date'].dt.year
    dim_date['month'] = df_dates['date'].dt.month
    dim_date['month_name'] = df_dates['date'].dt.strftime('%B')
    dim_date['day'] = df_dates['date'].dt.day
    dim_date['quarter'] = df_dates['date'].dt.quarter
    dim_date['day_of_week_name'] = df_dates['date'].dt.day_name()

    return dim_date