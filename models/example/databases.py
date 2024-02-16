import snowflake.snowpark as snowpark
import pandas as pd
import numpy as np

def main(dbt,session):
    dbt.config(materialized="table")
    # snowSession = snowpark.Session
    data = {
        'order_id': [1, 1, 2, 2, 3, 3, 4, 5, 5],
        'sku_id': [101, 102, 103, 104, 105, 106, 107, 108, 109],
        'price': [10, 15, 10, 25, 20, 15, 30, 40, 45],
        'quantity': [1, 2, 1, 3, 2, 1, 1, 1, 2],
        'category': ['shirt', 'pants', 'shirt', 'pants', 'shirt', 'pants', 'accessory', 'shirt', 'shirt']}

    df = pd.DataFrame(data)
        
    df['total_value'] = df['price'] * df['quantity']
    
    # Aggregate to get total order value and order size (in terms of item count)
    order_values = df.groupby('order_id').agg(total_order_value=('total_value', 'sum'), order_size=('sku_id', 'count')).reset_index()
    
    aov_by_order_size = order_values.groupby('order_size').agg(Average_Order_Value=('total_order_value', 'mean')).reset_index()

    return session.create_dataframe(aov_by_order_size)