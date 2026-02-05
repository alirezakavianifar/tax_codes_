from helpers import connect_to_sql
import pandas as pd
from constants import get_sql_con
import matplotlib.pyplot as plt

sql_query = 'select * from tblTgju'
df = connect_to_sql(sql_query=sql_query, connect_type='read from tblTgju', sql_con=get_sql_con(
    password='14579Ali.'), read_from_sql=True, return_df=True, index_col='eng_date')

df['coin'] = df['coin'].str.replace(',', '').astype('int64')

df.coin.plot()
