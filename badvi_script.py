from helpers import connect_to_sql
from sql_queries import get_badvi_tables, replace_last
import pandas as pd


df = connect_to_sql(get_badvi_tables(), read_from_sql=True,
                    connect_type='e', return_df=True)

sql_query = ''

for i, table in df.iterrows():
    sql_query += 'SELECT * FROM [10.52.0.114].[TestDb].[dbo].[%s] UNION \n' % table[0]

sql_query = replace_last(sql_query, strToReplace='UNION')

final_df = connect_to_sql(
    sql_query=sql_query, read_from_sql=True, connect_type='e', return_df=True)

final_df.to_excel(r'C:\final_badvi.xlsx', index=False)
