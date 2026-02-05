import pandas as pd
from helpers import connect_to_sql
from constants import get_sql_con


def get_edare4(x):
    return str(x[:4])


def get_edare5(x):
    return str(x[:5])


df = pd.read_excel(
    r'E:\automating_reports_V2\saved_dir\arzeshafzoodeh_sonati\test\1.xlsx')

sql_query = 'select edare from tblEdareShahr'

edare = connect_to_sql(sql_query, sql_con=get_sql_con(
    database='tax'), read_from_sql=True, return_df=True)

df['edare4'] = df['واحد مالیاتی'].apply(
    lambda x: get_edare4(str(x)))

df['edare5'] = df['واحد مالیاتی'].apply(
    lambda x: get_edare5(str(x)))

print('f')

merged1 = df.merge(edare, how='inner', right_on='edare', left_on='edare4')
merged2 = df.merge(edare, how='inner', right_on='edare', left_on='edare5')
