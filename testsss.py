from helpers import drop_into_db
import pandas as pd


df = pd.read_excel(
    r'E:\automating_reports_V2\saved_dir\پولشویی\merged_hassanah_fund_data.xlsx',)

df = df.astype('str')

drop_into_db(table_name='poolshoyee', columns=df.columns.tolist(
), values=df.values.tolist(), append_to_prev=False, db_name='TestDb',)
