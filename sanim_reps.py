
import xlsxwriter
import xlrd
import pandas as pd
from automation.sql_queries import get_sql_query_tashnoeblagh
from automation.helpers import connect_to_sql
from automation.constants import get_sql_con

sql_query = get_sql_query_tashnoeblagh()
path = r'E:\automating_reports_V2\saved_dir\test\sample\تشخیص ابلاغ نشده سنیم.xlsx'

df = connect_to_sql(
    sql_query, sql_con=get_sql_con(database='testdbV2'),
    read_from_sql=True, return_df=True)

dfs_g = df.groupby(['نام اداره']).size(
).reset_index().rename(columns={0: 'تعداد'})

with pd.ExcelWriter(path, engine='xlsxwriter') as writer:
    df.to_excel(writer, sheet_name="جزئیات", index=False)
    dfs_g.to_excel(writer, sheet_name="آمار", index=False)
    worksheet = writer.sheets['جزئیات']
    border_fmt = writer.book.add_format(
        {'bottom': 1, 'top': 1, 'left': 1, 'right': 1})
    worksheet.conditional_format(xlsxwriter.utility.xl_range(
        0, 0, len(df), len(df.columns)), {'type': 'no_errors', 'format': border_fmt})
    # Change the direction for the worksheet.
    worksheet.right_to_left()
    worksheet = writer.sheets['آمار']
    worksheet.conditional_format(xlsxwriter.utility.xl_range(0, 0, len(
        dfs_g), len(dfs_g.columns)), {'type': 'no_errors', 'format': border_fmt})
    worksheet.right_to_left()
