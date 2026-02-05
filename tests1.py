# import xlrd
# import pandas as pd
# from helpers import drop_into_db, merge_multiple_excel_files
import pandas as pd
from helpers import time_it, connect_to_sql, drop_into_db
from constants import get_sql_con


df = pd.read_excel(
    r'C:\Users\alkav\Desktop\صد پرونده مهم استان - 97 لغایت 1400.xlsx', sheet_name='ارزش افزوده')

dfg = df.groupby(
    ['کد اداره', 'نام اداره', 'شماره اقتصادی', 'نام مودی', 'سال عملکرد'])

lst = []

for k, v in dfg:
    v.set_index(['کد اداره', 'نام اداره', 'شماره اقتصادی', 'نام مودی', 'سال عملکرد', 'دوره عملکرد'],
                drop=True, inplace=True)
    lst.append(v)
pd.concat(lst).to_excel(
    r'E:\automating_reports_V2\saved_dir\test\sample\test.xlsx')


drop_into_db(table_name='Ray_Gharar_Kham', columns=df.columns.tolist(
), values=df.values.tolist(), append_to_prev=True, db_name='CommissionDb',)


sql_query = "select * from tblLog"

connect_to_sql(
    sql_query, sql_con=get_sql_con(database='testdbV2'), read_from_sql=True, return_df=True)

print('g')


@time_it(num_runs=2)
def hello(index=1):
    print('f')
    index = 2
    try:
        raise Exception
    except:
        return (index, 'error')


hello(index=1)


df1 = pd.read_excel(
    r'E:\automating_reports_V2\saved_dir\codeghtesadi\filess.xlsx')

df1 = df1.drop_duplicates()

df2 = pd.read_excel(
    r'E:\automating_reports_V2\saved_dir\codeghtesadi\files.xlsx')

df = df1.merge(df2, how='left', left_on='melli', right_on='melli')


df.to_excel(r'E:\automating_reports_V2\saved_dir\codeghtesadi\filess.xlsx')
print('f')
# r'E:\automating_reports_V2\saved_dir\codeghtesadi\codeeghtesadi.csv', chunksize=10)

# for item in df:
#     df1 = item
#     break

# df.to_csv(r'E:\automating_reports_V2\saved_dir\codeghtesadi\codeeghtesadi.csv')


# drop_into_db('tblCodeeghtesadi', df1.columns.tolist(), df1.values.tolist())
path_base = r'E:\automating_reports_V2\saved_dir\test\hoghoghi\اشخاص حقوقی سری اول_اداره کل امور مالياتي خوزستان_2463.xls'
path_hoghoghi = r'E:\automating_reports_V2\saved_dir\test\hoghoghi\Excel.xlsx'
path_dest = r'E:\automating_reports_V2\saved_dir\test\hoghoghi\dest.xlsx'
path_merged = r'E:\automating_reports_V2\saved_dir\test\hoghoghi\dest-merged.xlsx'

# df = merge_multiple_excel_files(
#     path, path, table_name='tblMostaghelatTashkhis', delete_after_merge=True, postfix='xls', drop_to_sql=True)


# df = xlrd.open_workbook(
#     r'E:\automating_reports_V2\saved_dir\mostaghelat\mostaghelat_ghatee\Rpt_Deterministic(18).xls')

df_base = pd.read_excel(path_base)
df_hoghoghi = pd.read_excel(path_hoghoghi)

df_base.columns.tolist()
df_hoghoghi.columns.tolist()
merged.columns.tolist()

selected_columns = [
    'کد اداره',
    'نام اداره',
    'سال عملکرد',
    'شناسه ملی / کد ملی (TIN)',
    'کد رهگیری ثبت نام',
    'نوع مودی',
    'نام مودی',
    'کدپستی مودی',
    'شناسه اظهارنامه']

merged = df_base.merge(df_hoghoghi, how='outer', left_on='Tin',
                       right_on='شناسه ملی / کد ملی (TIN)', indicator=True)
merged = merged.loc[lambda merged: merged['_merge'].isin(
    ['both', 'left_only'])]
merged = merged[selected_columns]

agg_merged = merged.groupby('نام اداره')


merged.to_excel(path_dest, index=False)


# df = pd.read_excel(
#     r'E:\automating_reports_V2\saved_dir\mostaghelat\mostaghelat_ghatee\Rpt_Deterministic(19).xls')
