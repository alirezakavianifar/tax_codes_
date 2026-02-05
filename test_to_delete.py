import pandas as pd
from helpers import read_multiple_excel_files, connect_to_sql,\
    read_multiple_excel_sheets, read_multiple_excel_sheets,\
    measure_time, wrap_func, connect_to_sql, leading_zero
import numpy as np
from sql_queries import get_sql_v_portal
from constants import get_sql_con
import numpy as np

path3 = r'E:\automating_reports_V2\saved_dir\test\haghighi-187.xlsx'

sql_query = 'SELECT * FROM [aspnet-MunicipalityWebApp].[dbo].[PermitOwnerInfos]'
df_77 = connect_to_sql(
    sql_query, sql_con=get_sql_con(database='aspnet-MunicipalityWebApp'),
    read_from_sql=True, return_df=True)

df_187 = read_multiple_excel_sheets(path3, sheet_name=None)

df_77['NationalIdField'] = df_77['NationalIdField'].astype(str)
df_187['کد ملی'] = df_187['کد ملی/ شناسه ملی مودی'].apply(
    lambda x: leading_zero(str(x)))

df_187['کد ملی'] = df_187['کد ملی'].astype(str)

df_merge = df_187.merge(df_77, how='left', left_on='کد ملی',
                        right_on='NationalIdField')

sql_query = 'SELECT * FROM [aspnet-MunicipalityWebApp].[dbo].[PermitMainInfos]'
df_parvaneh = connect_to_sql(
    sql_query, sql_con=get_sql_con(database='aspnet-MunicipalityWebApp'),
    read_from_sql=True, return_df=True)

df_merge_final = df_merge.merge(df_parvaneh, how='left', left_on='FileNoField',
                                right_on='FileNoField', indicator=True).\
    to_excel(r'E:\automating_reports_V2\saved_dir\test\34.xlsx')


df = pd.read_excel(path, sheet_name='Sheet1')
df_eghtesadi = pd.read_excel(path2)

# df_gasht = pd.read_excel(path, sheet_name='Sheet2')


df['شناسه ملی'] = df['شناسه ملی'].astype(np.int64)
df_eghtesadi['شناسه ملی'] = df_eghtesadi['شناسه ملی'].astype(
    np.int64)

df_merge = df.merge(df_eghtesadi, how='left', left_on='شناسه ملی', right_on='شناسه ملی').\
    drop_duplicates(subset='شناسه ملی').\
    to_excel(r'E:\automating_reports_V2\saved_dir\test\mainssxx.xlsx')

df_edares = connect_to_sql(
    """SELECT 
                [city]
                ,[edare]
                ,[Moavenat]
                ,[IrisCode]
            FROM [tax].[dbo].[tblEdareShahr]""",
    sql_con=get_sql_con(database='tax'), read_from_sql=True, return_df=True)

df_edares['edare'] = df_edares['edare'].astype('str')
df['edare'] = df['edare'].astype('str')

final_df = df.merge(df_edares, how='left', left_on=[
                    'edare'], right_on=['edare'])

# final_df.to_excel(r'C:\Users\alkav\Downloads\finals.xlsx')

dfg = final_df.groupby(by=['edare', 'city'])
for key, item in dfg:
    item.drop(columns='ردیف', inplace=True)
    item.to_excel(
        r'C:\Users\alkav\Downloads\%s.xlsx' % str(key), index=False)


dfs = []
for i in range(1387, 1400):

    df = pd.read_excel(path, sheet_name=f'{i}')
    cols = df.columns.tolist()
    if 'شهر' in cols:
        df = df.rename({'شهر': 'شهرستان'}, inplace=True)
    dfs.append(df)

final_df = pd.concat(dfs)
final_df = final_df.dropna(subset=['سال'])
final_df.loc[final_df['سال'] == 97.0, 'سال'] = 1397.0
final_df.loc[final_df['سال'] == 97.0, 'سال'] = 1397.0
final_df['آدرس'] = final_df['آدرس'].fillna(final_df['شهرستان'])
final_df.dropna(subset=['آدرس', 'شهرستان'], inplace=True)

final_df['آدرس نهایی'] = final_df['شهرستان'] + \
    ' ' + final_df['آدرس']
final_df.dropna(subset=['آدرس نهایی'], inplace=True)

final_df.to_excel(r'E:\automating_reports_V2\saved_dir\test\finalall.xlsx')
dfg = final_df.groupby(by=['شهرستان'])
for key, item in dfg:
    item.to_excel(r'E:\automating_reports_V2\saved_dir\test\%s.xlsx' % key)


def leading_zero(x):
    if len(x) == 8:
        return '00' + x
    elif len(x) == 9:
        return '0' + x
    return x


sql_query = """
SELECT 
    DISTINCT
	CASE SUBSTRING([سال عملکرد],1,4) WHEN 'nan' THEN '0' ELSE SUBSTRING([سال عملکرد],1,4) END AS [سال عملکرد]
    ,[کد رهگيري ثبت نام ]
	,[شماره اقتصادی]
	,[نام اداره]
  FROM [TestDbv2].[dbo].[V_PORTAL]
"""

df = connect_to_sql(
    sql_query, sql_con=get_sql_con(database='testdbV2'), read_from_sql=True, return_df=True)


df['سال عملکرد'] = df['سال عملکرد'].astype('int')

df_egh = pd.read_excel(
    r'E:\automating_reports_V2\saved_dir\test\SanimEdare\ثبت نام هاي اداره کل 16.xlsx')

df_egh['شناسه ملی جدید'] = df_egh['شناسه ملی'].apply(
    lambda x: leading_zero(str(x)))

df_egh['کد رهگیری'] = df_egh['کد رهگیری'].astype('str')
df['کد رهگيري ثبت نام '] = df['کد رهگيري ثبت نام '].astype(
    'str')


df.sort_values(by=['کد رهگيري ثبت نام ',
               'سال عملکرد'], ascending=False, inplace=True)
df.drop_duplicates(subset=['کد رهگيري ثبت نام '], inplace=True)


df_final = df.merge(df_egh, how='inner',
                    left_on='کد رهگيري ثبت نام ', right_on='کد رهگیری')

df_final.to_excel(
    r'E:\automating_reports_V2\saved_dir\test\SanimEdare\final.xlsx')


# pd.options.mode.use_inf_as_na = True
# files = read_multiple_excel_files(
#     r'C:\ezhar-temp\1396\1000_parvande\merge', postfix='xlsx')

# dfs = []

# try:

#     while (True):
#         file = next(files)
#         df = pd.read_excel(file)
#         dfs.append(df)

# except Exception as e:
#     df = pd.concat(dfs)

# # df.to_excel(r'C:\ezhar-temp\1396\1000_parvande\finaldf.xlsx')


# # df_g = df.groupby(['نام اداره', 'سال عملکرد', 'منبع مالیاتی',
# #                   'دارای برگ قطعی', 'دارای برگ تشخیص']).size().reset_index()
# cols = ['نام اداره', 'سال عملکرد', 'منبع مالیاتی',
#         'دارای برگ تشخیص', 'تعداد']

# dff_agg_t = pd.pivot_table(df[cols], values=['تعداد'], index=['نام اداره'],
#                            columns=['سال عملکرد',
#                                     'منبع مالیاتی', 'دارای برگ تشخیص'],
#                            aggfunc=np.sum, fill_value=0).reset_index()

# dff_agg_t.to_excel(r'C:\ezhar-temp\1396\1000_parvande\finalt.xlsx')
