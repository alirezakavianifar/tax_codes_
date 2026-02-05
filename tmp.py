import pandas as pd
from helpers import df_to_excelsheet


path = r'E:\automating_reports_V2\saved_dir\tmp\milliard_sheet1.xlsx'

df = pd.read_excel(path)

df_ag = df.groupby('نام اداره')

df = df_to_excelsheet(
    r'E:\automating_reports_V2\saved_dir\tmp\final.xlsx', df_ag, 1)


# df_ezhar = pd.read_excel(
#     r'E:\automating_reports_V2\saved_dir\tmp\Excel (3).xlsx')

# df_mil = pd.read_excel(
#     r'E:\automating_reports_V2\saved_dir\tmp\eee.xls')

# cols = ['کد اداره', 'نام اداره', 'شناسه ملی / کد ملی (TIN)']

# df_ezhar = df_ezhar[cols]

# df_merged = df_mil.merge(df_ezhar, how='left', left_on='TIN',
#                          right_on='شناسه ملی / کد ملی (TIN)', indicator=True)

# df_merged.to_excel(
#     r'E:\automating_reports_V2\saved_dir\tmp\milliard_sheet1.xlsx')

print('g')
