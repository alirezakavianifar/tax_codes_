import pandas as pd
import math
from functools import reduce

path_nim = r'E:\automating_reports_V2\automation\ezaf_refah_modir\اضافه کار و رفاهی تیر 1403\pro\nim\نیم درصد تیر.xls'
path_zarayeb = r'E:\automating_reports_V2\automation\ezaf_refah_modir\اضافه کار و رفاهی تیر 1403\pro\nim\ضرایب مدیر.xlsx'
path_nafar_edare = r'E:\automating_reports_V2\automation\ezaf_refah_modir\اضافه کار و رفاهی تیر 1403\pro\nim\نفر-اداره.xls'

nim_cols = ['شماره کارمند', 'نام کارمند']


df_nim = pd.read_excel(path_nim)[nim_cols]
df_zarayeb = pd.read_excel(path_zarayeb)
df_nafar_edare = pd.read_excel(path_nafar_edare)

lst_nim_nafar_edare = [df_nim, df_nafar_edare]

df_nim_nafar_edare = reduce(lambda left, right: pd.merge(
    left, right, on='شماره کارمند', how='left'), lst_nim_nafar_edare)

lst_nim_nafar_edare_zarayeb = [df_nim_nafar_edare, df_zarayeb]

df_nim_nafar_edare_zarayeb = reduce(lambda left, right: pd.merge(
    left, right, on='اداره', how='left'), lst_nim_nafar_edare_zarayeb).drop_duplicates(subset=['شماره کارمند'])


grouped_df = df_nim_nafar_edare_zarayeb.groupby('اداره', dropna=False).agg(
    sum_سرانه_پاداش=('سرانه پاداش', 'sum'),
    first_سرانه_پاداش=('سرانه پاداش', 'first'),
    count_شماره_کارمند=('شماره کارمند', 'count')
).reset_index()

# Renaming the 'شماره کارمند' column to 'تعداد' for clarity
grouped_df.rename(columns={'count_شماره_کارمند': 'تعداد'}, inplace=True)
grouped_df.rename(
    columns={'first_سرانه_پاداش': 'سرانه پاداش اختصاصی'}, inplace=True)
grouped_df.rename(columns={'sum_سرانه_پاداش': 'جمع سرانه پاداش'}, inplace=True)

grouped_df['اداره'] = grouped_df['اداره'].fillna('نامشخص')

grouped_df.to_excel(r'automation\ezaf_refah_modir\g_nim.xlsx')
df_nim_nafar_edare_zarayeb.to_excel(
    r'automation\ezaf_refah_modir\d_nim.xlsx')
