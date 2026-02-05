import pandas as pd
import math
from functools import reduce

path_ezafe = r'E:\automating_reports_V2\automation\ezaf_refah_modir\اضافه کار و رفاهی تیر 1403\pro\اضافه کار تير.xls'
path_refahi = r'E:\automating_reports_V2\automation\ezaf_refah_modir\اضافه کار و رفاهی تیر 1403\pro\رفاهي تير.xls'
path_zarayeb = r'E:\automating_reports_V2\automation\ezaf_refah_modir\اضافه کار و رفاهی تیر 1403\pro\ضرایب مدیر.xlsx'
path_nafar_edare = r'E:\automating_reports_V2\automation\ezaf_refah_modir\اضافه کار و رفاهی تیر 1403\pro\نفر-اداره.xls'

ezafe_cols = ['شماره کارمند', 'نام کارمند', 'نرخ اضافه کار']
refahi_cols = ['شماره کارمند', 'نرخ']
ezafe_refahi_cols = ['شماره کارمند', 'نام کارمند', 'نرخ اضافه کار', 'نرخ']


df_ezafe = pd.read_excel(path_ezafe)[ezafe_cols]
df_refahi = pd.read_excel(path_refahi)[refahi_cols]
df_zarayeb = pd.read_excel(path_zarayeb)
df_nafar_edare = pd.read_excel(path_nafar_edare)

lst_ezafe_refahi = [df_ezafe, df_refahi]

df_merged_ezafe_refahi = reduce(lambda left, right: pd.merge(
    left, right, on='شماره کارمند', how='outer'), lst_ezafe_refahi)

lst_merged_ezafe_refahi_edare = [df_merged_ezafe_refahi, df_nafar_edare]

df_merged_ezafe_refahi_edare = reduce(lambda left, right: pd.merge(
    left, right, on='شماره کارمند', how='left'), lst_merged_ezafe_refahi_edare)

lst_merged_ezafe_refahi_edare_zarayeb = [
    df_merged_ezafe_refahi_edare, df_zarayeb]

df_merged_ezafe_refahi_edare_zarayeb = reduce(lambda left, right: pd.merge(
    left, right, on='اداره', how='left'), lst_merged_ezafe_refahi_edare_zarayeb).drop_duplicates(subset=['شماره کارمند'])


def calculate_ezafe(x):
    try:
        score = math.ceil(x['سرانه اضافه کار'] * x['نرخ اضافه کار'])
        return score
    except ValueError:
        return 0


def calculate_refahi(x):
    try:
        score = math.ceil((x['سرانه رفاهی'] * x['نرخ']) / 100)
        return score
    except ValueError:
        return 0


df_merged_ezafe_refahi_edare_zarayeb['مبلغ اضافه کار اولیه'] = df_merged_ezafe_refahi_edare_zarayeb.apply(
    lambda row: calculate_ezafe(row), axis=1)

df_merged_ezafe_refahi_edare_zarayeb['مبلغ رفاهی اولیه'] = df_merged_ezafe_refahi_edare_zarayeb.apply(
    lambda row: calculate_refahi(row), axis=1)

cols_df_merged_ezafe_refahi_edare_zarayeb = [
    'اداره', 'شماره کارمند', 'نام کارمند', 'نرخ اضافه کار', 'نرخ']

grouped_df = df_merged_ezafe_refahi_edare_zarayeb.groupby('اداره', dropna=False).agg({
    'مبلغ اضافه کار اولیه': 'sum',
    'مبلغ رفاهی اولیه': 'sum',
    'شماره کارمند': 'count',  # Counting rows in each group
    'سرانه اضافه کار': 'first',  # Adding first value of 'سرانه اضافه کار'
    'سرانه رفاهی': 'first'  # Adding first value of 'سرانه رفاهی'
}).reset_index()

# Renaming the 'شماره کارمند' column to 'تعداد' for clarity
grouped_df.rename(columns={'شماره کارمند': 'تعداد'}, inplace=True)

grouped_df['اداره'] = grouped_df['اداره'].fillna('نامشخص')

grouped_df.to_excel(r'automation\ezaf_refah_modir\main.xlsx')
df_merged_ezafe_refahi_edare_zarayeb[cols_df_merged_ezafe_refahi_edare_zarayeb].to_excel(
    r'automation\ezaf_refah_modir\details.xlsx')
