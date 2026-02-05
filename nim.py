# from helpers import read_multiple_excel_files
import pandas as pd
import glob
import os
import numpy as np
import math

df_nim_edare_score = pd.read_excel(
    r'EE:\automating_reports_V2\automation\motamam1402\1403\payaneh\پایانه های فروشگاهی خرداد1403\تجمیع ارسالی خرداد 1403.xlsx')


df_nim_modir_score = pd.read_excel(
    r'E:\automating_reports_V2\automation\motamam1402\1403\payaneh\پایانه های فروشگاهی خرداد1403\امتیاز مدیر به ادارات پایانه فروشگاهی خرداد.xlsx')

# df_nim_modir_score['سرانه حسابرس'] = df_nim_modir_score['سرانه حسابرس'] + 5_000_000
# df_nim_modir_score['سرانه حسابرس ارشد'] = df_nim_modir_score['سرانه حسابرس ارشد'] + 5_000_000
# df_nim_modir_score['سرانه رئیس گروه'] = df_nim_modir_score['سرانه رئیس گروه'] + 5_000_000

joined_df = pd.merge(df_nim_edare_score, df_nim_modir_score,
                     left_on='اداره', right_on='اداره', how='left')


grouped_edare = joined_df.groupby('اداره')
lst_all = []

for key, item in grouped_edare:
    posts_dict = dict(item['پست جدید'].value_counts())
    if 'حسابرس' not in posts_dict.keys():
        posts_dict['حسابرس'] = 0
    if 'رییس گروه' not in posts_dict.keys():
        posts_dict['رییس گروه'] = 0
    item['سرانه اداره'] = posts_dict['حسابرس'] * item['سرانه حسابرس'] +\
        posts_dict['حسابرس ارشد'] * item['سرانه حسابرس ارشد'] + \
        posts_dict['رییس گروه'] * item['سرانه رئیس گروه']
    lst_all.append(item)

df_final = pd.concat(lst_all)


def calculate_bonus(x):
    if x['پست جدید'] == 'رییس گروه':
        return np.int64((x['حد اکثر رئیس گروه'] * x['جمع امتیاز ']) / 100)
    elif x['پست جدید'] == 'حسابرس ارشد':
        return np.int64((x['حد اکثر حسابرس ارشد'] * x['جمع امتیاز ']) / 100)
    elif x['پست جدید'] == 'حسابرس':
        return np.int64((x['حد اکثر حسابرس'] * x['جمع امتیاز ']) / 100)


df_final['مبلغ نیم درصد'] = df_final.apply(
    lambda row: calculate_bonus(row), axis=1)

grouped_df = df_final.groupby('اداره')
lst_all = []

for key, item in grouped_df:

    sum_sarane = sum(item['مبلغ نیم درصد'])
    if item['سرانه اداره'].iloc[-1] < sum_sarane:
        price_to_deduct = (
            sum_sarane - item['سرانه اداره'].iloc[-1]) / len(item)
        item['مبلغ نیم درصد'] -= price_to_deduct
        item['مبلغ نیم درصد'] = item['مبلغ نیم درصد'].astype(np.int64)

    lst_all.append(item)


df_final = pd.concat(lst_all)

df_final.to_excel(
    r'E:\automating_reports_V2\automation\motamam1402\1403\nim\Main\نیم خرداد 1403\nimi_joon.xlsx')


print('f')


# Calculate the sum of all values
total_sum = sum(dics.values())
grouped_df = df_nim.groupby('نام اداره')
lst = []

# Iterate through each group and save as separate Excel workbook
for key, item in grouped_df:
    print(item)
    sum_score = item['جمع امتیاز'].sum()
    item['مبلغ نیم درصد'] = np.int64(
        ((40_000_000_000 / total_sum) * dics[key]) * (item['جمع امتیاز'] / sum_score))
    item['جمع امتیاز اداره'] = sum_score
    item['امتیاز مدیر به اداره'] = dics[key]
    item['جمع امتیازات مدیر به ادارات'] = total_sum
    lst.append(item)

final_df = pd.concat(lst)
final_df.to_excel(
    r'E:\automating_reports_V2\automation\motamam1402\1403\nim\test\final_test.xlsx', index=False)

print('f')


try:
    all_excels = []
    for file in files:
        file_name = file.split('\\')[-1].split('.')[0]
        for key, value in dicts.items():
            if key == file_name:
                ezaf_ref = key
                break
        df_edare = pd.read_excel(file, skiprows=1)[
            ['نام و نام خانوادگی', 'شماره کارمندی', 'جمع امتیاز']].dropna()
        df_meged_ezafe = df_edare.merge(
            base_ezafe, how='left', right_on='شماره کارمند', left_on='شماره کارمندی')
        df_meged_final = df_meged_ezafe.merge(
            base_refahi, how='left', right_on='شماره کارمند', left_on='شماره کارمندی')
        ezafe_kar_avalie = dicts[ezaf_ref]['ezafe'] * \
            df_meged_final['نرخ اضافه کار'].sum()
        df_meged_final['ezafe_kar_avalie'] = dicts[ezaf_ref]['ezafe'] * \
            df_meged_final['نرخ اضافه کار'].sum()

        if df_meged_final['نرخ اضافه کار'].isna().any():
            nan_rows = df_meged_final[df_meged_final['نرخ اضافه کار'].isna()]

            # Print the rows where 'column_name' contains NaN values
            print(nan_rows)
            df_meged_final = df_meged_final.dropna()

        jam_emtiaz = df_meged_final['جمع امتیاز'].sum()

        avg_ezafe = ezafe_kar_avalie / jam_emtiaz

        df_meged_final['مبلغ اضافه کار اصلی'] = avg_ezafe * \
            df_meged_final['جمع امتیاز']

        df_meged_final['ساعت اضافه کار اصلی'] = (
            df_meged_final['مبلغ اضافه کار اصلی'] / df_meged_final['نرخ اضافه کار']).apply(math.ceil)

        df_meged_final['اضافه کار اولیه'] = dicts[ezaf_ref]['ezafe'] * \
            df_meged_final['نرخ اضافه کار']

        df_meged_final['سرانه اضافه کار'] = dicts[ezaf_ref]['ezafe']
        df_meged_final['سرانه رفاهی'] = dicts[ezaf_ref]['refahi']

        df_meged_final['رفاهی اصلی'] = df_meged_final['جمع امتیاز'] * \
            (((df_meged_final['نرخ رفاهی'] * dicts[ezaf_ref]
             ['refahi']) / 100).sum() / jam_emtiaz)
        df_meged_final['رفاهی درصد'] = (
            (df_meged_final['رفاهی اصلی'] / df_meged_final['نرخ رفاهی']) * 100).apply(math.ceil)

        df_meged_final['نام اداره'] = file_name

        all_excels.append(df_meged_final)

    final_df = pd.concat(all_excels)
    final_df = final_df[['نام اداره', 'نام و نام خانوادگی',
                         'شماره کارمندی', 'جمع امتیاز', 'ساعت اضافه کار اصلی', 'رفاهی درصد']].\
        to_excel(
            r'E:\automating_reports_V2\automation\motamam1402\edarat\base\final.xlsx')
    print('f')

    # df_meged_final.to_excel(os.path.join(
    #     r'E:\automating_reports_V2\automation\motamam1402\edarat\tmp', file.split('\\')[-1]))

except Exception as e:
    print('done')
