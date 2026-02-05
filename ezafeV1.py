# from helpers import read_multiple_excel_files
import pandas as pd
import glob
import os
import numpy as np
import math
df_nim = pd.read_excel(
    r'E:\automating_reports_V2\automation\motamam1402\1403\nim\test\final.xlsx')


# path = r'E:\automating_reports_V2\automation\motamam1402\edarat\base'

# files = glob.glob(os.path.join(path, "*.%s" % 'xlsx'))

dics = {
    '1601': 93,
    '1602': 76,
    '1603': 89,
    '1605': 81,
    '1604': 77,
    '1606': 92,
    '1607': 62,
    '1608': 70,
    '1609': 82,
    '1610': 99,
    '1611': 76,
    '1612': 98,
    '1613': 95,
    '1614': 72,
    '1615': 78,
    '1616': 64,
    '1617': 75,
    '1618': 67,
    '1619': 78,
    '1620': 70,
    '1621': 78,
    '1668': 90,
    'آبادان': 96,
    'آغاجاری': 69,
    'اموراداری': 77,
    'آموزش': 74,
    'امیدیه': 99,
    'اندیمشک': 66,
    'ایذه': 95,
    'باغملک': 66,
    'بندرامام': 98,
    'بهبهان': 89,
    'بودجه': 61,
    'تدارکات': 85,
    'حقوقی': 98,
    'خرمشهر': 65,
    'درآمد': 86,
    'دزفول': 92,
    'دشت آزادگان': 68,
    'ذیحسابی': 65,
    'رامشیر': 94,
    'رامهرمز': 93,
    'روابط عمومی': 68,
    'شادگان': 70,
    'شوشتر': 93,
    'فناوری': 74,
    'گتوند': 85,
    'لالی': 95,
    'ماهشهر': 79,
    'مسجد سلیمان': 83,
    'هفتکل': 90,
    'هندیجان': 89,
    'هویزه': 91
}

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
