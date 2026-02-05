# from helpers import read_multiple_excel_files
import pandas as pd
import glob
import os
import math
base_ezafe = pd.read_excel(
    r'E:\automating_reports_V2\automation\motamam1402\1403\ezafeRefahi\140301\اضافه کار و رفاهی فروردین 1403\فايل اوليه اضافه کار فروردين.xls')[['شماره کارمند', 'نوع استخدام', 'نرخ اضافه کار']]
base_refahi = pd.read_excel(
    r'E:\automating_reports_V2\automation\motamam1402\1403\ezafeRefahi\140301\اضافه کار و رفاهی فروردین 1403\فايل اوليه رفاهي فروردين.xls')[['شماره کارمند', 'نرخ رفاهی']]

path = r'E:\automating_reports_V2\automation\motamam1402\1403\ezafeRefahi\140301\اضافه کار و رفاهی فروردین 1403\پاسخ ادارات-اضافه و رفاهی فروردین1403'

files = glob.glob(os.path.join(path, "*.%s" % 'xlsx'))


dicts = {
    '1601': {'ezafe': 146, 'refahi': 85},
    '1602': {'ezafe': 121, 'refahi': 75},
    '1603': {'ezafe': 120, 'refahi': 80},
    '1604': {'ezafe': 120, 'refahi': 80},
    '1605': {'ezafe': 138, 'refahi': 70},
    '1606': {'ezafe': 144, 'refahi': 70},
    '1607': {'ezafe': 125, 'refahi': 70},
    '1608': {'ezafe': 141, 'refahi': 85},
    '1609': {'ezafe': 128, 'refahi': 65},
    '1610': {'ezafe': 112, 'refahi': 65},
    '1611': {'ezafe': 138, 'refahi': 80},
    '1612': {'ezafe': 132, 'refahi': 68},
    '1613': {'ezafe': 132, 'refahi': 68},
    '1614': {'ezafe': 124, 'refahi': 68},
    '1615': {'ezafe': 134, 'refahi': 75},
    '1616': {'ezafe': 98, 'refahi': 68},
    '1617': {'ezafe': 132, 'refahi': 68},
    '1618': {'ezafe': 127, 'refahi': 65},
    '1619': {'ezafe': 134, 'refahi': 65},
    '1620': {'ezafe': 132, 'refahi': 68},
    '1621': {'ezafe': 127, 'refahi': 65},
    '1668': {'ezafe': 165, 'refahi': 85},
    'آبادان': {'ezafe': 143, 'refahi': 75},
    'آغاجاری': {'ezafe': 152, 'refahi': 85},
    'اموراداری': {'ezafe': 132, 'refahi': 68},
    'آموزش': {'ezafe': 132, 'refahi': 68},
    'امیدیه': {'ezafe': 117, 'refahi': 70},
    'اندیمشک': {'ezafe': 124, 'refahi': 75},
    'ایذه': {'ezafe': 152, 'refahi': 70},
    'باغملک': {'ezafe': 127, 'refahi': 65},
    'بندرامام': {'ezafe': 162, 'refahi': 80},
    'بهبهان': {'ezafe': 154, 'refahi': 85},
    'بودجه': {'ezafe': 132, 'refahi': 68},
    'تدارکات': {'ezafe': 132, 'refahi': 68},
    'حقوقی': {'ezafe': 132, 'refahi': 68},
    'خرمشهر': {'ezafe': 156, 'refahi': 85},
    'درآمد': {'ezafe': 132, 'refahi': 68},
    'دزفول': {'ezafe': 137, 'refahi': 70},
    'دشت آزادگان': {'ezafe': 117, 'refahi': 75},
    'ذیحسابی': {'ezafe': 132, 'refahi': 68},
    'رامشیر': {'ezafe': 150, 'refahi': 85},
    'رامهرمز': {'ezafe': 99, 'refahi': 65},
    'روابط عمومی': {'ezafe': 132, 'refahi': 68},
    'شادگان': {'ezafe': 145, 'refahi': 68},
    'شوشتر': {'ezafe': 137, 'refahi': 75},
    'فناوری': {'ezafe': 132, 'refahi': 68},
    'گتوند': {'ezafe': 137, 'refahi': 85},
    'لالی': {'ezafe': 135, 'refahi': 80},
    'ماهشهر': {'ezafe': 152, 'refahi': 70},
    'مسجد سلیمان': {'ezafe': 127, 'refahi': 65},
    'هفتکل': {'ezafe': 147, 'refahi': 70},
    'هندیجان': {'ezafe': 136, 'refahi': 68},
    'هویزه': {'ezafe': 132, 'refahi': 70},
}

try:
    all_excels = []
    for file in files:
        file_name = file.split('\\')[-1].split('.')[0]
        for key, value in dicts.items():
            if key == file_name:
                ezaf_ref = key
                break
        df_edare = pd.read_excel(file, skiprows=1)[
            ['نام و نام خانوادگی', 'شماره کارمندی', 'جمع امتیاز ']].dropna()
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

        jam_emtiaz = df_meged_final['جمع امتیاز '].sum()

        avg_ezafe = ezafe_kar_avalie / jam_emtiaz

        df_meged_final['مبلغ اضافه کار اصلی'] = avg_ezafe * \
            df_meged_final['جمع امتیاز ']

        df_meged_final['ساعت اضافه کار اصلی'] = (
            df_meged_final['مبلغ اضافه کار اصلی'] / df_meged_final['نرخ اضافه کار']).apply(math.ceil)

        df_meged_final['اضافه کار اولیه'] = dicts[ezaf_ref]['ezafe'] * \
            df_meged_final['نرخ اضافه کار']

        df_meged_final['سرانه اضافه کار'] = dicts[ezaf_ref]['ezafe']
        df_meged_final['سرانه رفاهی'] = dicts[ezaf_ref]['refahi']

        df_meged_final['رفاهی اصلی'] = df_meged_final['جمع امتیاز '] * \
            (((df_meged_final['نرخ رفاهی'] * dicts[ezaf_ref]
             ['refahi']) / 100).sum() / jam_emtiaz)
        df_meged_final['رفاهی درصد'] = (
            (df_meged_final['رفاهی اصلی'] / df_meged_final['نرخ رفاهی']) * 100).apply(math.ceil)

        df_meged_final['نام اداره'] = file_name

        all_excels.append(df_meged_final)

    final_df = pd.concat(all_excels)
    final_df = final_df[['نام اداره', 'نام و نام خانوادگی', 'نوع استخدام',
                         'شماره کارمندی', 'جمع امتیاز ', 'ساعت اضافه کار اصلی', 'رفاهی درصد', 'نرخ اضافه کار', 'نرخ رفاهی']].\
        to_excel(
            r'E:\automating_reports_V2\automation\motamam1402\edarat\base\finale2.xlsx')
    print('f')

    # df_meged_final.to_excel(os.path.join(
    #     r'E:\automating_reports_V2\automation\motamam1402\edarat\tmp', file.split('\\')[-1]))

except Exception as e:
    print('done')
