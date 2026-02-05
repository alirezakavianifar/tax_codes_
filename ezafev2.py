# from helpers import read_multiple_excel_files
import pandas as pd
import glob
import os
import math
import numpy as np

base_ezafe = pd.read_excel(
    r'E:\automating_reports_V2\automation\motamam1402\1403\ezafeRefahi\140301\اضافه کار و رفاهی خرداد 1403\ارسالی ادارات\تجمیع\تجمیع ارسالی خرداد 1403.xlsx')

base_ezafe['اداره'] = base_ezafe['اداره'].astype('str')

dicts = {
    '1601': {'ezafe': 90, 'refahi': 67},
    '1602': {'ezafe': 100, 'refahi': 75},
    '1603': {'ezafe': 95, 'refahi': 70},
    '1604': {'ezafe': 100, 'refahi': 75},
    '1605': {'ezafe': 100, 'refahi': 75},
    '1606': {'ezafe': 90, 'refahi': 67},
    '1607': {'ezafe': 90, 'refahi': 67},
    '1608': {'ezafe': 90, 'refahi': 67},
    '1609': {'ezafe': 90, 'refahi': 67},
    '1610': {'ezafe': 100, 'refahi': 75},
    '1611': {'ezafe': 95, 'refahi': 70},
    '1612': {'ezafe': 85, 'refahi': 60},
    '1613': {'ezafe': 100, 'refahi': 75},
    '1614': {'ezafe': 90, 'refahi': 65},
    '1615': {'ezafe': 85, 'refahi': 65},
    '1616': {'ezafe': 95, 'refahi': 67},
    '1617': {'ezafe': 95, 'refahi': 70},
    '1618': {'ezafe': 95, 'refahi': 70},
    '1619': {'ezafe': 95, 'refahi': 70},
    '1620': {'ezafe': 95, 'refahi': 70},
    '1621': {'ezafe': 95, 'refahi': 70},
    '1668': {'ezafe': 85, 'refahi': 65},
    'آبادان': {'ezafe': 97, 'refahi': 74},
    'آغاجاری': {'ezafe': 90, 'refahi': 67},
    'اموراداری': {'ezafe': 95, 'refahi': 65},
    'آموزش': {'ezafe': 95, 'refahi': 65},
    'امیدیه': {'ezafe': 90, 'refahi': 67},
    'اندیمشک': {'ezafe': 90, 'refahi': 67},
    'ایذه': {'ezafe': 100, 'refahi': 75},
    'باغملک': {'ezafe': 90, 'refahi': 67},
    'بندرامام': {'ezafe': 93, 'refahi': 67},
    'بهبهان': {'ezafe': 100, 'refahi': 75},
    'بودجه': {'ezafe': 80, 'refahi': 60},
    'تدارکات': {'ezafe': 90, 'refahi': 65},
    'حقوقی': {'ezafe': 90, 'refahi': 65},
    'خرمشهر': {'ezafe': 100, 'refahi': 75},
    'درآمد': {'ezafe': 85, 'refahi': 65},
    'دزفول': {'ezafe': 97, 'refahi': 72},
    'دشت آزادگان': {'ezafe': 90, 'refahi': 67},
    'ذیحسابی': {'ezafe': 80, 'refahi': 60},
    'رامشیر': {'ezafe': 90, 'refahi': 67},
    'رامهرمز': {'ezafe': 85, 'refahi': 65},
    'روابط عمومی': {'ezafe': 100, 'refahi': 70},
    'شادگان': {'ezafe': 90, 'refahi': 67},
    'شوشتر': {'ezafe': 90, 'refahi': 67},
    'فناوری': {'ezafe': 95, 'refahi': 65},
    'گتوند': {'ezafe': 90, 'refahi': 67},
    'لالی': {'ezafe': 85, 'refahi': 65},
    'ماهشهر': {'ezafe': 98, 'refahi': 73},
    'مسجد سلیمان': {'ezafe': 95, 'refahi': 70},
    'هفتکل': {'ezafe': 85, 'refahi': 65},
    'هندیجان': {'ezafe': 85, 'refahi': 65},
    'هویزه': {'ezafe': 85, 'refahi': 65},
}


def calculate_ezafe(x):
    score = np.int64(
        math.ceil((x['جمع امتیاز '] * dicts[x['اداره']]['ezafe']) / 100))
    if (x['نوع استخدام'] == 'مشاغل کارگري' and score > 120):
        score = 120
    return score


def calculate_refahi(x):
    score = np.int64(
        math.ceil((x['جمع امتیاز '] * dicts[x['اداره']]['refahi']) / 100))
    return score


def calculate_sarane_ezafe(x):
    return dicts[x['اداره']]['ezafe']


def calculate_sarane_refahi(x):
    return dicts[x['اداره']]['refahi']


base_ezafe['ساعت اضافه کار'] = base_ezafe.apply(
    lambda row: calculate_ezafe(row), axis=1)

base_ezafe['سرانه اضافه کار داده شده به اداره توسط مدیر'] = base_ezafe.apply(
    lambda row: calculate_sarane_ezafe(row), axis=1)

base_ezafe['درصد رفاهی'] = base_ezafe.apply(
    lambda row: calculate_refahi(row), axis=1)

base_ezafe['سرانه رفاهی داده شده به اداره توسط مدیر'] = base_ezafe.apply(
    lambda row: calculate_sarane_refahi(row), axis=1)


base_ezafe.to_excel(
    r'E:\automating_reports_V2\automation\motamam1402\1403\ezafeRefahi\140301\اضافه کار و رفاهی خرداد 1403\ارسالی ادارات\تجمیع\final.xlsx')
