import os
import math
import pandas as pd
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
import jdatetime
from automation.helpers import read_multiple_excel_files, connect_to_sql, get_update_date, drop_into_db, get_sqltable_colnames
import numpy as np
from automation.sql_queries import get_sql_v_portal, get_tax_types, get_sql_query_sanimdata, \
    get_sql_hoghoghi_sonati_tash_ghat, get_sql_tash_ghat_most, get_sql_hoghigh_sonati_238
from automation.constants import get_sql_con
from automation.scrape import Scrape
from automation.helpers import maybe_make_dir, remove_excel_files, get_edare_shahr, get_update_date, measure_time, list_files
from automation.vportal_reports.vportal_helpers import *
from automation.mashaghelsonati import get_mashaghelsonati
from functools import reduce


def drop_duplicate_columns(df):
    columns = df.columns
    duplicate_columns = [
        col for col in columns if columns.tolist().count(col) > 1]
    df = df.loc[:, ~df.columns.duplicated()]
    return df, duplicate_columns


def ensure_unique_column_names(dfs):
    seen_columns = set()
    for df in dfs:
        new_columns = []
        for col in df.columns:
            new_col = col
            suffix = 1
            while new_col in seen_columns:
                new_col = f"{col}_{suffix}"
                suffix += 1
            seen_columns.add(new_col)
            new_columns.append(new_col)
        df.columns = new_columns
    return dfs


files = list_files(
    directory=r'E:\automating_reports_V2\automation\vportal_reports\queryforarzyabi', extension='sql')

results = []


df_edare = connect_to_sql(
    "SELECT * FROM tblEdareShahr", sql_con=get_sql_con(database='tax'),
    read_from_sql=True, return_df=True)

for file in files:
    # Read the SQL query from file
    try:
        with open(file, 'r', encoding='utf-8') as sql_file:
            from_date = '1403/04/25'
            to_date = '1403/05/24'
            sql_query = sql_file.read()
            sql_query = sql_query.lstrip('\ufeff')
            sql_query = sql_query.replace('1403/01/25', from_date)
            sql_query = sql_query.replace('1403/02/25', to_date)

            if file.split('\\')[-1] == 'تعداد پرونده ارسالی به هیات سنتی1.sql':
                mash_sql_query = sql_query
                df_mash,  _ = get_mashaghelsonati(mashaghel_type=None,
                                                  date=None,
                                                  eblagh=True,
                                                  save_on_folder=False,
                                                  saved_folder='saved_folder',
                                                  save_how='db',
                                                  return_df=False,
                                                  sql_query=mash_sql_query,
                                                  database='COMMISSION',
                                                  username='commission_user',
                                                  password='babataher')
                df_mash, _ = drop_duplicate_columns(df_mash)
                drop_into_db('tblMashCommission',
                             df_mash.columns.tolist(),
                             df_mash.values.tolist(),
                             append_to_prev=False,
                             db_name='TestDb')
                continue

        df = connect_to_sql(
            sql_query, sql_con=get_sql_con(database='TestDb'),
            read_from_sql=True, return_df=True)

        df.set_index(['کد اداره'], inplace=True)
        df, _ = drop_duplicate_columns(df)

        results.append(df)

    except Exception as e:
        print(e)

# Ensure unique column names
results = ensure_unique_column_names(results)

df_merged = reduce(lambda left, right: pd.merge(left, right, left_index=True, right_index=True,
                                                how='outer'), results)

df_merged = df_merged.merge(
    df_edare, how='left', left_on='کد اداره', right_on='IrisCode')
# final_df = pd.concat(results, axis=1)
cols = ['city', 'edare', '238هایی که منجر به قطعی شده اند',
        'تشخیص قطعی شده بدون اعتراض و هیات-سنیم',
        'تعداد پرونده های ارسال به هیات سنتی',
        'تعداد پرونده ارسالی به هیات سنیم',
        'حسابرسی ارزش افزوده 1401',
        'حسابرسی انباره شرکت ها 1401',
        'حسابرسی انباره مشاغل1401', 'رسیدگی حقوق 1397-1401',
        'قطعی سازی 100 پرونده دادستانی',
        'قطعی سازی اوراق دارای تشخیص',
        'تشخیص صادره ارزش افزوده سال 1397',
        'تشخیص صادره حقیقی و حقوقی 1397']
df_merged = df_merged[cols]
df_merged.to_excel(
    r'E:\automating_reports_V2\automation\vportal_reports\queryforarzyabi\final.xlsx')
