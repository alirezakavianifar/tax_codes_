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
from automation.helpers import maybe_make_dir, remove_excel_files, get_edare_shahr, get_update_date, measure_time
from automation.vportal_reports.vportal_helpers import *
from functools import reduce

YEARS = ['1401', '1400', '1399', '1398', '1397', '1396', '1395']


def rpt_tashkhis_ghatee_238(years=['1400.0'],
                            manabe=['مالیات بر درآمد مشاغل',
                                    'مالیات بر درآمد شرکت ها',
                                    'مالیات بر ارزش افزوده'], details=False):

    for year in years:

        sql_query = get_sql_v_portal(year=year)

        df = connect_to_sql(
            sql_query, sql_con=get_sql_con(database='testdbV2'), read_from_sql=True, return_df=True)

        df_tax_types = get_tax_types()

        df['ghatee'] = df['شماره برگ قطعی'].apply(
            lambda x: is_ghatee(x))
        df['tashkhis_eblagh'] = df['تاریخ ابلاغ برگ تشخیص'].apply(
            lambda x: is_ghatee(x))
        df['tashkhis'] = df['تاریخ صدور برگ تشخیص (تاریخ تایید گزارش حسابرسی)'].apply(
            lambda x: is_ghatee(x))
        df['manba'] = df['منبع مالیاتی'].apply(
            lambda x: map_work(x, df_tax_types))

        col_names = df.columns.tolist()

        # Different dfs for manba

        df_mash = df.loc[df['manba'] ==
                         'مالیات بر درآمد مشاغل']
        df_hoghogh = df.loc[df['manba'] ==
                            'مالیات بر درآمد شرکت ها']
        df_arzesh = df.loc[df['manba'] ==
                           'مالیات بر ارزش افزوده']

        # Results for Tashkhis

        res_mash_tash = df_mash['tashkhis'].loc[(df_mash['tashkhis'] == True)].count(
        ) / df_mash['tashkhis'].count() * 100

        res_hoghogh_tash = df_hoghogh['tashkhis'].loc[(
            df_hoghogh['tashkhis'] == True)].count() / df_hoghogh['tashkhis'].count() * 100

        res_arzesh_tash = df_arzesh['tashkhis'].loc[(df_arzesh['tashkhis'] == True)].count(
        ) / df_arzesh['tashkhis'].count() * 100

        res_mash_mal_tash = ((df_mash.loc[(df_mash['tashkhis'] == True)]['مالیات ابرازی'].astype(
            'float').sum()) / (df_mash['مالیات ابرازی'].astype('float').sum())) * 100

        res_hoghogh_mal_tash = ((df_hoghogh.loc[(df_hoghogh['tashkhis'] == True)]['مالیات ابرازی'].astype(
            'float').sum()) / (df_hoghogh['مالیات ابرازی'].astype('float').sum())) * 100

        res_arzesh_mal_tash = ((df_arzesh.loc[(df_arzesh['tashkhis'] == True)]['مالیات ابرازی'].astype(
            'float').sum()) / (df_arzesh['مالیات ابرازی'].astype('float').sum())) * 100

        # results for Ghatee

        res_mash_ghatee = df_mash['ghatee'].loc[(df_mash['ghatee'] == True)].count(
        ) / df_mash['ghatee'].count() * 100

        res_hoghogh_ghatee = df_hoghogh['ghatee'].loc[(
            df_hoghogh['ghatee'] == True)].count() / df_hoghogh['ghatee'].count() * 100

        res_arzesh_ghatee = df_arzesh['ghatee'].loc[(df_arzesh['ghatee'] == True)].count(
        ) / df_arzesh['ghatee'].count() * 100

        res_mash_mal_ghatee = ((df_mash.loc[(df_mash['ghatee'] == True)]['مالیات ابرازی'].astype(
            'float').sum()) / (df_mash['مالیات ابرازی'].astype('float').sum())) * 100

        res_hoghogh_mal_ghatee = ((df_hoghogh.loc[(df_hoghogh['ghatee'] == True)]['مالیات ابرازی'].astype(
            'float').sum()) / (df_hoghogh['مالیات ابرازی'].astype('float').sum())) * 100

        res_arzesh_mal_ghatee = ((df_arzesh.loc[(df_arzesh['ghatee'] == True)]['مالیات ابرازی'].astype(
            'float').sum()) / (df_arzesh['مالیات ابرازی'].astype('float').sum())) * 100

        # 238 results

        res_mash_238 = df_mash['توافق'].loc[(df_mash['توافق'] == 'Y')].count(
        ) / df_mash['tashkhis_eblagh'].loc[(df_mash['tashkhis_eblagh'] == True)].count() * 100

        res_hoghogh_238 = df_hoghogh['توافق'].loc[(
            df_hoghogh['توافق'] == 'Y')].count() / df_hoghogh['tashkhis_eblagh'].loc[(df_hoghogh['tashkhis_eblagh'] == True)].count() * 100

        res_arzesh_238 = df_arzesh['توافق'].loc[(df_arzesh['توافق'] == 'Y')].count(
        ) / df_arzesh['tashkhis_eblagh'].loc[(df_arzesh['tashkhis_eblagh'] == True)].count() * 100

        res_mash_mal_238 = ((df_mash.loc[(df_mash['توافق'] == 'Y')]['مالیات ابرازی'].astype(
            'float').sum()) / (df_mash.loc[(df_mash['tashkhis_eblagh'] == True)]['مالیات ابرازی'].astype('float').sum())) * 100

        res_hoghogh_mal_238 = ((df_hoghogh.loc[(df_hoghogh['توافق'] == 'Y')]['مالیات ابرازی'].astype(
            'float').sum()) / (df_hoghogh.loc[(df_hoghogh['tashkhis_eblagh'] == True)]['مالیات ابرازی'].astype('float').sum())) * 100

        res_arzesh_mal_238 = ((df_arzesh.loc[(df_arzesh['توافق'] == 'Y')]['مالیات ابرازی'].astype(
            'float').sum()) / (df_arzesh.loc[(df_arzesh['tashkhis_eblagh'] == True)]['مالیات ابرازی'].astype('float').sum())) * 100

        final_df = pd.DataFrame({'برگ تشخیص صادره مشاغل به کل اظهارنامه ها': [res_mash_tash],
                                'برگ تشخیص صادره حقوقی به کل اظهارنامه ها': [res_hoghogh_tash],
                                 'برگ تشخیص صادره ارزش افزوده به کل اظهارنامه ها': [res_arzesh_tash],
                                 'برگ قطعی صادره مشاغل به کل اظهارنامه ها': [res_mash_ghatee],
                                 'برگ قطعی صادره حقوقی به کل اظهارنامه ها': [res_hoghogh_ghatee],
                                 'برگ قطعی صادره ارزش افزوده به کل اظهارنامه ها': [res_arzesh_ghatee],
                                 'تعداد توافق صادره 238 مشاغل به تشخیص ابلاغ شده': [res_mash_238],
                                 'تعداد توافق صادره 238 حقوقی به تشخیص ابلاغ شده': [res_hoghogh_238],
                                 'تعداد توافق صادره 238 ارزش افزوده به تشخیص ابلاغ شده': [res_arzesh_238],
                                 'مالیات ابرازی تشخیص شده مشاغل به کل مالیات ابرازی': [res_mash_mal_tash],
                                 'مالیات ابرازی تشخیص شده حقوقی به کل مالیات ابرازی': [res_hoghogh_mal_tash],
                                 'مالیات ابرازی تشخیص شده ارزش افزوده به کل مالیات ابرازی': [res_arzesh_mal_tash],
                                 'مالیات ابرازی قطعی شده مشاغل به کل مالیات ابرازی': [res_mash_mal_ghatee],
                                 'مالیات ابرازی قطعی شده حقوقی به کل مالیات ابرازی': [res_hoghogh_mal_ghatee],
                                 'مالیات ابرازی قطعی شده ارزش افزوده به کل مالیات ابرازی': [res_arzesh_mal_ghatee],
                                 'مالیات توافق شده 238 مشاغل به مالیات تشخیصی ابلاغ شده': [res_mash_mal_238],
                                 'مالیات توافق شده 238 حقوقی به مالیات تشخیصی ابلاغ شده': [res_hoghogh_mal_238],
                                 'مالیات توافق شده 238 ارزش افزوده به مالیات تشخیصی ابلاغ شده': [res_arzesh_mal_238],
                                 'سال عملکرد': [year],
                                 'تاریخ بروزرسانی': [get_update_date()]
                                 })

        table_name = 'tbl_rpt_vportal_tashkhis_ghatee_238'

        drop_into_db(table_name,
                     final_df.columns.tolist(),
                     final_df.values.tolist(),
                     append_to_prev=True,
                     db_name='testdbV2')


def get_important_modis(years=YEARS, drop_db=True, group_by=None, path_dir=None):

    cols = get_sqltable_colnames(db_name='TestDb', tbl_name='V_PORTAL')

    for year in years:
        sql_query = get_sql_v_portal(
            year=year, cols='*')

        df = connect_to_sql(
            sql_query, sql_con=get_sql_con(database='TestDb'), read_from_sql=True, return_df=True)

        df = df[cols[1:]]

        df['پرونده مهم'] = df['کد رهگیری اظهارنامه سنیم - آخرین ورژن اعمال شد'].apply(
            lambda x: is_modi_important(str(x[-4:]), type='IMHR'))

        df_tax_types = get_tax_types()

        df['manba'] = df['منبع مالیاتی'].apply(
            lambda x: map_work(x, df_tax_types))

        df_imp_modi = df.loc[df['پرونده مهم'] == 'بلی']
        df_imp_modi['درآمد ابرازی'] = df_imp_modi['درآمد ابرازی'].astype(
            'float')

        df_imp_modi = df_imp_modi.sort_values(
            ['درآمد ابرازی'], ascending=False)

        df_moavenat = connect_to_sql(
            """SELECT [Moavenat]
                      ,[IrisCode]
                FROM [tax].[dbo].[tblEdareShahr]""",
            sql_con=get_sql_con(database='tax'), read_from_sql=True, return_df=True)

        final_df = df_imp_modi.merge(df_moavenat, how='left', left_on=[
                                     'کد اداره'], right_on=['IrisCode'])

        if drop_db:

            drop_into_db('tblImportant_modi',
                         final_df.columns.tolist(),
                         final_df.values.tolist(),
                         append_to_prev=True,
                         db_name='testdbV2')

        if group_by is not None:
            df_g = final_df.groupby(group_by)

            for key, item in df_g:
                item = item.sort_values(
                    ['درآمد ابرازی'], ascending=False).reset_index()
                item['درآمد ابرازی'] = item['درآمد ابرازی'].astype(
                    'str')

                dir = os.path.join(path_dir, f'{key}.xlsx')
                item.to_excel(dir)


# def detect_last_condition_(x):
#     if len(str(x['شماره برگ قطعی'])) > 5:
#         return 'برگ قطعی دارد'
#     if len(str(x['تاریخ اعتراض هیات تجدید نظر'])) > 5:
#         return 'هیات تجدید نظر'
#     if (len(str(x['تاریخ تایید رای'])) > 5 and (x['توافق'] == 'N')):
#         return 'هیات بدوی'
#     if len(str(x['شماره برگ تشخیص'])) > 5:
#         return 'دارای برگ تشخیص'

#     return 'فاقد تشخیص'

@measure_time
def sanim_data(year=None, n_days=257):
    dfs = []

    # for year in years[5:6]:

    sql_query = get_sql_query_sanimdata(year=year)

    df = connect_to_sql(
        sql_query, sql_con=get_sql_con(database='testdbV2'),
        read_from_sql=True, return_df=True)

    df['آخرین وضعیت'] = df['آخرین وضعیت'].fillna(
        value='نامشخص')

    cols = [col for col in df.columns.tolist() if '_' in col]

    # تعداد اظهارنامه ها به تفکیک منبع

    df = df[df['منبع مالیاتی'].isin(['مالیات بر درآمد مشاغل',
                                     'مالیات بر ارزش افزوده', 'مالیات بر درآمد شرکت ها'])]

    dfs_g = df.groupby(['نام اداره', 'کد اداره', 'منبع مالیاتی', 'نوع اظهارنامه']).size(
    ).reset_index().rename(columns={0: 'تعداد'})

    df_count = pd.pivot_table(dfs_g, values='تعداد',
                              index=['نام اداره', 'کد اداره',
                                     'نوع اظهارنامه'],
                              columns=['منبع مالیاتی'],
                              fill_value=0).reset_index().\
        set_index(['کد اداره', 'نوع اظهارنامه'])

    cols = df_count.columns.tolist()

    # تعداد تشخیص های صادره به تفکیک منبع

    df['رسیدگی شده'] = df['شماره برگ تشخیص'].apply(
        lambda x: is_residegi(x, 'رسیدگی'))

    dfs_residegi = df[df['رسیدگی شده']
                      == 'رسیدگی شده']
    dfs_residegi = dfs_residegi[dfs_residegi['شناسه حسابرسی'] != '0']

    dfs_g_rsidegi_manba = dfs_residegi.groupby(['نام اداره', 'کد اداره', 'منبع مالیاتی', 'نوع اظهارنامه']).size(
    ).reset_index().rename(columns={0: 'تعداد پرونده رسیدگی شده'})

    df_residegi_manba = pd.pivot_table(dfs_g_rsidegi_manba, values='تعداد پرونده رسیدگی شده',
                                       index=['نام اداره', 'کد اداره', 'نوع اظهارنامه'], columns=['منبع مالیاتی'], fill_value=0).\
        reset_index().set_index(['کد اداره', 'نوع اظهارنامه'])

    cols = df_residegi_manba.columns.tolist()

    for index, col in enumerate(cols):
        new_col = f'رسیدگی شده {col}'
        df_residegi_manba.rename(columns={col: new_col}, inplace=True)

    # تعداد تشخیص های ابلاغی به تفکیک منبع

    df['تشخیص ابلاغ شده'] = df['تاریخ ابلاغ برگ تشخیص'].apply(
        lambda x: is_residegi(x, 'تشخیص ابلاغ'))

    dfs_eblagh = df[df['تشخیص ابلاغ شده']
                    == 'تشخیص ابلاغ شده']

    dfs_eblagh = dfs_eblagh[dfs_eblagh['ابلاغ الکترونیک برگ تشخیص'] == 'خیر']

    dfs_g_eblagh_manba = dfs_eblagh.groupby(['نام اداره', 'کد اداره', 'منبع مالیاتی', 'نوع اظهارنامه']).size(
    ).reset_index().rename(columns={0: 'تعداد تشخیص ابلاغ شده'})

    df_eblagh_manba = pd.pivot_table(dfs_g_eblagh_manba, values='تعداد تشخیص ابلاغ شده',
                                     index=['نام اداره', 'کد اداره', 'نوع اظهارنامه'], columns=['منبع مالیاتی'], fill_value=0).\
        reset_index().set_index(['کد اداره', 'نوع اظهارنامه'])

    cols = df_eblagh_manba.columns.tolist()

    for index, col in enumerate(cols):
        new_col = f'ابلاغ شده {col}'
        df_eblagh_manba.rename(columns={col: new_col}, inplace=True)

    # تعداد تشخیص صادره در روزهای گذشته

    df[f'تشخیص صادره در {n_days} روز گذشته'] = dfs_residegi['تاریخ صدور برگ تشخیص (تاریخ تایید گزارش حسابرسی)'].\
        apply(lambda x: count_tash_done(x, n_days))

    df_tash_in_ndays = dfs_residegi[
        df[f'تشخیص صادره در {n_days} روز گذشته'] == True]

    dfs_g_tash_in_ndays_manba = df_tash_in_ndays.groupby(['نام اداره', 'کد اداره', 'منبع مالیاتی', 'نوع اظهارنامه']).size(
    ).reset_index().rename(columns={0: f'تعداد تشخیص صادره در {n_days} روز گذشته'})

    df_tash_in_ndays_manba = pd.pivot_table(dfs_g_tash_in_ndays_manba, values=f'تعداد تشخیص صادره در {n_days} روز گذشته',
                                            index=['نام اداره', 'کد اداره', 'نوع اظهارنامه'], columns=['منبع مالیاتی'], fill_value=0).\
        reset_index().set_index(['کد اداره', 'نوع اظهارنامه'])

    cols = df_tash_in_ndays_manba.columns.tolist()

    for index, col in enumerate(cols):
        new_col = f'تعداد تشخیص صادره {col} در {n_days} روز گذشته'
        df_tash_in_ndays_manba.rename(
            columns={col: new_col}, inplace=True)

    # تعداد تشخیص ابلاغی در روزهای گذشته

    df[f'تشخیص ابلاغی در {n_days} روز گذشته'] = df['تاریخ ابلاغ برگ تشخیص'].\
        apply(lambda x: count_tash_done(x, n_days))

    df_tash_eblagh_in_ndays = df[
        df[f'تشخیص ابلاغی در {n_days} روز گذشته'] == True]

    df_tash_eblagh_in_ndays = df_tash_eblagh_in_ndays[
        df_tash_eblagh_in_ndays['ابلاغ الکترونیک برگ تشخیص'] == 'خیر']

    dfs_g_tash_eblagh_in_ndays_manba = df_tash_eblagh_in_ndays.groupby(['نام اداره', 'کد اداره', 'منبع مالیاتی', 'نوع اظهارنامه']).size(
    ).reset_index().rename(columns={0: f'تعداد تشخیص ابلاغی در {n_days} روز گذشته'})

    df_tash_eblagh_in_ndays_manba = pd.pivot_table(dfs_g_tash_eblagh_in_ndays_manba, values=f'تعداد تشخیص ابلاغی در {n_days} روز گذشته',
                                                   index=['نام اداره', 'کد اداره', 'نوع اظهارنامه'], columns=['منبع مالیاتی'], fill_value=0).\
        reset_index().set_index(['کد اداره', 'نوع اظهارنامه'])

    cols = df_tash_eblagh_in_ndays_manba.columns.tolist()

    for index, col in enumerate(cols):
        new_col = f'تعداد تشخیص ابلاغی {col} در {n_days} روز گذشته'
        df_tash_eblagh_in_ndays_manba.rename(
            columns={col: new_col}, inplace=True)

    # تعداد قطعی های صادره به تفکیک منبع

    df['قطعی شده'] = df['شماره برگ قطعی'].apply(
        lambda x: is_residegi(x, 'قطعی'))

    dfs_ghatee_sadere = df[df['قطعی شده'] == 'قطعی شده']
    dfs_ghatee_sadere = dfs_ghatee_sadere[dfs_ghatee_sadere['شناسه حسابرسی'] != '0']

    dfs_g_ghatee_sadere_manba = dfs_ghatee_sadere.groupby(['نام اداره', 'کد اداره', 'منبع مالیاتی', 'نوع اظهارنامه']).size(
    ).reset_index().rename(columns={0: 'تعداد قطعی صادر شده'})

    df_ghatee_sadere_manba = pd.pivot_table(dfs_g_ghatee_sadere_manba, values='تعداد قطعی صادر شده',
                                            index=['نام اداره', 'کد اداره', 'نوع اظهارنامه'], columns=['منبع مالیاتی'], fill_value=0).\
        reset_index().set_index(['کد اداره', 'نوع اظهارنامه'])

    cols = df_ghatee_sadere_manba.columns.tolist()

    for index, col in enumerate(cols):
        new_col = f'قطعی شده {col}'
        df_ghatee_sadere_manba.rename(
            columns={col: new_col}, inplace=True)

    # تعداد قطعی های ابلاغی به تفکیک منبع

    df['قطعی ابلاغ شده'] = df['تاریخ ابلاغ برگ قطعی'].apply(
        lambda x: is_residegi(x, 'قطعی ابلاغ'))

    dfs_ghatee_eblaghi = df[df['قطعی ابلاغ شده']
                            == 'قطعی ابلاغ شده']

    dfs_ghatee_eblaghi = dfs_ghatee_eblaghi[dfs_ghatee_eblaghi['ابلاغ الکترونیک برگ قطعی'] == 'خیر']

    dfs_g_ghatee_eblagh_manba = dfs_ghatee_eblaghi.groupby(['نام اداره', 'کد اداره', 'منبع مالیاتی', 'نوع اظهارنامه']).size(
    ).reset_index().rename(columns={0: 'تعداد قطعی ابلاغ شده'})

    df_ghatee_eblagh_manba = pd.pivot_table(dfs_g_ghatee_eblagh_manba, values='تعداد قطعی ابلاغ شده',
                                            index=['نام اداره', 'کد اداره', 'نوع اظهارنامه'], columns=['منبع مالیاتی'], fill_value=0).\
        reset_index().set_index(['کد اداره', 'نوع اظهارنامه'])

    cols = df_ghatee_eblagh_manba.columns.tolist()

    for index, col in enumerate(cols):
        new_col = f'قطعی ابلاغ شده {col}'
        df_ghatee_eblagh_manba.rename(
            columns={col: new_col}, inplace=True)

    # تعداد قطعی صادره در روزهای گذشته

    df[f'قطعی صادره در {n_days} روز گذشته'] = dfs_ghatee_sadere['تاريخ ايجاد برگ قطعي'].\
        apply(lambda x: count_tash_done(x, n_days))

    df_ghatee_in_ndays = dfs_ghatee_sadere[
        df[f'قطعی صادره در {n_days} روز گذشته'] == True]

    dfs_g_ghatee_in_ndays_manba = df_ghatee_in_ndays.groupby(['نام اداره', 'کد اداره', 'منبع مالیاتی', 'نوع اظهارنامه']).size(
    ).reset_index().rename(columns={0: f'تعداد قطعی صادره در {n_days} روز گذشته'})

    df_ghatee_in_ndays_manba = pd.pivot_table(dfs_g_ghatee_in_ndays_manba, values=f'تعداد قطعی صادره در {n_days} روز گذشته',
                                              index=['نام اداره', 'کد اداره', 'نوع اظهارنامه'], columns=['منبع مالیاتی'], fill_value=0).\
        reset_index().set_index(['کد اداره', 'نوع اظهارنامه'])

    cols = df_ghatee_in_ndays_manba.columns.tolist()

    for index, col in enumerate(cols):
        new_col = f'تعداد قطعی صادره {col} در {n_days} روز گذشته'
        df_ghatee_in_ndays_manba.rename(
            columns={col: new_col}, inplace=True)

    # تعداد قطعی ابلاغی در روزهای گذشته

    df[f'قطعی ابلاغی در {n_days} روز گذشته'] = df['تاریخ ابلاغ برگ قطعی'].\
        apply(lambda x: count_tash_done(x, n_days))

    df_ghatee_eblaghi_in_ndays = df[
        df[f'قطعی ابلاغی در {n_days} روز گذشته'] == True]

    df_ghatee_eblaghi_in_ndays = df_ghatee_eblaghi_in_ndays[
        df_ghatee_eblaghi_in_ndays['ابلاغ الکترونیک برگ قطعی'] == 'خیر']

    dfs_g_ghatee_eblaghi_in_ndays_manba = df_ghatee_eblaghi_in_ndays.groupby(['نام اداره', 'کد اداره', 'منبع مالیاتی', 'نوع اظهارنامه']).size(
    ).reset_index().rename(columns={0: f'تعداد قطعی ابلاغی در {n_days} روز گذشته'})

    df_ghatee_eblaghi_in_ndays_manba = pd.pivot_table(dfs_g_ghatee_eblaghi_in_ndays_manba, values=f'تعداد قطعی ابلاغی در {n_days} روز گذشته',
                                                      index=['نام اداره', 'کد اداره', 'نوع اظهارنامه'], columns=['منبع مالیاتی'], fill_value=0).\
        reset_index().set_index(['کد اداره', 'نوع اظهارنامه'])

    cols = df_ghatee_eblaghi_in_ndays_manba.columns.tolist()

    for index, col in enumerate(cols):
        new_col = f'تعداد قطعی ابلاغی {col} در {n_days} روز گذشته'
        df_ghatee_eblaghi_in_ndays_manba.rename(
            columns={col: new_col}, inplace=True)

    # تعداد توافق در روزهای گذشته

    df_tavafogh = df[df['توافق'] == 'Y']

    df_tavafogh[f'توافق در {n_days} روز گذشته'] = df_tavafogh['تاریخ تایید رای'].\
        apply(lambda x: count_tash_done(x, n_days))

    df_tavafogh_in_ndays = df_tavafogh[
        df_tavafogh[f'توافق در {n_days} روز گذشته'] == True]

    dfs_g_tavafogh_in_ndays_manba = df_tavafogh_in_ndays.groupby(['نام اداره', 'کد اداره', 'منبع مالیاتی', 'نوع اظهارنامه']).size(
    ).reset_index().rename(columns={0: f'تعداد توافق در {n_days} روز گذشته'})

    df_tavafogh_in_ndays_manba = pd.pivot_table(dfs_g_tavafogh_in_ndays_manba, values=f'تعداد توافق در {n_days} روز گذشته',
                                                index=['نام اداره', 'کد اداره', 'نوع اظهارنامه'], columns=['منبع مالیاتی'], fill_value=0).\
        reset_index().set_index(['کد اداره', 'نوع اظهارنامه'])

    cols = df_tavafogh_in_ndays_manba.columns.tolist()

    for index, col in enumerate(cols):
        new_col = f'تعداد توافق {col} در {n_days} روز گذشته'
        df_tavafogh_in_ndays_manba.rename(
            columns={col: new_col}, inplace=True)

    df_merged = reduce(lambda left, right: pd.merge(left, right, left_index=True, right_index=True,
                                                    how='outer'), [
        # df_count,
        #    df_residegi_manba,
        #    df_eblagh_manba,
        #    df_ghatee_sadere_manba,
        #    df_ghatee_eblagh_manba,
        df_tash_in_ndays_manba,
        df_tash_eblagh_in_ndays_manba,
        df_ghatee_in_ndays_manba,
        df_ghatee_eblaghi_in_ndays_manba,
        df_tavafogh_in_ndays_manba
    ])

    return df_merged

# وضعیت رسیدگی اطلاعیه تبصره 6 ارزش افزوده


@measure_time
def arzeshi(df_merged):
    sql_query = """
        SELECT 
            [testdbV2].[dbo].[V_PORTAL].[نام اداره],
            [testdbV2].[dbo].[V_PORTAL].[کد اداره]
            ,[testdbV2].[dbo].[V_PORTAL].[منبع مالیاتی]
            ,[testdb].[dbo].[TblArzeshAfzodeEtelaeiye].[نوع شخصیت]
            , N'ريسک متوسط' AS [نوع اظهارنامه]
        FROM
            [testdb].[dbo].[TblArzeshAfzodeEtelaeiye]
            join
            [testdbV2].[dbo].[V_PORTAL]
            ON [testdb].[dbo].[TblArzeshAfzodeEtelaeiye].[کدرهگیری ثبت نام]=[testdbV2].[dbo].[V_PORTAL].[کد رهگيري ثبت نام ]
            AND [testdb].[dbo].[TblArzeshAfzodeEtelaeiye].[سال]=[testdbV2].[dbo].[V_PORTAL].[سال عملکرد]
            AND [testdb].[dbo].[TblArzeshAfzodeEtelaeiye].[دوره]=[testdbV2].[dbo].[V_PORTAL].[دوره عملکرد]
            AND [testdbV2].[dbo].[V_PORTAL].[منبع مالیاتی]=N'مالیات بر ارزش افزوده'
            WHERE [تاریخ صدور برگ تشخیص (تاریخ تایید گزارش حسابرسی)] BETWEEN '1402/01/01' AND '1402/12/29'
            OR [تاريخ ايجاد برگ قطعي] BETWEEN '1402/01/01' AND '1402/12/29'
        """

    df_arzesh_tab6 = connect_to_sql(
        sql_query, sql_con=get_sql_con(database='testdbV2'),
        read_from_sql=True, return_df=True)
    df_arzesh_tab6['نوع اظهارنامه'] = 'ريسک متوسط'

    df_arzesh_tab6 = df_arzesh_tab6.groupby(['نام اداره', 'کد اداره', 'نوع شخصیت', 'نوع اظهارنامه']).size(
    ).reset_index().rename(columns={0: f'تعداد رسیدگی اطلاعیه چهارم و پنجم ارزش افزوده- تبصره 6'})

    df_arzesh_tab6 = pd.pivot_table(df_arzesh_tab6, values=f'تعداد رسیدگی اطلاعیه چهارم و پنجم ارزش افزوده- تبصره 6',
                                    index=['نام اداره', 'کد اداره', 'نوع اظهارنامه'], columns=['نوع شخصیت'], fill_value=0).\
        reset_index().set_index(['کد اداره', 'نوع اظهارنامه']).\
        rename(columns={'مالیات بر ارزش افزوده':
                        f'تعداد رسیدگی اطلاعیه چهارم و پنجم ارزش افزوده- تبصره 6'})

    df_merged_ = reduce(lambda left, right: pd.merge(left, right, left_index=True, right_index=True,
                                                     how='outer'), [df_merged,
                                                                    df_arzesh_tab6,
                                                                    ])
    return df_merged_


# مانده های 97
@measure_time
def mande97(df_merged_):

    sql_query = """
                SELECT 
                IrisCode AS [کد اداره]
                ,N'ریسک متوسط'AS [نوع اظهارنامه]
                ,COUNT(*) AS [جمع کل مانده]
                FROM
                [KARKARD].[dbo].[tblBardasht97NEW]
                LEFT JOIN
                tax.[dbo].[tblEdareShahr]
                ON 
                tax.[dbo].[udf_GetNumeric]([اداره-شهر])=tax.[dbo].[tblEdareShahr].edare
                GROUP BY [اداره-شهر],IrisCode
                HAving IrisCode IS NOT NULL AND IrisCode<>9999
    """

    df_mande = connect_to_sql(
        sql_query, sql_con=get_sql_con(database='testdbV2'),
        read_from_sql=True, return_df=True).dropna(subset=['کد اداره'])
    df_mande['نوع اظهارنامه'] = 'ريسک متوسط'

    df_mande['کد اداره'] = df_mande['کد اداره'].replace(
        '.0', '')
    df_mande.set_index(
        ['کد اداره', 'نوع اظهارنامه'], inplace=True)

    df_merged__ = reduce(lambda left, right: pd.merge(left, right, left_index=True, right_index=True,
                                                      how='outer'), [df_merged_,
                                                                     df_mande,
                                                                     ])

    return df_merged__


# گزارش تشخیص و قطعی های مستغلات
@measure_time
def get_tash_ghat_most(df_merged__):

    sql_query = get_sql_tash_ghat_most()

    df_tash_most = connect_to_sql(
        sql_query, sql_con=get_sql_con(database='tax'),
        read_from_sql=True, return_df=True).rename(columns={'IrisCode': 'کد اداره', 'تعداد': 'تعداد تشخیص مستغلات'}).\
        reset_index()
    df_tash_most['نوع اظهارنامه'] = 'ريسک متوسط'
    df_tash_most.set_index(['کد اداره', 'نوع اظهارنامه'], inplace=True)

    df_merged__ = reduce(lambda left, right: pd.merge(left, right, left_index=True, right_index=True,
                                                      how='outer'), [df_merged__,
                                                                     df_tash_most,
                                                                     ])
    return df_merged__


# گزارش حقوقی سنتی تشخیص و قطعی
@measure_time
def get_tash_ghat_hoghoghi_sonati(df_merged__):

    sql_query = get_sql_hoghoghi_sonati_tash_ghat()

    df_hoghoghi_sonati_tash_ghat = connect_to_sql(
        sql_query, sql_con=get_sql_con(database='tax'),
        read_from_sql=True, return_df=True).rename(columns={'IrisCode': 'کد اداره'}).\
        reset_index()
    df_hoghoghi_sonati_tash_ghat['نوع اظهارنامه'] = 'ريسک متوسط'
    df_hoghoghi_sonati_tash_ghat.set_index(
        ['کد اداره', 'نوع اظهارنامه'], inplace=True)

    df_merged__ = reduce(lambda left, right: pd.merge(left, right, left_index=True, right_index=True,
                                                      how='outer'), [df_merged__,
                                                                     df_hoghoghi_sonati_tash_ghat,
                                                                     ])
    return df_merged__


# گزارش حقوقی سنتی توافق 238
@measure_time
def get_hoghoghi_sonati238(df_merged__):
    sql_query = get_sql_hoghigh_sonati_238()

    df_hoghigh_sonati_238 = connect_to_sql(
        sql_query, sql_con=get_sql_con(database='tax'),
        read_from_sql=True, return_df=True).rename(columns={'IrisCode': 'کد اداره'}).\
        reset_index()
    df_hoghigh_sonati_238['نوع اظهارنامه'] = 'ريسک متوسط'
    df_hoghigh_sonati_238.set_index(
        ['کد اداره', 'نوع اظهارنامه'], inplace=True)

    df_merged__ = reduce(lambda left, right: pd.merge(left, right, left_index=True, right_index=True,
                                                      how='outer'), [df_merged__,
                                                                     df_hoghigh_sonati_238,
                                                                     ])
    return df_merged__


def get_sodor_eblagh(years=['1401', '1400', '1399', '1398'], n_days=257):
    '''
    گزارش تشخیص و قطعی های صادر شده و ابلاغ شده
    '''
    x = Scrape()
    project_root = os.path.dirname(os.path.dirname(__file__))
    path_mostaghelat_ezhar = os.path.join(
        project_root, r'saved_dir\mostaghelat\mostaghelat_ezhar')
    path_mostaghelat_residegi = os.path.join(
        project_root, r'saved_dir\mostaghelat\mostaghelat_residegi')
    maybe_make_dir([path_mostaghelat_residegi])
    remove_excel_files(file_path=path_mostaghelat_ezhar, postfix=['xls'])
    remove_excel_files(file_path=path_mostaghelat_residegi, postfix=['xls'])

    df_ezhar = x.scrape_mostaghelat(path=path_mostaghelat_ezhar, scrape=True,
                                    report_type='Ezhar', table_name='tblEzharMost', read_from_repo=False,
                                    drop_to_sql=False, append_to_prev=False, return_df=True,
                                    del_prev_files=True, headless=False)

    df_ezhar['وضعیت رسیدگی'] = df_ezhar['تاریخ برگ تشخیص'].apply(
        lambda x: is_residegi(str(x)))

    df_ezhar.replace('nan', 0, inplace=True)
    df_ezhar['اداره'] = df_ezhar['اداره'].astype(
        str).astype(float).astype(int)

    df_edare_shahr = get_edare_shahr()
    df_edare_shahr['edare'] = df_edare_shahr['edare'].astype(int)

    df_ezhar_edare = df_ezhar.merge(
        df_edare_shahr, how='left', left_on='اداره', right_on='edare')

    df_ezhar_edare['تشخیص صادر شده مستغلات در هفته جاری'] = \
        df_ezhar_edare['تاریخ برگ تشخیص'].\
        apply(lambda x: count_tash_done(str(x), n_days))

    df_ezhar_mostahelat_in_n_days = df_ezhar_edare[df_ezhar_edare[
        'تشخیص صادر شده مستغلات در هفته جاری'] == True]

    df_ezhar_mostahelat_in_n_days = df_ezhar_mostahelat_in_n_days.groupby(['IrisCode']).\
        size().reset_index().rename(columns={
            0: 'تعداد تشخیص صادر شده مستغلات در هفته جاری'})

    df_ezhar_edare_g = df_ezhar_edare.groupby(['IrisCode', 'وضعیت رسیدگی']).\
        size().reset_index().rename(columns={0: 'تعداد'})

    df_ezhar_edare_g = pd.pivot_table(df_ezhar_edare_g, values='تعداد',
                                      index=['IrisCode'], columns=['وضعیت رسیدگی'], aggfunc='sum',
                                      fill_value=0, margins=True, margins_name='انباره مستغلات').\
        reset_index().drop(labels=['رسیدگی نشده'], axis=1).\
        rename(
            columns={'رسیدگی شده': 'رسیدگی شده مستغلات'})

    df_ezhar_edare_g = df_ezhar_edare_g.merge(df_ezhar_mostahelat_in_n_days,
                                              how='left', left_on='IrisCode', right_on='IrisCode').\
        fillna(0).iloc[:-1, :]

    df_final = res.merge(df_ezhar_edare_g, how='left',
                         left_on='کد اداره', right_on='IrisCode').\
        fillna(0).drop(['IrisCode'], axis=1)

    df_final['جمع انباره'] = (df_final['انباره مالیات بر ارزش افزوده'] +
                              df_final['انباره مالیات بر درآمد شرکت ها'] +
                              df_final['انباره مالیات بر درآمد مشاغل'] +
                              df_final['انباره مستغلات']).astype(int)

    df_final['جمع تشخیص صادره'] = (df_final['رسیدگی شده مستغلات'] + df_final['رسیدگی شده مالیات بر درآمد مشاغل'] +
                                   df_final['رسیدگی شده مالیات بر درآمد شرکت ها'] + df_final['رسیدگی شده مالیات بر ارزش افزوده']).astype(int)

    df_final['پیشرفت حسابرسی مستغلات'] = (df_final['رسیدگی شده مستغلات'] / df_final['انباره مستغلات']).\
        apply(lambda x: format_percentage(x))
    df_final['پیشرفت حسابرسی مشاغل'] = (df_final['رسیدگی شده مالیات بر درآمد مشاغل'] / df_final['انباره مالیات بر درآمد مشاغل']).\
        apply(lambda x: format_percentage(x))
    df_final['پیشرفت حسابرسی شرکت ها'] = (df_final['رسیدگی شده مالیات بر درآمد شرکت ها'] / df_final['انباره مالیات بر درآمد شرکت ها']).\
        apply(lambda x: format_percentage(x))
    df_final['پیشرفت حسابرسی ارزش افزوده'] = (df_final['رسیدگی شده مالیات بر ارزش افزوده'] / df_final['انباره مالیات بر ارزش افزوده']).\
        apply(lambda x: format_percentage(x))
    df_final['پیشرفت حسابرسی کل'] = (df_final['جمع تشخیص صادره'] / df_final['جمع انباره']).\
        apply(lambda x: format_percentage(x))
    df_final['امتیاز وزنی مستغلات رسیدگی شده'] = (
        df_final['رسیدگی شده مستغلات'] * 1)
    df_final['امتیاز وزنی مشاغل رسیدگی شده'] = (
        df_final['رسیدگی شده مالیات بر درآمد مشاغل'] * 2)
    df_final['امتیاز وزنی شرکت ها رسیدگی شده'] = (
        df_final['رسیدگی شده مالیات بر درآمد شرکت ها'] * 4)
    df_final['امتیاز وزنی ارزش افزوده رسیدگی شده'] = (
        df_final['رسیدگی شده مالیات بر ارزش افزوده'] * 1)
    df_final['امتیاز وزنی رسیدگی شده کل'] = df_final['امتیاز وزنی مستغلات رسیدگی شده'] + \
        df_final['امتیاز وزنی مشاغل رسیدگی شده'] + df_final['امتیاز وزنی شرکت ها رسیدگی شده'] + \
        df_final['امتیاز وزنی ارزش افزوده رسیدگی شده']

    df_final['وزن کل انباره'] = df_final['انباره مستغلات'] + df_final['انباره مالیات بر ارزش افزوده'] + \
        (df_final['انباره مالیات بر درآمد مشاغل'] * 2) + \
        (df_final['انباره مالیات بر درآمد شرکت ها'] * 4)

    df_final['درصد کسب امتیاز از کل'] = (df_final['امتیاز وزنی رسیدگی شده کل'] / df_final['وزن کل انباره']).\
        apply(lambda x: format_percentage(x))

    cols = ['نام اداره', 'کد اداره', 'انباره مالیات بر درآمد مشاغل', 'انباره مالیات بر درآمد شرکت ها',
            'انباره مالیات بر ارزش افزوده', 'انباره مستغلات', 'جمع انباره',
            'رسیدگی شده مالیات بر درآمد مشاغل', 'رسیدگی شده مالیات بر درآمد شرکت ها',
            'رسیدگی شده مالیات بر ارزش افزوده', 'رسیدگی شده مستغلات',
            'تعداد تشخیص صادره در هفته جاری مالیات بر درآمد شرکت ها',
            'تعداد تشخیص صادره در هفته جاری مالیات بر ارزش افزوده',
            'تعداد تشخیص صادره در هفته جاری مالیات بر درآمد مشاغل',
            'تعداد تشخیص صادر شده مستغلات در هفته جاری',
            'جمع تشخیص صادره',
            'پیشرفت حسابرسی مشاغل', 'پیشرفت حسابرسی شرکت ها',
            'پیشرفت حسابرسی ارزش افزوده', 'پیشرفت حسابرسی مستغلات', 'پیشرفت حسابرسی کل',
            'امتیاز وزنی مستغلات رسیدگی شده', 'امتیاز وزنی مشاغل رسیدگی شده',
            'امتیاز وزنی شرکت ها رسیدگی شده', 'امتیاز وزنی ارزش افزوده رسیدگی شده',
            'امتیاز وزنی رسیدگی شده کل', 'وزن کل انباره', 'درصد کسب امتیاز از کل']

    df_final = df_final[cols]

    df_final['آخرین بروزرسانی'] = get_update_date()

    df_final.replace('nan%', '0.00%', inplace=True)

    drop_into_db('tblResidegiAnbare',
                 df_final.columns.tolist(),
                 df_final.values.tolist(),
                 append_to_prev=False,
                 db_name='testdbV2')


def get_anbare_residegi(years=['1401'], n_days=7):
    '''
    گزارش انباره رسیدگی مستغلات،مشاغل، شرکت ها و ارزش افزوده عملکرد 1401
    '''
    dfs = []

    for year in years:

        sql_query = """
                    SELECT [نام مودی],[شماره اقتصادی],[دوره عملکرد],
                    [منبع مالیاتی],[کد رهگیری اظهارنامه سنیم - آخرین ورژن اعمال شد],
                    [شماره برگ تشخیص], [تاریخ صدور برگ تشخیص (تاریخ تایید گزارش حسابرسی)],
                    [تاریخ ابلاغ برگ تشخیص],[شماره برگ قطعی], [تاریخ ابلاغ برگ قطعی],
                    [سال عملکرد],[نام اداره], [کد اداره], [آخرین وضعیت]
                    FROM [testdbV2].[dbo].[V_PORTAL]
                    where [سال عملکرد] = '%s'     
                    """ % year

        df = connect_to_sql(
            sql_query, sql_con=get_sql_con(database='testdbV2'),
            read_from_sql=True, return_df=True)

        df_arzesh = df[df['منبع مالیاتی']
                       == 'مالیات بر ارزش افزوده']

        df['آخرین وضعیت'] = df['آخرین وضعیت'].fillna(
            value='نامشخص')

        df['نوع اظهارنامه'] = df['کد رهگیری اظهارنامه سنیم - آخرین ورژن اعمال شد'].apply(
            lambda x: get_ezhar_type(str(x)))

        df = df[df['نوع اظهارنامه'].isin(['اظهارنامه برآوردی صفر', 'رتبه ریسک بالا',
                                          'ریسک متوسط', "مودیان مهم با ریسک بالا"])]

        df_merge = df.merge(
            df_arzesh, how='inner', left_on='شماره اقتصادی', right_on='شماره اقتصادی')

        cols = [col for col in df_merge.columns.tolist() if '_' in col]

        for col in cols:
            df_merge.rename(columns={col: col.split('_')[0]}, inplace=True)

        df_merge = df_merge.loc[:, ~df_merge.columns.duplicated(keep='last')]

        dfs.append(pd.concat([df, df_merge]))

    dfs = pd.concat(dfs)

    dfs['تشخیص صادر شده در هفته جاری'] = dfs['تاریخ صدور برگ تشخیص (تاریخ تایید گزارش حسابرسی)'].\
        apply(lambda x: count_tash_done(x, n_days))

    dfs_g = dfs.groupby(['نام اداره', 'کد اداره', 'منبع مالیاتی']).size(
    ).reset_index().rename(columns={0: 'تعداد'})

    # df_count = pd.pivot_table(dfs_g, values='تعداد',
    #                           index=['نام اداره', 'کد اداره'], columns=['منبع مالیاتی'], aggfunc='sum',
    #                           fill_value=0, margins=True, margins_name='جمع کلی')

    df_count = pd.pivot_table(dfs_g, values='تعداد',
                              index=['نام اداره', 'کد اداره'], columns=['منبع مالیاتی'], fill_value=0)

    cols = df_count.columns.tolist()

    for index, col in enumerate(cols):
        new_col = f'انباره {col}'
        df_count.rename(columns={col: new_col}, inplace=True)

    dfs['رسیدگی شده'] = dfs['شماره برگ تشخیص'].apply(
        lambda x: is_residegi(x, 'رسیدگی'))

    dfs_residegi = dfs[dfs['رسیدگی شده'] == 'رسیدگی شده']

    dfs_g_rsidegi = dfs_residegi.groupby(['نام اداره', 'کد اداره', 'منبع مالیاتی']).size(
    ).reset_index().rename(columns={0: 'تعداد پرونده رسیدگی شده'})

    # df_residegi = pd.pivot_table(dfs_g_rsidegi, values='تعداد پرونده رسیدگی شده',
    #                              index=['نام اداره', 'کد اداره'], columns=['منبع مالیاتی'], aggfunc='sum',
    #                              fill_value=0, margins=True, margins_name='جمع کلی')

    df_residegi = pd.pivot_table(dfs_g_rsidegi, values='تعداد پرونده رسیدگی شده',
                                 index=['نام اداره', 'کد اداره'], columns=['منبع مالیاتی'], fill_value=0)

    cols = df_residegi.columns.tolist()

    for index, col in enumerate(cols):
        new_col = f'رسیدگی شده {col}'
        df_residegi.rename(columns={col: new_col}, inplace=True)

    # تعداد تشخیص صادره در هفته جاری

    df_tash_in_ndays = dfs[dfs['تشخیص صادر شده در هفته جاری'] == True]

    dfs_g_tash_in_ndays = df_tash_in_ndays.groupby(['نام اداره', 'کد اداره', 'منبع مالیاتی']).size(
    ).reset_index().rename(columns={0: 'تعداد تشخیص صادر شده در هفته جاری'})

    df_tash_in_ndays = pd.pivot_table(dfs_g_tash_in_ndays, values='تعداد تشخیص صادر شده در هفته جاری',
                                      index=['نام اداره', 'کد اداره'], columns=['منبع مالیاتی'], fill_value=0)

    cols = df_tash_in_ndays.columns.tolist()

    for index, col in enumerate(cols):
        new_col = f'تعداد تشخیص صادره در هفته جاری {col}'
        df_tash_in_ndays.rename(columns={col: new_col}, inplace=True)

    res = pd.concat([df_count, df_residegi, df_tash_in_ndays], axis=1)

    res = res.reset_index()

    x = Scrape()
    project_root = os.path.dirname(os.path.dirname(__file__))
    path_mostaghelat_ezhar = os.path.join(
        project_root, r'saved_dir\mostaghelat\mostaghelat_ezhar')
    path_mostaghelat_residegi = os.path.join(
        project_root, r'saved_dir\mostaghelat\mostaghelat_residegi')
    maybe_make_dir([path_mostaghelat_residegi])
    remove_excel_files(file_path=path_mostaghelat_ezhar, postfix=['xls'])
    remove_excel_files(file_path=path_mostaghelat_residegi, postfix=['xls'])

    df_ezhar = x.scrape_mostaghelat(path=path_mostaghelat_ezhar, scrape=True,
                                    report_type='Ezhar', table_name='tblEzharMost', read_from_repo=False,
                                    drop_to_sql=False, append_to_prev=False, return_df=True,
                                    del_prev_files=True, headless=False)

    df_ezhar['وضعیت رسیدگی'] = df_ezhar['تاریخ برگ تشخیص'].apply(
        lambda x: is_residegi(str(x), 'رسیدگی'))

    df_ezhar.replace('nan', 0, inplace=True)
    df_ezhar['اداره'] = df_ezhar['اداره'].astype(
        str).astype(float).astype(int)

    df_edare_shahr = get_edare_shahr()
    df_edare_shahr['edare'] = df_edare_shahr['edare'].astype(int)

    df_ezhar_edare = df_ezhar.merge(
        df_edare_shahr, how='left', left_on='اداره', right_on='edare')

    df_ezhar_edare['تشخیص صادر شده مستغلات در هفته جاری'] = \
        df_ezhar_edare['تاریخ برگ تشخیص'].\
        apply(lambda x: count_tash_done(str(x), n_days))

    df_ezhar_mostahelat_in_n_days = df_ezhar_edare[df_ezhar_edare[
        'تشخیص صادر شده مستغلات در هفته جاری'] == True]

    df_ezhar_mostahelat_in_n_days = df_ezhar_mostahelat_in_n_days.groupby(['IrisCode']).\
        size().reset_index().rename(columns={
            0: 'تعداد تشخیص صادر شده مستغلات در هفته جاری'})

    df_ezhar_edare_g = df_ezhar_edare.groupby(['IrisCode', 'وضعیت رسیدگی']).\
        size().reset_index().rename(columns={0: 'تعداد'})

    df_ezhar_edare_g = pd.pivot_table(df_ezhar_edare_g, values='تعداد',
                                      index=['IrisCode'], columns=['وضعیت رسیدگی'], aggfunc='sum',
                                      fill_value=0, margins=True, margins_name='انباره مستغلات').\
        reset_index().drop(labels=['رسیدگی نشده'], axis=1).\
        rename(
            columns={'رسیدگی شده': 'رسیدگی شده مستغلات'})

    df_ezhar_edare_g = df_ezhar_edare_g.merge(df_ezhar_mostahelat_in_n_days,
                                              how='left', left_on='IrisCode', right_on='IrisCode').\
        fillna(0).iloc[:-1, :]

    df_final = res.merge(df_ezhar_edare_g, how='left',
                         left_on='کد اداره', right_on='IrisCode').\
        fillna(0).drop(['IrisCode'], axis=1)

    df_final['جمع انباره'] = (df_final['انباره مالیات بر ارزش افزوده'] +
                              df_final['انباره مالیات بر درآمد شرکت ها'] +
                              df_final['انباره مالیات بر درآمد مشاغل'] +
                              df_final['انباره مستغلات']).astype(int)

    df_final['جمع تشخیص صادره'] = (df_final['رسیدگی شده مستغلات'] + df_final['رسیدگی شده مالیات بر درآمد مشاغل'] +
                                   df_final['رسیدگی شده مالیات بر درآمد شرکت ها'] + df_final['رسیدگی شده مالیات بر ارزش افزوده']).astype(int)

    df_final['پیشرفت حسابرسی مستغلات'] = (df_final['رسیدگی شده مستغلات'] / df_final['انباره مستغلات']).\
        apply(lambda x: format_percentage(x))
    df_final['پیشرفت حسابرسی مشاغل'] = (df_final['رسیدگی شده مالیات بر درآمد مشاغل'] / df_final['انباره مالیات بر درآمد مشاغل']).\
        apply(lambda x: format_percentage(x))
    df_final['پیشرفت حسابرسی شرکت ها'] = (df_final['رسیدگی شده مالیات بر درآمد شرکت ها'] / df_final['انباره مالیات بر درآمد شرکت ها']).\
        apply(lambda x: format_percentage(x))
    df_final['پیشرفت حسابرسی ارزش افزوده'] = (df_final['رسیدگی شده مالیات بر ارزش افزوده'] / df_final['انباره مالیات بر ارزش افزوده']).\
        apply(lambda x: format_percentage(x))
    df_final['پیشرفت حسابرسی کل'] = (df_final['جمع تشخیص صادره'] / df_final['جمع انباره']).\
        apply(lambda x: format_percentage(x))
    df_final['امتیاز وزنی مستغلات رسیدگی شده'] = (
        df_final['رسیدگی شده مستغلات'] * 1)
    df_final['امتیاز وزنی مشاغل رسیدگی شده'] = (
        df_final['رسیدگی شده مالیات بر درآمد مشاغل'] * 2)
    df_final['امتیاز وزنی شرکت ها رسیدگی شده'] = (
        df_final['رسیدگی شده مالیات بر درآمد شرکت ها'] * 4)
    df_final['امتیاز وزنی ارزش افزوده رسیدگی شده'] = (
        df_final['رسیدگی شده مالیات بر ارزش افزوده'] * 1)
    df_final['امتیاز وزنی رسیدگی شده کل'] = df_final['امتیاز وزنی مستغلات رسیدگی شده'] + \
        df_final['امتیاز وزنی مشاغل رسیدگی شده'] + df_final['امتیاز وزنی شرکت ها رسیدگی شده'] + \
        df_final['امتیاز وزنی ارزش افزوده رسیدگی شده']

    df_final['وزن کل انباره'] = df_final['انباره مستغلات'] + df_final['انباره مالیات بر ارزش افزوده'] + \
        (df_final['انباره مالیات بر درآمد مشاغل'] * 2) + \
        (df_final['انباره مالیات بر درآمد شرکت ها'] * 4)

    df_final['درصد کسب امتیاز از کل'] = (df_final['امتیاز وزنی رسیدگی شده کل'] / df_final['وزن کل انباره']).\
        apply(lambda x: format_percentage(x))

    cols = ['نام اداره', 'کد اداره', 'انباره مالیات بر درآمد مشاغل', 'انباره مالیات بر درآمد شرکت ها',
            'انباره مالیات بر ارزش افزوده', 'انباره مستغلات', 'جمع انباره',
            'رسیدگی شده مالیات بر درآمد مشاغل', 'رسیدگی شده مالیات بر درآمد شرکت ها',
            'رسیدگی شده مالیات بر ارزش افزوده', 'رسیدگی شده مستغلات',
            'تعداد تشخیص صادره در هفته جاری مالیات بر درآمد شرکت ها',
            'تعداد تشخیص صادره در هفته جاری مالیات بر ارزش افزوده',
            'تعداد تشخیص صادره در هفته جاری مالیات بر درآمد مشاغل',
            'تعداد تشخیص صادر شده مستغلات در هفته جاری',
            'جمع تشخیص صادره',
            'پیشرفت حسابرسی مشاغل', 'پیشرفت حسابرسی شرکت ها',
            'پیشرفت حسابرسی ارزش افزوده', 'پیشرفت حسابرسی مستغلات', 'پیشرفت حسابرسی کل',
            'امتیاز وزنی مستغلات رسیدگی شده', 'امتیاز وزنی مشاغل رسیدگی شده',
            'امتیاز وزنی شرکت ها رسیدگی شده', 'امتیاز وزنی ارزش افزوده رسیدگی شده',
            'امتیاز وزنی رسیدگی شده کل', 'وزن کل انباره', 'درصد کسب امتیاز از کل']

    df_final = df_final[cols]

    df_final['آخرین بروزرسانی'] = get_update_date()

    df_final.replace('nan%', '0.00%', inplace=True)

    drop_into_db('tblResidegiAnbare',
                 df_final.columns.tolist(),
                 df_final.values.tolist(),
                 append_to_prev=False,
                 db_name='testdbV2')


def get_important_parvandes():

    df_dfs = []
    sql_query = """
                SELECT *
                FROM [testdbV2].[dbo].[V_PORTAL]
                where [پرونده مهم] = N'بلی'     
                """

    df = connect_to_sql(
        sql_query, sql_con=get_sql_con(database='testdbV2'),
        read_from_sql=True, return_df=True)

    cols = df.columns.tolist()

    df = df.replace('nan', 0)

    [col for col in cols if 'بده' in col]

    df['سال عملکرد'] = df['سال عملکرد'].apply(
        np.float).apply(np.int64)

    years = df['سال عملکرد'].unique().tolist()
    manabe = df['منبع مالیاتی'].unique().tolist()

    for year in years:

        for manba in manabe:
            dfs = []
            df_year_manba = df.loc[(df['سال عملکرد'] == year) &
                                   (df['منبع مالیاتی'] == manba)]

            df_year_manba.replace('nan', 0, inplace=True)

            df_year_manba['مالیات تشخیص'] = df_year_manba['مالیات تشخیص'].apply(
                np.float).apply(np.int64)

            df_year_manba['مالیات ابرازی'] = df_year_manba['مالیات ابرازی'].apply(
                np.float).apply(np.int64)

            df_year_manba['مالیات قطعی'] = df_year_manba['مالیات قطعی'].apply(
                np.float).apply(np.int64)

            cols = df_year_manba.columns.tolist()

            # تعداد کل اظهارنامه های مالیاتی دریافت شده
            df_ezhar_all = df_year_manba.groupby(['نام اداره', 'کد اداره']).size(
            ).reset_index().rename(columns={0: 'تعداد کل اظهارنامه مالیاتی دریافت شده'})

            dfs.append(df_ezhar_all)

            # تعداد شناسه حسابرسی صادر شده
            df_shenase_count = df_year_manba.loc[(df_year_manba['شناسه حسابرسی'].str.len() > 5)].\
                groupby(['نام اداره', 'کد اداره']).size().reset_index().\
                rename(
                    columns={0: 'تعداد شناسه حسابرسی صادر شده'})
            df_shenase_count['تعداد شناسه حسابرسی صادر شده'] = \
                df_shenase_count['تعداد شناسه حسابرسی صادر شده'].astype(
                    str)
            dfs.append(df_shenase_count)

            # تعداد برگ تشخیص صادر شده
            df_tash_sadershode = df_year_manba.loc[(df_year_manba['شماره برگ تشخیص'].str.len() > 5)].\
                groupby(['نام اداره', 'کد اداره']).size().reset_index().\
                rename(
                    columns={0: 'تعداد برگ تشخیص صادر شده'})

            df_tash_sadershode['تعداد برگ تشخیص صادر شده'] = df_tash_sadershode['تعداد برگ تشخیص صادر شده'].\
                astype(str)
            dfs.append(df_tash_sadershode)

            # میزان مالیات ابرازی
            df_mal_ebraz = df_year_manba.groupby(['نام اداره', 'کد اداره'])['مالیات ابرازی'].sum(
            ).reset_index().rename(columns={'مالیات ابرازی': 'میزان مالیات ابرازی'})

            df_mal_ebraz['میزان مالیات ابرازی'] = df_mal_ebraz['میزان مالیات ابرازی'].astype(
                str)

            dfs.append(df_mal_ebraz)

            #  میزان مالیات ابرازی پرونده های دارای برگ تشخیص
            df_tash = df_year_manba.loc[(
                df_year_manba['شماره برگ تشخیص'].str.len() > 5)]
            df_tash.replace('nan', 0, inplace=True)
            df_tash['مالیات تشخیص'] = df_tash['مالیات تشخیص'].apply(
                np.float).apply(np.int64)
            df_mal_ebraz_whohave_tash = df_tash\
                .groupby(['نام اداره', 'کد اداره'])['مالیات ابرازی'].sum(
                ).reset_index().rename(columns={'مالیات ابرازی': 'میزان مالیات ابرازی پرونده های دارای برگ تشخیص'})

            df_mal_ebraz_whohave_tash['میزان مالیات ابرازی پرونده های دارای برگ تشخیص'] = \
                df_mal_ebraz_whohave_tash['میزان مالیات ابرازی پرونده های دارای برگ تشخیص'].apply(
                np.float).apply(np.int64)

            #  میزان مالیات تشخیصی پرونده های دارای برگ تشخیص
            df_mal_tashkhis_whohave_tash = df_tash\
                .groupby(['نام اداره', 'کد اداره'])['مالیات تشخیص'].sum(
                ).reset_index().rename(columns={'مالیات تشخیص': 'میزان مالیات تشخیصی پرونده های دارای برگ تشخیص'})

            df_mal_tashkhis_whohave_tash['میزان مالیات تشخیصی پرونده های دارای برگ تشخیص'] = \
                df_mal_tashkhis_whohave_tash['میزان مالیات تشخیصی پرونده های دارای برگ تشخیص'].apply(
                np.float).apply(np.int64)

            #  میزان مالیات تشخیصی پرونده های دارای برگ تشخیص

            df_ebraz_tash_tash_merge = df_mal_ebraz_whohave_tash.merge(df_mal_tashkhis_whohave_tash,
                                                                       how='outer',
                                                                       left_on=[
                                                                           'کد اداره', 'نام اداره'],
                                                                       right_on=['کد اداره', 'نام اداره'])

            # درصد افزایش ملیات تشخیصی پرونده های دارای برگ تشخیص به مالیات ابرازی آنها

            df_ebraz_tash_tash_merge['درصدافزایش مالیات تشخیصی پرونده های دارای برگ تشخیص به مالیات ابرازی آنها'] = \
                ((df_ebraz_tash_tash_merge['میزان مالیات تشخیصی پرونده های دارای برگ تشخیص'] -
                 df_ebraz_tash_tash_merge['میزان مالیات ابرازی پرونده های دارای برگ تشخیص']) /
                 df_ebraz_tash_tash_merge['میزان مالیات ابرازی پرونده های دارای برگ تشخیص']).\
                apply(lambda x: format_percentage(x))

            df_ebraz_tash_tash_merge['میزان مالیات تشخیصی پرونده های دارای برگ تشخیص'] = \
                df_ebraz_tash_tash_merge['میزان مالیات تشخیصی پرونده های دارای برگ تشخیص'].astype(
                    str)

            df_ebraz_tash_tash_merge['میزان مالیات ابرازی پرونده های دارای برگ تشخیص'] = \
                df_ebraz_tash_tash_merge['میزان مالیات ابرازی پرونده های دارای برگ تشخیص'].astype(
                    str)

            dfs.append(df_ebraz_tash_tash_merge)

            # درصد میزان مالیات تشخیصی به کل برگ تشخیص

            # تعدا برگ تشخیص ابلاغ شده
            dfs.append(df_year_manba.loc[(df_year_manba['تاریخ ابلاغ برگ تشخیص'].str.len() > 5)].
                       groupby(['نام اداره', 'کد اداره']).size().reset_index().
                       rename(columns={0: 'تعداد برگ تشخیص ابلاغ شده'}))

            # تعداد توافق انجام شده در اجرای ماده 238
            dfs.append(df_year_manba[(df_year_manba['توافق'] == 'Y')].
                       groupby(['نام اداره', 'کد اداره']).size().reset_index().
                       rename(columns={0: 'تعداد توافق انجام شده در اجرای ماده 238 ق.م.م'}))

            # تعداد پرونده های ارسالی به هیات حل اختلاف مالیاتی
            dfs.append(df_year_manba.loc[(df_year_manba['تاریخ اعتراض هیات بدوی'].str.len() > 5)].
                       groupby(['نام اداره', 'کد اداره']).size().reset_index().
                       rename(columns={0: 'تعداد پرونده های ارسالی به هیات حل اختلاف مالیاتی'}))

            # تعداد قطعی بر اساس سکوت مودی
            dfs.append(df_year_manba.loc[(df_year_manba['شماره برگ قطعی'].str.len() > 5) &
                                         (df_year_manba['شماره برگ تمکین'].str.len() < 5) &
                                         (df_year_manba['تاریخ اعتراض هیات بدوی'].str.len() < 5) &
                                         (df_year_manba['تاریخ اعتراض'].str.len() < 5)].
                       groupby(['نام اداره', 'کد اداره']).size().reset_index().
                       rename(columns={0: 'تعداد قطعی بر اساس سکوت مودی'}))

            # تعداد قطعی بر اساس تمکین مودی
            dfs.append(df_year_manba.loc[(df_year_manba['شماره برگ قطعی'].str.len() > 5) &
                                         (df_year_manba['شماره برگ تمکین'].str.len() > 5)].
                       groupby(['نام اداره', 'کد اداره']).size().reset_index().
                       rename(columns={0: 'تعداد قطعی بر اساس تمکین مودی'}))

            # تعداد قطعی بر مبنای ماده 238 ق.م.م
            dfs.append(df_year_manba.loc[(df_year_manba['شماره برگ قطعی'].str.len() > 5) &
                                         (df_year_manba['توافق'] == 'Y')].
                       groupby(['نام اداره', 'کد اداره']).size().reset_index().
                       rename(columns={0: 'تعداد قطعی بر مبنای ماده 238 ق.م.م'}))

            # تعداد قطعی بر مبنای رای بدوی
            dfs.append(df_year_manba.loc[(df_year_manba['شماره برگ قطعی'].str.len() > 5) &
                                         (df_year_manba['تاریخ رای بدوی'].str.len() > 5) &
                                         (df_year_manba['تاریخ رای تجدید نظر'].str.len(
                                         ) < 5)
                                         ].
                       groupby(['نام اداره', 'کد اداره']).size().reset_index().
                       rename(columns={0: 'تعداد قطعی بر مبنای رای بدوی'}))

            # تعداد قطعی بر مبنای رای تجدید نظر
            dfs.append(df_year_manba.loc[(df_year_manba['شماره برگ قطعی'].str.len() > 5) &
                                         (df_year_manba['تاریخ رای بدوی'].str.len() > 5) &
                                         (df_year_manba['تاریخ رای تجدید نظر'].str.len(
                                         ) > 5)
                                         ].
                       groupby(['نام اداره', 'کد اداره']).size().reset_index().
                       rename(columns={0: 'تعداد قطعی بر مبنای رای تجدید نظر'}))

            # نسبت تعداد رای های صادره هیات به کل پرونده های ارسالی به هیات

            df_heiat = df_year_manba.loc[(df_year_manba['تاریخ اعتراض هیات بدوی'].str.len() > 5) &
                                         (df_year_manba['تاریخ رای بدوی'].str.len() < 5)].\
                groupby(['نام اداره', 'کد اداره']).size().reset_index().\
                rename(
                    columns={0: 'تعداد پرونده های ارسالی به هیات'})

            df_ray = df_year_manba.loc[(df_year_manba['تاریخ اعتراض هیات بدوی'].str.len() > 5) &
                                       (df_year_manba['تاریخ رای بدوی'].str.len() > 5)].\
                groupby(['نام اداره', 'کد اداره']).size().reset_index().\
                rename(
                    columns={0: 'تعداد رای های صادره هیات'})

            df_heait_ray = df_heiat.merge(df_ray, how='left',
                                          left_on=['نام اداره', 'کد اداره'], right_on=['نام اداره', 'کد اداره'])

            df_heait_ray['نسبت تعداد رای های صادره هیات به کل پرونده های ارسالی به هیات'] = \
                (df_heait_ray['تعداد رای های صادره هیات'] / df_heait_ray['تعداد پرونده های ارسالی به هیات']).\
                apply(lambda x: format_percentage(x))

            dfs.append(df_heait_ray)

            # تعداد برگ قطعی صادر شده
            df_all_ghatee = df_year_manba.loc[(df_year_manba['شماره برگ قطعی'].str.len() > 5)].\
                groupby(['نام اداره', 'کد اداره']).size().reset_index().\
                rename(
                    columns={0: 'تعداد برگ قطعی صادر شده'})

            # درصد برگ قطعی صادر شده

            df_ezhar_ghatee = df_ezhar_all.merge(df_all_ghatee, how='left',
                                                 left_on=['نام اداره', 'کد اداره'], right_on=['نام اداره', 'کد اداره'])

            df_ezhar_ghatee['درصد برگ قطعی صادر شده'] = (df_ezhar_ghatee['تعداد برگ قطعی صادر شده'] /
                                                         df_ezhar_ghatee['تعداد کل اظهارنامه مالیاتی دریافت شده']).\
                apply(lambda x: format_percentage(x))

            dfs.append(df_ezhar_ghatee)

            # میزان مالیات ابرازی پرونده های دارای برگ قطعی

            df_mal_ebraz = df_year_manba.loc[(df_year_manba['شماره برگ قطعی'].str.len() > 5)]. \
                groupby(['نام اداره', 'کد اداره'])['مالیات ابرازی'].sum().reset_index(). \
                rename(
                columns={'مالیات ابرازی': 'میزان مالیات ابرازی پرونده های دارای برگ قطعی'})

            # میزان مالیات تشخیصی پرونده های دارای برگ قطعی

            df_mal_tash = df_year_manba.loc[(df_year_manba['شماره برگ قطعی'].str.len() > 5)]. \
                groupby(['نام اداره', 'کد اداره'])['مالیات تشخیص'].sum().reset_index(). \
                rename(
                columns={'مالیات تشخیص': 'میزان مالیات تشخیصی پرونده های دارای برگ قطعی'})

            # درصد افزایش مالیات تشخیص پرونده های دارای برگ قطعی به مالیات ابرازی آنها

            df_mal_ebraz_tash = df_mal_ebraz.merge(df_mal_tash, how='left',
                                                   left_on=[
                                                       'نام اداره', 'کد اداره'],
                                                   right_on=['نام اداره', 'کد اداره'])

            df_mal_ebraz_tash['درصد افزایش مالیات تشخیص پرونده های دارای برگ قطعی به مالیات ابرازی آنها'] = \
                ((df_mal_ebraz_tash['میزان مالیات تشخیصی پرونده های دارای برگ قطعی'].apply(
                    np.float).apply(np.int64) - df_mal_ebraz_tash['میزان مالیات ابرازی پرونده های دارای برگ قطعی']) /
                 df_mal_ebraz_tash['میزان مالیات ابرازی پرونده های دارای برگ قطعی']).\
                apply(lambda x: format_percentage(x))

            df_mal_ebraz_tash['میزان مالیات ابرازی پرونده های دارای برگ قطعی'] = \
                df_mal_ebraz_tash['میزان مالیات ابرازی پرونده های دارای برگ قطعی'].apply(
                np.float).apply(np.int64)

            df_mal_ebraz_tash['میزان مالیات تشخیصی پرونده های دارای برگ قطعی'] = \
                df_mal_ebraz_tash['میزان مالیات تشخیصی پرونده های دارای برگ قطعی'].apply(
                np.float).apply(np.int64)

            dfs.append(df_mal_ebraz_tash)

            # میزان مالیات قطعی
            df_year_manba['مالیات قطعی'] = df_year_manba['مالیات قطعی'].apply(
                np.float).apply(np.int64)
            df_mal_ghatee = df_year_manba.loc[(df_year_manba['شماره برگ قطعی'].str.len() > 5)]. \
                groupby(['نام اداره', 'کد اداره'])['مالیات قطعی'].sum().reset_index(). \
                rename(
                columns={'مالیات قطعی': 'میزان مالیات قطعی'})

            df_mal_ghatee['میزان مالیات قطعی'] = df_mal_ghatee['میزان مالیات قطعی'].astype(
                str)

            dfs.append(df_mal_ghatee)

            df_end = pd.concat(dfs, axis=1)

            df_end = df_end.loc[:, ~df_end.columns.duplicated()]

            df_end.fillna('0', inplace=True)

            df_end.replace('nan%', '0', inplace=True)
            df_end.replace('inf%', '0', inplace=True)

            df_end['سال عملکرد'] = year
            df_end['منبع مالیاتی'] = manba

            df_dfs.append(df_end)

    df_final = pd.concat(df_dfs)
    df_final['آخرین بروزرسانی'] = get_update_date()

    table_name = 'tblParvandehayeMohem'

    drop_into_db(table_name,
                 df_final.columns.tolist(),
                 df_final.values.tolist(),
                 append_to_prev=False,
                 db_name='testdbV2')


app = FastAPI()
origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
async def main():
    try:
        res = get_anbare_residegi()
        return "done"
    except:
        return "Error"


if __name__ == "__main__":

    # uvicorn.run(app, host="0.0.0.0", port=8000)

    # res = get_anbare_residegi()
    # res = get_sodor_eblagh()
    df_merged = sanim_data(n_days=268)
    df_merged_ = arzeshi(df_merged)
    df_merged__ = mande97(df_merged_)
    df_merged__ = get_tash_ghat_most(df_merged__)
    df_merged__ = get_tash_ghat_hoghoghi_sonati(df_merged__)
    df_merged__ = get_hoghoghi_sonati238(df_merged__)
    print('f')

    # get_important_parvandes()
