from concurrent.futures import ThreadPoolExecutor, wait, as_completed
from automation.helpers import get_tblreports_date, connect_to_sql
from automation.constants import get_table_names, geck_location
from automation.sql_queries import sql_delete, create_sql_table, insert_into
import schedule
import time
from automation.sql_queries import get_sql_mashaghelsonati, get_sql_mashaghelsonati_ghatee, get_sql_mashaghelsonati_tashkhisEblaghNoGhatee, \
    get_sql_mashaghelsonati_ghateeEblaghNashode, get_sql_mashaghelsonati_tashkhisEblaghNashode, \
    get_sql_mashaghelsonati_amadeghatee, get_sql_mashaghelsonati_amadeersalbeheiat, \
    sql_delete, create_sql_table, insert_into, get_tblupdateDate
from automation.constants import geck_location, set_gecko_prefs, get_remote_sql_con, get_sql_con, get_str_help, get_comm_reports, \
    get_heiat, get_lst_reports, get_all_years, get_common_years, get_str_years, get_years, get_common_reports, \
    get_comm_years, get_heiat_reports, get_server_namesV2
from tqdm import tqdm
import pandas as pd
from automation.helpers import get_update_date, check_if_col_exists, drop_into_db, find_edares, log_the_func

saved_folder = geck_location(set_save_dir=False)
eblagh = False
date = 140106
TIME_TO_RUN = '08:02'
RUN_ALL = True


def done(func):
    def try_it(*args, **kwargs):
        result = func(*args, **kwargs)
        print('done')
        return result
    return try_it


@done
@log_the_func('none')
def get_tashkhis_ghatee_sonati(date=date, eblagh=eblagh,
                               saved_folder=saved_folder,
                               return_df=False, field=None,
                               *args, **kwargs):
    #
    if eblagh == False:
        params = ['Amade_ghatee', 'Amade_heiat',
                  'tashkhis', 'tashkhis', 'ghatee']
        results = []
        dates = [date, date, date, date, date]
        eblaghs = [eblagh, eblagh, eblagh, True, eblagh]
        saved_folders = [saved_folder, saved_folder,
                         saved_folder, saved_folder, saved_folder]

        with ThreadPoolExecutor(len(params)) as executor:
            for result in executor.map(get_mashaghelsonati, params, dates, eblaghs, saved_folders):
                results.append(result)
        if return_df:
            return tashkhis_saderNashode, agg_tashkhis_saderNashode, ghatee_saderNashode, agg_ghatee_saderNashode, amade_ersal_beheiat, agg_amade_ersal_beheiat, tashkhis_eblagh_noGhatee, agg_tashkhis_eblagh_noGhatee, agg_amade_ghatee

    else:

        tashkhis_sadere, tashkhis_eblaghi = get_mashaghelsonati(
            'tashkhis', date=date, eblagh=eblagh)
        ghatee_sadere, ghatee_eblaghi = get_mashaghelsonati(
            'ghatee', date=date, eblagh=eblagh)
        # tashkhis_sadere.to_excel(
        #     '%s/mashaghelSonati_tashkhisSadere.xlsx' % saved_folder)
        # ghatee_sadere.to_excel(
        #     '%s/mashaghelSonati_ghateeSadere.xlsx' % saved_folder)
        # tashkhis_eblaghi.to_excel(
        #     '%s/mashaghelSonati_tashkhisEblaghi.xlsx' % saved_folder)
        # ghatee_eblaghi.to_excel(
        #     '%s/mashaghelSonati_ghateeEblaghi.xlsx' % saved_folder)
        if return_df:
            return tashkhis_sadere, ghatee_sadere, tashkhis_eblaghi, ghatee_eblaghi


def dump_to_sql():

    table_names = [
        ('tblMashaghelSonatiTashkhisSadere', tashkhis_sadere),
        ('tblMashaghelSonatiTashkhisEblaghi', tashkhis_eblaghi),
        ('tblMashaghelSonatiGhateeSadere', ghatee_sadere),
        ('tblMashaghelSonatiGhateeEblaghi', ghatee_eblaghi)
    ]

    for item in table_names:

        sql_delete_query = sql_delete(item[0])
        connect_to_sql(sql_query=sql_delete_query,
                       connect_type='dropping sql table')

        sql_create_table_query = create_sql_table(item[0], item[1].columns)
        connect_to_sql(sql_query=sql_create_table_query,
                       connect_type='creating sql table')

        sql_insert = insert_into(item[0], item[1].columns)
        connect_to_sql(sql_query=sql_insert, df_values=item[1].values.tolist(
        ), connect_type='inserting into sql table')


def get_mashaghelsonati(mashaghel_type,
                        date=None,
                        eblagh=True,
                        save_on_folder=False,
                        saved_folder=saved_folder,
                        save_how='db',
                        return_df=False,
                        sql_query=None,
                        database='MASHAGHEL',
                        username='mash',
                        password='123456',
                        preprecess=True):

    i = 0
    j = 24

    # Specify sql query
    if sql_query is None:
        if (mashaghel_type == "Amade_ghatee" and eblagh == False):
            sql_query = get_sql_mashaghelsonati_amadeghatee(date='14020701')
        if (mashaghel_type == "tashkhis" and eblagh == False):
            sql_query = get_sql_mashaghelsonati_tashkhisEblaghNashode()
        elif (mashaghel_type == "ghatee" and eblagh == False):
            sql_query = get_sql_mashaghelsonati_ghateeEblaghNashode()
        elif mashaghel_type == "tashkhis":
            sql_query = get_sql_mashaghelsonati_tashkhisEblaghNoGhatee()
        elif mashaghel_type == "Amade_heiat":
            sql_query = get_sql_mashaghelsonati_amadeersalbeheiat(
                date='14030201')
        elif mashaghel_type == "ghatee":
            sql_query = get_sql_mashaghelsonati_ghatee()

    # Go through each server one by one
    servers = get_server_namesV2()
    all_df = []

    for i, server in tqdm(enumerate(servers[i:j])):
        df = connect_to_sql(sql_query,
                            sql_con=get_sql_con(server=server[1],
                                                database=database,
                                                username=username,
                                                password=password),
                            read_from_sql=True,
                            connect_type='',
                            return_df=True)
        all_df.append(df)
        i += 1
    # merge all dfs
    df_all = pd.concat(all_df)
    df_all['تاریخ بروزرسانی'] = get_update_date()
    if preprecess:
        df_all_cols = df_all.columns.tolist()
        if 'کد اداره' not in (df_all_cols):
            df_all = df_all.astype(str)
            df_all = find_edares(df_all,
                                 colname_tomerge='حوزه',
                                 persian_name='شماره ثبت برگ تشخیص')
        # preprocess and create final df
        if mashaghel_type in ['Amade_heiat', 'Amade_ghatee']:

            agg_heiat = df_all.groupby(['کد اداره', 'شهرستان',
                                        'تاریخ بروزرسانی']).size()
            agg_heiat = agg_heiat.reset_index()
            agg_heiat['تاریخ بروزرسانی'] = get_update_date()

            agg_heiat.rename(columns={0: 'تعداد آماده ارسال به هیات مشاغل سنتی'},
                             inplace=True)
            # if save_on_folder:
            #     file_name = 'amade_ersal_beheiat_details.xlsx'
            #     output_dir = os.path.join(saved_folder, file_name)
            #     df_all.to_excel(output_dir)
            #     file_name = 'amade_ersal_beheiat_agg.xlsx'
            #     output_dir = os.path.join(saved_folder, file_name)
            #     agg_heiat.to_excel(output_dir)
            if save_how == 'db':
                drop_into_db(table_name='tbl%sMashSonati' % (mashaghel_type),
                             columns=df_all.columns.tolist(),
                             values=df_all.values.tolist(),
                             append_to_prev=False,
                             db_name='testdbV2')
            agg_heiat = agg_heiat.rename(
                columns={'کد اداره': 'نام اداره سنتی'})
            if return_df:
                return df_all, agg_heiat

        if (check_if_col_exists(df_all, 'تاريخ ثبت برگ تشخيص')):
            df_all['ماه صدور تشخیص'] = df_all['تاريخ ثبت برگ تشخيص'].str.slice(
                0, 6).astype('int64')

            if eblagh:
                df_all['ماه ابلاغ تشخیص'] = df_all[
                    'تاريخ ابلاغ برگ تشخيص'].str.slice(0, 6)
                if date is not None:
                    df_all = df_all.loc[
                        df_all['ماه ابلاغ تشخیص'].astype('int64') < date]
                # df_all_sodor = df_all.loc[df_all['ماه صدور تشخیص'] == '%s%s' % (year, month) ]
                # df_all_eblagh = df_all.loc[df_all['ماه ابلاغ تشخیص'] == '%s%s' % (year, month) ]
                agg_tashkhis_sodor = df_all.groupby(
                    ['کد اداره', 'شهرستان', 'ماه صدور تشخیص',
                     'تاریخ بروزرسانی']).size()
                agg_tashkhis_eblagh = df_all.groupby(
                    ['کد اداره', 'شهرستان', 'تاریخ بروزرسانی']).size()
                agg_tashkhis_sodor = agg_tashkhis_sodor.reset_index(level=0)
                agg_tashkhis_eblagh = agg_tashkhis_eblagh.reset_index()
                agg_tashkhis_sodor['تاریخ بروزرسانی'] = get_update_date(
                )
                agg_tashkhis_eblagh['تاریخ بروزرسانی'] = get_update_date(
                )
                agg_tashkhis_sodor = agg_tashkhis_sodor.reset_index(level=0)
                agg_tashkhis_sodor = agg_tashkhis_sodor.reset_index(level=0)
                agg_tashkhis_eblagh.rename(
                    columns={0: 'تعداد تشخیص ابلاغی مشاغل سنتی'}, inplace=True)
                agg_tashkhis_sodor.rename(
                    columns={0: 'تعداد تشخیص صادره مشاغل سنتی'}, inplace=True)

                # if save_on_folder:

                #     file_name_details_t_sadere = 'tashkhis_saderShode_details.xlsx'
                #     file_name_details = 'tashkhis_sadervaeblaghi_details.xlsx'
                #     file_name_agg_t_sadere = 'tashkhis_saderShode_agg.xlsx'
                #     file_name_details_t_eblaghi = 'tashkhis_eblaghshode_details.xlsx'
                #     file_name_agg_t_eblaghi = 'tashkhis_eblaghshode_agg.xlsx'
                #     output_dir = os.path.join(saved_folder, file_name_details)
                #     df_all.to_excel(output_dir)
                #     output_dir = os.path.join(saved_folder, file_name_agg_t_sadere)
                #     agg_tashkhis_sodor.to_excel(output_dir)
                #     output_dir = os.path.join(
                #         saved_folder, file_name_agg_t_eblaghi)
                #     agg_tashkhis_eblagh.to_excel(output_dir)

                if save_how == 'db':
                    drop_into_db(table_name='tblTashkhisEblaghNoGhatee',
                                 columns=df_all.columns.tolist(),
                                 values=df_all.values.tolist(),
                                 append_to_prev=False,
                                 db_name='testdbV2')
                # return df_all, agg_tashkhis_sodor, agg_tashkhis_eblagh
                agg_tashkhis_eblagh = agg_tashkhis_eblagh.rename(
                    columns={'کد اداره': 'نام اداره سنتی'})
                if return_df:
                    return df_all, agg_tashkhis_eblagh

            else:
                if date is not None:
                    df_all = df_all.loc[df_all['ماه صدور تشخیص'] < date]

                agg_tashkhis_sodor = df_all.groupby(
                    ['کد اداره', 'شهرستان', 'تاریخ بروزرسانی']).size()
                agg_tashkhis_sodor = agg_tashkhis_sodor.reset_index()
                agg_tashkhis_sodor['تاریخ بروزرسانی'] = get_update_date(
                )
                agg_tashkhis_sodor.rename(
                    columns={0: 'تعداد تشخیص ابلاغ نشده مشاغل سنتی'}, inplace=True)
                if save_how == 'db':
                    drop_into_db(table_name='tblTashkhisSodorNoElagh',
                                 columns=df_all.columns.tolist(),
                                 values=df_all.values.tolist(),
                                 append_to_prev=False,
                                 db_name='testdbV2')

                agg_tashkhis_sodor = agg_tashkhis_sodor.rename(
                    columns={'کد اداره': 'نام اداره سنتی'})

                if return_df:
                    return df_all, agg_tashkhis_sodor

        if (check_if_col_exists(df_all, 'تاريخ ثبت برگ قطعي')):
            df_all['ماه صدور قطعی'] = df_all['تاريخ ثبت برگ قطعي'].str.slice(
                0, 6).astype('int64')
            if eblagh:
                df_all['ماه ابلاغ قطعی'] = df_all[
                    'تاریخ ابلاغ برگ قطعی'].str.slice(0, 6)
                # df_all_sodor = df_all.loc[df_all['ماه صدور قطعی'] == '%s%s' % (year, month) ]
                # df_all_eblagh = df_all.loc[df_all['ماه ابلاغ قطعی'] == '%s%s' % (year, month) ]
                agg_ghatee_sodor = df_all.groupby(
                    ['کد اداره', 'شهرستان', 'ماه صدور قطعی',
                     'تاریخ بروزرسانی']).size()
                agg_ghatee_eblagh = df_all.groupby(
                    ['کد اداره', 'شهرستان', 'ماه ابلاغ قطعی',
                     'تاریخ بروزرسانی']).size()
                agg_ghatee_sodor = agg_ghatee_sodor.reset_index(level=0)
                agg_ghatee_eblagh = agg_ghatee_eblagh.reset_index(level=0)
                agg_ghatee_sodor['تاریخ بروزرسانی'] = get_update_date()
                agg_ghatee_eblagh['تاریخ بروزرسانی'] = get_update_date()
                agg_ghatee_sodor = agg_ghatee_sodor.reset_index(level=0)
                agg_ghatee_eblagh = agg_ghatee_eblagh.reset_index(level=0)
                agg_ghatee_sodor = agg_ghatee_sodor.reset_index(level=0)
                agg_ghatee_eblagh = agg_ghatee_eblagh.reset_index(level=0)
                agg_ghatee_eblagh.rename(
                    columns={0: 'تعداد قطعی ابلاغی مشاغل سنتی'}, inplace=True)
                agg_ghatee_sodor.rename(columns={0: 'تعداد قطعی مشاغل سنتی'},
                                        inplace=True)

                # if save_on_folder:

                #     file_name_details_g_sadere = 'ghatee_saderShode_details.xlsx'
                #     file_name_details = 'ghatee_sadervaeblaghi_details.xlsx'
                #     file_name_agg_g_sadere = 'ghatee_saderShode_agg.xlsx'
                #     file_name_details_g_eblaghi = 'ghatee_eblaghshode_details.xlsx'
                #     file_name_agg_g_eblaghi = 'ghatee_eblaghshode_agg.xlsx'
                #     output_dir = os.path.join(saved_folder, file_name_details)
                #     df_all.to_excel(output_dir)
                #     output_dir = os.path.join(saved_folder, file_name_agg_g_sadere)
                #     agg_ghatee_sodor.to_excel(output_dir)
                #     output_dir = os.path.join(
                #         saved_folder, file_name_agg_g_eblaghi)
                #     agg_ghatee_eblagh.to_excel(output_dir)

                if save_how == 'db':
                    drop_into_db(table_name='tblGhateeSodorAndEblagh',
                                 columns=df_all.columns.tolist(),
                                 values=df_all.values.tolist(),
                                 append_to_prev=False,
                                 db_name='testdbV2')

                agg_ghatee_sodor = agg_ghatee_sodor.rename(
                    columns={'کد اداره': 'نام اداره سنتی'})

                agg_ghatee_eblagh = agg_ghatee_eblagh.rename(
                    columns={'کد اداره': 'نام اداره سنتی'})
                if return_df:
                    return df_all, agg_ghatee_sodor, agg_ghatee_eblagh
            else:
                if date is not None:
                    df_all = df_all.loc[df_all['ماه صدور قطعی'] < date]

                agg_ghatee_sodor = df_all.groupby(
                    ['کد اداره', 'شهرستان', 'تاریخ بروزرسانی']).size()
                agg_ghatee_sodor = agg_ghatee_sodor.reset_index()
                agg_ghatee_sodor['تاریخ بروزرسانی'] = get_update_date()
                agg_ghatee_sodor.rename(
                    columns={0: 'تعداد قطعی ابلاغ نشده مشاغل سنتی'}, inplace=True)

                if save_how == 'db':
                    drop_into_db(table_name='tblGhateeSodorNoEblagh',
                                 columns=df_all.columns.tolist(),
                                 values=df_all.values.tolist(),
                                 append_to_prev=False,
                                 db_name='testdbV2')

                agg_ghatee_sodor = agg_ghatee_sodor.rename(
                    columns={'کد اداره': 'نام اداره سنتی'})

                if return_df:
                    return df_all, agg_ghatee_sodor

    return df_all, ''


def schedule_tasks(time_to_run='07:49', run_all=RUN_ALL):

    schedule.every().day.at(time_to_run).do(
        get_tashkhis_ghatee_sonati, eblagh=False, return_df=False)

    while True:

        if run_all:
            schedule.run_all(delay_seconds=10)
            break
        else:
            schedule.run_pending()
            time.sleep(1)


if __name__ == '__main__':
    print('Starting the schedule...')
    schedule_tasks(time_to_run=TIME_TO_RUN)
    # tashkhis_saderNashode, agg_tashkhis_saderNashode, ghatee_saderNashode, agg_ghatee_saderNashode, amade_ersal_beheiat, agg_amade_ersal_beheiat, tashkhis_eblagh_noGhatee, agg_tashkhis_eblagh_noGhatee, agg_amade_ghatee = get_tashkhis_ghatee_sonati(
    # eblagh=True, return_df=True)

    # tashkhis_saderNashode, agg_tashkhis_saderNashode, ghatee_saderNashode, agg_ghatee_saderNashode, amade_ersal_beheiat, agg_amade_ersal_beheiat, tashkhis_eblagh_noGhatee, agg_tashkhis_eblagh_noGhatee, agg_amade_ghatee = get_tashkhis_ghatee_sonati(
    #     eblagh=True, return_df=False)
