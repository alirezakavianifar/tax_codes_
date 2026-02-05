import logging
import os
import time
import math
import cx_Oracle
import pandas as pd
import numpy as np
import random
from contextlib import contextmanager
import traceback  # Import traceback module for error line logging
from automation.helpers import connect_to_sql, get_update_date, time_it, georgian_to_persian, leading_zero, \
    is_date, is_int, drop_into_db, check_if_col_exists, log_the_func, calculate_days_difference
from automation.helpersV2 import rename_duplicates
from automation.sql_queries import sql_delete, create_sql_table, insert_into, get_sql_allsanim, get_sql_allejra, \
    get_sql_sanimusers, get_sql_allhesabrasi, get_sql_alleterazat, \
    get_sql_allbakhshodegi, get_sql_allpayment, get_sql_allcodes, get_sql_allareasusp, get_sql_alltshsmtm, \
    get_sql_allhesabdari, get_sql_allanbare, get_sql_sanim_count, get_tax_types
import schedule
import threading
import argparse
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, wait
from automation.constants import get_sql_con, sanim_conditions, next_sanim_conditions
from automation.vportal_reports.v_portal_reports import rpt_tashkhis_ghatee_238, get_important_modis

MULTI_THREADING = False
# Number of rows fetched every time connecting to oracle
NUM_ROWS = 20_000
TIME_TO_RUN = '08:24'

base_path = r'E:\automating_reports_V2\mapping'

excel_files = {
    'allsanim': 'allsanim.xlsx',
    'alltshsmtm': 'alltshsmtm.xlsx',
    'allareasusp': 'allareasusp.xlsx',
    'allusers': 'allusers.xlsx',
    'allhesabrasi': 'allhesabrasi.xlsx',
    'alleterazat': 'alleterazat.xlsx',
    # 'allbakhshodegi': 'allbakhshodegi.xlsx',
    'allhesabdari': 'allhesabdari.xlsx',
    'allejra': 'allejra.xlsx',
    'allanbare': 'allanbare.xlsx',
    # 'allpayment': 'allpayment.xlsx',
    'allcodes': 'allcodes.xlsx',
}

tbl_names = {
    'allsanim': 'V_PORTAL',
    'alltshsmtm': 'V_TSHSMTM',
    'allareasusp': 'V_AREASUSP',
    'allusers': 'V_USERS',
    'allcodes': 's200_codes_22',
    'allhesabrasi': 'V_AUD35',
    'alleterazat': 'V_OBJ60',
    # 'allbakhshodegi': 'V_IMPUNITY',
    'allhesabdari': 'V_EHMOADI',
    'allejra': 'V_CASEEJRA',
    'allanbare': 'V_AUDPOOL',
    # 'allpayment': 'V_PAYMENT',
}

cx_Oracle.init_oracle_client(lib_dir=r"E:\downloads\instantclient_21_7")

# Create a connection pool with increased size and retry logic
pool = cx_Oracle.SessionPool(user="ostan_khozestan", password="S_KfvDKu_9851z@hFsTf",
                             dsn="10.1.1.200:1521/EXTDB", min=1, max=4, increment=1, threaded=True)


# @contextmanager
def acquire_connection_with_retry(retries=3000000, delay=10):
    for attempt in range(retries):
        delay = random.randint(5, 20)
        try:
            return pool.acquire()
        except cx_Oracle.DatabaseError as e:
            if attempt < retries - 1:
                logging.warning(
                    f"Connection attempt {attempt + 1} failed. Retrying in {delay} seconds...")
                time.sleep(delay)
            else:
                logging.error("All connection attempts failed.")
                raise e


def is_modi_important(x, type='IMHR'):
    if x == type:
        return "بلی"
    return "خیر"


def get_ezhar_type(x):
    try:
        if len(x['کد رهگیری اظهارنامه سنیم - آخرین ورژن اعمال شد']) <= 1:
            return 'نامشخص'
        elif x['کد رهگیری اظهارنامه سنیم - آخرین ورژن اعمال شد'][-5:] == 'TERHR':
            return 'بر اساس تراکنش بانکی'
        elif str(x['شماره اظهارنامه']) == x['کد رهگیری اظهارنامه سنیم - آخرین ورژن اعمال شد'].split('/')[0]:
            return 'اظهارنامه تولیدشده توسط اداره'
        elif x['کد رهگیری اظهارنامه سنیم - آخرین ورژن اعمال شد'][-4:] == 'IMHR':
            return "مودیان مهم با ریسک بالا"
        elif x['کد رهگیری اظهارنامه سنیم - آخرین ورژن اعمال شد'][-4:] == 'BDLR':
            return 'اظهارنامه های ارزش افزوده مشمول ضوابط اجرایی موضوع بند (ب)تبصره (6) قانون بودجه سال 1400'
        elif x['کد رهگیری اظهارنامه سنیم - آخرین ورژن اعمال شد'][-4:] == 'T100':
            return 'تبصره 100'
        elif x['کد رهگیری اظهارنامه سنیم - آخرین ورژن اعمال شد'][-4:] == 'ERHR':
            return 'رتبه ریسک بالا'
        elif x['کد رهگیری اظهارنامه سنیم - آخرین ورژن اعمال شد'][-4:] == 'MRHR':
            return 'ریسک متوسط'
        elif x['کد رهگیری اظهارنامه سنیم - آخرین ورژن اعمال شد'][-4:] == 'DMHR':
            return 'اظهارنامه برآوردی صفر'
        elif x['کد رهگیری اظهارنامه سنیم - آخرین ورژن اعمال شد'][-3:] == 'ZHR':
            return 'اظهارنامه صفر دارای اطلاعات سیستمی'
        elif x['کد رهگیری اظهارنامه سنیم - آخرین ورژن اعمال شد'][-2:] == 'LR':
            return 'رتبه ریسک پایین'
        elif x['کد رهگیری اظهارنامه سنیم - آخرین ورژن اعمال شد'][-2:] == 'ZR':
            return 'اظهارنامه صفر فاقد اطلاعات سیستمی/جهت بررسی توسط اداره'
        elif x['کد رهگیری اظهارنامه سنیم - آخرین ورژن اعمال شد'][-2:] == 'DM':
            return 'اظهارنامه برآوردی غیر صفر'
        elif x['کد رهگیری اظهارنامه سنیم - آخرین ورژن اعمال شد'][-2:] == 'HR':
            return 'انتخاب شده بدون اعمال معیار ریسک'
        else:
            return 'نامشخص'
    except:
        return 'نامشخص'


def detect_last_condition(x):
    for key, message in sanim_conditions.items():
        if len(str(x[key])) > 5:
            return message
    return 'فاقد حکم رسیدگی'


def detect_next_condition(x):

    for key, message in sanim_conditions.items():
        if (key == 'نوع ابلاغ برگ تشخیص' and x[key] == 'قانونی' and len(x['تاريخ ايجاد برگ قطعي']) < 5 and len(x['تاریخ اعتراض هیات بدوی']) < 5 and len(x['تاریخ اعتراض هیات تجدید نظر']) < 5 and len(x['تاریخ اعتراض']) < 5):
            return 'آماده ارسال به هیات - ابلاغ قانونی'
        if (len(x['تاريخ ايجاد برگ قطعي']) < 5 and len(x['تاریخ اعتراض هیات بدوی']) < 5 and len(x['تاریخ اعتراض هیات تجدید نظر']) < 5 and len(x['تاریخ اعتراض']) > 5 and x['توافق'] == 'N'):
            return 'آماده ارسال به هیات'
        if (len(x['تاريخ ايجاد برگ قطعي']) < 5 and len(x['تاریخ اعتراض هیات بدوی']) < 5 and len(x['تاریخ اعتراض هیات تجدید نظر']) < 5 and len(x['تاریخ اعتراض']) > 5 and calculate_days_difference(x['تاریخ اعتراض'], get_update_date(delimiter='/')) > 45):
            return 'آماده ارسال به هیات'

    for key, message in next_sanim_conditions.items():
        if str(x['آخرین وضعیت']) == key:
            return message
    return 'در انتظار صدور حکم رسیدگی'


def input_info():
    parser = argparse.ArgumentParser()
    parser.add_argument('--tasks', type=str,
                        help='Tasks to be run', default='all')
    parser.add_argument('--RUNALL', type=str,
                        help='Tasks to be run', default='false')
    args = parser.parse_args()
    selected_tasks = args.tasks.split(',')
    RUNALL = args.RUNALL
    return selected_tasks, RUNALL


tasks = ['all']
RUNALL = 'true'

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - %(levelname)s - %(message)s')


def get_allSanim(excel_file=None, selected_sql_query=None, tbl_name=None, del_tbl=True, only_schema=False, df_tax_types=None):
    try:
        with acquire_connection_with_retry() as connection:
            with connection.cursor() as cursor:
                cursor.execute(selected_sql_query)
                if excel_file is None:
                    rows = cursor.fetchall()
                    return rows[0][0] if rows else None
                else:
                    path = os.path.join(base_path, excel_file)
                    df_mapping = pd.read_excel(path).astype('str')
                    df_mapping = dict(rename_duplicates(
                        df_mapping, df_mapping.columns.tolist()).to_numpy())
                    columns = pd.DataFrame(
                        [c[0] for c in cursor.description], columns=['engcol'])
                    columns.replace({'engcol': df_mapping}, inplace=True)
                    columns = list(columns.to_numpy().flatten())
                    if del_tbl:
                        if tbl_name == 'V_PORTAL':
                            columns.extend(
                                ['نوع اظهارنامه', 'پرونده مهم', 'آخرین وضعیت', 'وضعیت بعدی'])
                        columns.append('آخرین بروزرسانی')
                        sql_delete_query = sql_delete(tbl_name)
                        connect_to_sql(
                            sql_query=sql_delete_query, connect_type=f'dropping sql table {tbl_name}')
                        sql_create_table_query = create_sql_table(
                            tbl_name, columns)
                        connect_to_sql(sql_query=sql_create_table_query,
                                       connect_type=f'creating sql table {tbl_name}')
                    if only_schema:
                        return
                    num_rows = 50_000  # Adjust batch size as needed
                    while True:
                        rows = cursor.fetchmany(num_rows)
                        if not rows:
                            break
                        rows = [
                            row[:-1] if 'RNUM' in columns else row for row in rows]
                        df = pd.DataFrame(rows, columns=columns[:-1])
                        df = preprocess_data(df, tbl_name, df_tax_types)
                        sql_insert = insert_into(tbl_name, df.columns.tolist())
                        connect_to_sql(sql_query=sql_insert, df_values=df.values.tolist(
                        ), connect_type='inserting into sql table')
    except cx_Oracle.DatabaseError as e:
        logging.error(f"Database connection or query execution failed: {e}")
        logging.error(traceback.format_exc())
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        logging.error(traceback.format_exc())


def preprocess_data(df, tbl_name, df_tax_types):
    try:
        search_terms = ['تاریخ', 'مهلت']
        date_cols = []
        df = df.astype(str)
        df_cols = df.columns.tolist()
        for item in df_cols:
            for search_term in search_terms:
                if search_term in item and item not in date_cols:
                    date_cols.append(item)
        df.replace({'منبع مالیاتی': df_tax_types}, inplace=True)
        df.fillna(0, inplace=True)
        df.replace(['nan', 'None', 'NaN', 'NaT'], '0', inplace=True)
        numeric_columns = df.apply(lambda x: pd.to_numeric(
            x, errors='coerce').notna().all())
        numeric_columns = df.columns[numeric_columns].tolist()
        df[numeric_columns] = df[numeric_columns].apply(
            np.float64).apply(np.int64)
        if (tbl_name == 'V_PORTAL'):
            df['نوع اظهارنامه'] = df.apply(lambda x: get_ezhar_type(x), axis=1)
            df['سال عملکرد'] = df['سال عملکرد'].apply(
                np.float64).replace(np.nan, 0).apply(np.int64)
            df['شماره اقتصادی'] = df['شماره اقتصادی'].apply(
                lambda x: leading_zero(x, sanim_based=True))
            df.replace(np.nan, '', inplace=True)
            df.fillna(0, inplace=True)
            # int_cols = ['شماره اظهارنامه', 'دوره عملکرد', 'شناسه حسابرسی', 'شماره برگ قطعی', 'شماره برگ اجرا',
            #             'عوارض قطعی', 'پرداختی از اصل مالیات', 'میزان مبلغ بخشودگی', 'مالیات تشخیص', 'درآمد تشخیص',
            #             'شماره برگ تشخیص', 'فروش تشخیص', 'فروش قطعی', 'مالیات قطعی', 'درآمد قطعی', 'مالیات ابرازی',
            #             'درآمد ابرازی', 'عوارض ابرازی', 'اعتبار ابرازی', 'عوارض تشخیص', 'فروش ابرازی',
            #             'سود و زیان ابرازی', 'مانده بدهی از اصل مالیات']

            int_cols = ['شماره اظهارنامه', 'دوره عملکرد', 'شناسه حسابرسی', 'شماره برگ قطعی', 'شماره برگ اجرا',
                        'عوارض قطعی', 'میزان مبلغ بخشودگی', 'مالیات تشخیص', 'درآمد تشخیص',
                        'شماره برگ تشخیص', 'فروش تشخیص', 'فروش قطعی', 'مالیات قطعی', 'درآمد قطعی', 'مالیات ابرازی',
                        'درآمد ابرازی', 'عوارض ابرازی', 'اعتبار ابرازی', 'عوارض تشخیص', 'فروش ابرازی',
                        'سود و زیان ابرازی',]

            for col in int_cols:
                df[col] = df[col].apply(np.float64).apply(np.int64)
            df['کد رهگیری اظهارنامه سنیم - آخرین ورژن اعمال شد'] = df['کد رهگیری اظهارنامه سنیم - آخرین ورژن اعمال شد'].astype(
                str)
            df['پرونده مهم'] = df['کد رهگیری اظهارنامه سنیم - آخرین ورژن اعمال شد'].apply(
                lambda x: is_modi_important(str(x[-4:]), type='IMHR'))
            df['آخرین وضعیت'] = df.apply(detect_last_condition, axis=1)
            df['وضعیت بعدی'] = df.apply(detect_next_condition, axis=1)
        df.replace('0', '', inplace=True)
        df.replace(0, '', inplace=True)
        df = df.astype('str')
        df['آخرین بروزرسانی'] = get_update_date()
        for item in date_cols:
            df[item] = df[item].astype(str).apply(lambda x: is_date(x))
        return df
    except Exception as e:
        logging.error(f"Error in preprocess_data: {e}")
        logging.error(traceback.format_exc())
        raise


@log_the_func('none')
def count_tbls(tbl=tbl_names['allhesabdari'], num=4_000_000):
    if tbl != 's200_codes_22':
        ostan_txt = '_khozestan'
    else:
        ostan_txt = ''
    count = get_allSanim(None, get_sql_sanim_count(
        tbl, ostan_txt=ostan_txt), None)
    num_threads = math.ceil(count/num)
    num_threads = 1
    buckets = []
    tmp = 0
    c_num = num
    while count > num:
        buckets.append((tmp, num))
        tmp = num + 1
        num += c_num
    if num > count:
        buckets.append((tmp, count))
    return buckets, num_threads, count


def backup_func():
    for key, value in tbl_names.items():
        sql_query = 'SELECT * INTO %s FROM [TestDb].[dbo].[%s]' % (
            value, value)
        drop_into_db(value, append_to_prev=False, db_name='testdbV2',
                     create_tbl='no', sql_query=sql_query)
        sql_query = 'ALTER TABLE %s ADD PRIMARY KEY (ID)' % value
        connect_to_sql(sql_query, get_sql_con(database='testdbV2'))


def run_thread(job_func, *args, **kwargs):
    job_thread = threading.Thread(target=job_func, args=args)
    job_thread.start()


params = {
    'allsanim': [excel_files['allsanim'], get_sql_allsanim(), tbl_names['allsanim']],
    'alltshsmtm': [excel_files['alltshsmtm'], get_sql_alltshsmtm(), tbl_names['alltshsmtm']],
    'allusers': [excel_files['allusers'], get_sql_sanimusers(), tbl_names['allusers']],
    'allareasusp': [excel_files['allareasusp'], get_sql_allareasusp(), tbl_names['allareasusp']],
    # 'allbakhshodegi': [excel_files['allbakhshodegi'], get_sql_allbakhshodegi(), tbl_names['allbakhshodegi']],
    'alleterazat': [excel_files['alleterazat'], get_sql_alleterazat(), tbl_names['alleterazat']],
    'allhesabdari': [excel_files['allhesabdari'], get_sql_allhesabdari(), tbl_names['allhesabdari']],
    'allanbare': [excel_files['allanbare'], get_sql_allanbare(), tbl_names['allanbare']],
    'allejra': [excel_files['allejra'], get_sql_allejra(), tbl_names['allejra']],
    'allhesabrasi': [excel_files['allhesabrasi'], get_sql_allhesabrasi(), tbl_names['allhesabrasi']],
    # 'allpayment': [excel_files['allpayment'], get_sql_allpayment(), tbl_names['allpayment']],
    'allcodes': [excel_files['allcodes'], get_sql_allcodes(), tbl_names['allcodes']],
}


def run_it(buckets=None, num_threads=None, field=None):
    df_tax_types = get_tax_types()
    for key, value in tbl_names.items():
        if key != 'allcodes':
            ostan_txt = '_khozestan'
        else:
            ostan_txt = ''
        get_allSanim(excel_files[key], get_sql_sanimusers(
            tblname=value, ostan_txt=ostan_txt), value, only_schema=True)
        num_threads = 0
        while (num_threads <= 0):
            buckets, num_threads, count = count_tbls(tbl=value)
        executor = ThreadPoolExecutor(num_threads)
        # executor = ProcessPoolExecutor(num_threads)
        jobs = [executor.submit(get_allSanim, excel_files[key], get_sql_sanimusers(
            item[0], item[1], value, ostan_txt), value, False, False, df_tax_types) for item in buckets]
        wait(jobs)
    backup_func()


def schedule_tasks(time_to_run='15:30'):
    if 'all' in tasks:
        schedule.every().day.at(time_to_run).do(run_it)
    if '1' in tasks:
        schedule.every().day.at(time_to_run).do(run_thread, get_allSanim,
                                                params['allusers'][0], params['allusers'][1], params['allusers'][2])
    if '2' in tasks:
        schedule.every().day.at(time_to_run).do(run_thread, get_allSanim,
                                                params['allsanim'][0], params['allsanim'][1], params['allsanim'][2])
    if '3' in tasks:
        schedule.every().day.at(time_to_run).do(run_thread, get_allSanim,
                                                params['allbakhshodegi'][0], params['allbakhshodegi'][1], params['allbakhshodegi'][2])
    if '4' in tasks:
        schedule.every().day.at(time_to_run).do(run_thread, get_allSanim,
                                                params['alleterazat'][0], params['alleterazat'][1], params['alleterazat'][2])
    if '5' in tasks:
        schedule.every().day.at(time_to_run).do(run_thread, get_allSanim,
                                                params['allhesabdari'][0], params['allhesabdari'][1], params['allhesabdari'][2])
    if '6' in tasks:
        schedule.every().day.at(time_to_run).do(run_thread, get_allSanim,
                                                params['allanbare'][0], params['allanbare'][1], params['allanbare'][2])
    if '7' in tasks:
        schedule.every().day.at(time_to_run).do(run_thread, get_allSanim,
                                                params['allhesabrasi'][0], params['allhesabrasi'][1], params['allhesabrasi'][2])
    if '8' in tasks:
        schedule.every().day.at(time_to_run).do(run_thread, get_allSanim,
                                                params['allejra'][0], params['allejra'][1], params['allejra'][2])
    if '9' in tasks:
        schedule.every().day.at(time_to_run).do(run_thread, get_allSanim,
                                                params['allpayment'][0], params['allpayment'][1], params['allpayment'][2])
    if '10' in tasks:
        schedule.every().day.at(time_to_run).do(run_thread, get_allSanim,
                                                params['allcodes'][0], params['allcodes'][1], params['allcodes'][2])
    if '11' in tasks:
        schedule.every().day.at(time_to_run).do(run_thread, get_allareasusp,
                                                params['allareasusp'][0], params['allareasusp'][1], params['allareasusp'][2])
    if '12' in tasks:
        schedule.every().day.at(time_to_run).do(run_thread, get_alltshsmtm,
                                                params['alltshsmtm'][0], params['alltshsmtm'][1], params['alltshsmtm'][2])
    while True:
        if RUNALL == 'true':
            schedule.run_all(delay_seconds=10)
            break
        else:
            schedule.run_pending()
            time.sleep(1)


def run_without_scheduling():
    get_allSanim(excel_files['allsanim'],
                 get_sql_allsanim(), tbl_names['allsanim'])
    get_allSanim(excel_files['allejra'],
                 get_sql_allejra(), tbl_names['allejra'])
    get_allSanim(excel_files['allusers'],
                 get_sql_sanimusers(), tbl_names['allusers'])
    get_allSanim(excel_files['allareasusp'],
                 get_sql_allareasusp(), tbl_names['allareasusp'])
    get_allSanim(excel_files['alltshsmtm'],
                 get_sql_alltshsmtm(), tbl_names['alltshsmtm'])
    get_allSanim(excel_files['alleterazat'],
                 get_sql_alleterazat(), tbl_names['alleterazat'])
    # get_allSanim(excel_files['allbakhshodegi'],
    #              get_sql_allbakhshodegi(), tbl_names['allbakhshodegi'])
    get_allSanim(excel_files['allhesabdari'],
                 get_sql_allhesabdari(), tbl_names['allhesabdari'])
    get_allSanim(excel_files['allanbare'],
                 get_sql_allanbare(), tbl_names['allanbare'])
    get_allSanim(excel_files['allhesabrasi'],
                 get_sql_allhesabrasi(), tbl_names['allhesabrasi'])
    # get_allSanim(excel_files['allpayment'],
    #              get_sql_allpayment(), tbl_names['allpayment'])
    get_allSanim(excel_files['allcodes'],
                 get_sql_allcodes(), tbl_names['allcodes'])


if __name__ == '__main__':
    print('Starting the schedule...')
    schedule_tasks(time_to_run=TIME_TO_RUN)
    # run_without_scheduling()
