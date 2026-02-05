import functools as ft
import os
import pandas as pd
import numpy as np
from automation.helpers import maybe_make_dir, count_num_files, \
    insert_into_tbl, connect_to_sql, final_most, rename_duplicate_columns, wrap_it_with_params, \
    drop_into_db, connect_to_sql, is_int, get_update_date, log_the_func, merge_multiple_excel_files
from automation.scrape import Scrape
from automation.constants import get_sql_con
import schedule
from concurrent.futures import ThreadPoolExecutor, wait
import argparse
import click

PROMPT = False
SAVING_PATH = 'codeghtesadi'


@click.command()
@click.option('--saving_path', prompt=PROMPT, default='codeghtesadi')
@click.option('--set_important_corps', prompt=PROMPT, default=False)
@click.option('--getdata', prompt=PROMPT, default=False)
@click.option('--get_dadrasi', prompt=PROMPT, default=False)
@click.option('--get_amlak', prompt=PROMPT, default=False)
@click.option('--merge', prompt=PROMPT, default=False)
@click.option('--adam', prompt=PROMPT, default='1_1')
@click.option('--set_hoze', prompt=PROMPT, default=False)
@click.option('--set_user_permissions', prompt=PROMPT, default=False)
@click.option('--del_prev_files', prompt=PROMPT, default=False)
@click.option('--get_info', prompt=PROMPT, default=False)
@click.option('--get_sabtenamcodeeghtesadidata', prompt=PROMPT, default=False)
@click.option('--down_url', prompt=PROMPT, default='')
def get_params(saving_path, set_important_corps, getdata, get_dadrasi, get_amlak, merge, adam, set_hoze, set_user_permissions, del_prev_files, get_info, get_sabtenamcodeeghtesadidata, down_url):
    return (saving_path, set_important_corps, getdata, get_dadrasi, get_amlak, merge, adam, set_hoze, set_user_permissions, del_prev_files, get_info, get_sabtenamcodeeghtesadidata, down_url)


SAVING_PATH, SET_IMPORTANT_CORPS, GET_DATA, GET_DADRASI, GET_AMLAK, MERGE, ADAM, SET_HOZE, SET_USER_PERMISSION, DEL_PREV_FILES, GET_INFO, GET_SABTENAMCODEEGHTESADIDATA, DOWN_URL = get_params.main(
    standalone_mode=False)

# DF_ADAM = pd.read_excel(
#     r'E:\automating_reports_V2\automation\saved_dir\adam.xlsx')

# drop_into_db(table_name='tbladam', columns=DF_ADAM.columns.tolist(),
#              values=DF_ADAM.values.tolist(), db_name='testDbV2')

ADAM_s = bool(ADAM.split('_')[0])
part = int(ADAM.split('_')[1])

DF_ADAM = connect_to_sql(
    sql_query="SELECT * FROM tbladam WHERE [message] IS NULL", sql_con=get_sql_con(database='testdbV2'),
    read_from_sql=True, return_df=True)

DF_ADAM = np.array_split(DF_ADAM, 40)[part]

TIME_TO_RUN = '10:10'
RUN_ALL = True
project_root = os.path.dirname(os.path.dirname(__file__))

path_codeghtesadi = os.path.join(project_root, r'saved_dir\%s' % SAVING_PATH)

maybe_make_dir([path_codeghtesadi])

file_path = r'E:\automating_reports_V2\saved_dir\test\melli_eghtesadi.xlsx'
file_path = r'E:\automating_reports_V2\saved_dir\test\1400-1401\splitted.xlsx'

PARAMS = {'set_important_corps': SET_IMPORTANT_CORPS,
          'getdata': GET_DATA,
          'get_dadrasi': GET_DADRASI,
          'get_amlak': GET_AMLAK,
          'merge': MERGE,
          'adam': ADAM_s,
          'set_hoze': SET_HOZE,
          'set_user_permissions': SET_USER_PERMISSION,
          'del_prev_files': DEL_PREV_FILES,
          'df_toadam': DF_ADAM,
          'get_sabtenamCodeEghtesadiData': GET_SABTENAMCODEEGHTESADIDATA,
          'down_url': DOWN_URL,
          'df_set_hoze': 'df',
          'get_info': GET_INFO,
          'saving_dir': path_codeghtesadi,
          'df': 'df',
          }

# @log_the_func('log_path')


@wrap_it_with_params(15, 30000, True, False, False, False, record_process_details=True)
def code_eghtesadi(df=None, path=None, index=None, *args, **kwargs):

    for k, v in PARAMS.items():
        if k in kwargs.keys():
            PARAMS[k] = kwargs[k]

    if path is not None:
        saving_dir = os.path.join(path, 'backup', index + '.xlsx')
    else:
        saving_dir = r'E:\automating_reports_V2\saved_dir\test\eghtesadi_data'

    x = Scrape()
    x.scrape_codeghtesadi(path=path_codeghtesadi, return_df=False,
                          data_gathering=False, pred_captcha=False,
                          codeeghtesadi={'state': True,
                                         'params': PARAMS},
                          headless=False,
                          soratmoamelat={'state': False})


def schedule_tasks(time_to_run='15:30', run_all=RUN_ALL, run_it_params={'df': None, 'path': None, 'index': None}):

    schedule.every().day.at(time_to_run).do(
        code_eghtesadi, run_it_params['df'], run_it_params['path'], run_it_params['index'])

    while True:

        if run_all:
            schedule.run_all(delay_seconds=10)
            break
        else:
            schedule.run_pending()
            time.sleep(1)


if __name__ == '__main__':

    print('Starting the schedule...')

    schedule_tasks(time_to_run=TIME_TO_RUN, run_it_params={
                   'df': 'df', 'path': None, 'index': None})
