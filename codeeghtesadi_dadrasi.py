import functools as ft
import os
import pandas as pd
import numpy as np
from helpers import maybe_make_dir, count_num_files, \
    insert_into_tbl, connect_to_sql, final_most, rename_duplicate_columns, \
    drop_into_db, connect_to_sql, is_int, get_update_date, log_the_func, merge_multiple_excel_files
from scrape import Scrape
from constants import get_sql_con
import schedule
from concurrent.futures import ThreadPoolExecutor, wait
import argparse
import click

PROMPT = False


@click.command()
@click.option('--set_important_corps', prompt=PROMPT, default=False)
@click.option('--getdata', prompt=PROMPT, default=False)
@click.option('--get_dadrasi', prompt=PROMPT, default=False)
@click.option('--merge', prompt=PROMPT, default=False)
@click.option('--adam', prompt=PROMPT, default=False)
@click.option('--set_hoze', prompt=PROMPT, default=False)
@click.option('--set_user_permissions', prompt=PROMPT, default=False)
@click.option('--del_prev_files', prompt=PROMPT, default=False)
@click.option('--get_info', prompt=PROMPT, default=False)
def get_params(set_important_corps, getdata, get_dadrasi, merge, adam, set_hoze, set_user_permissions, del_prev_files, get_info):
    return (set_important_corps, getdata, get_dadrasi, merge, adam, set_hoze, set_user_permissions, del_prev_files, get_info)


SET_IMPORTANT_CORPS, GET_DATA, GET_DADRASI, MERGE, ADAM, SET_HOZE, SET_USER_PERMISSION, DEL_PREV_FILES, GET_INFO = get_params.main(
    standalone_mode=False)


TIME_TO_RUN = '10:10'
RUN_ALL = True
project_root = os.path.dirname(os.path.dirname(__file__))

path_codeghtesadi = os.path.join(project_root, r'saved_dir\codeghtesadi')

maybe_make_dir([path_codeghtesadi])

file_path = r'E:\automating_reports_V2\saved_dir\test\melli_eghtesadi.xlsx'
file_path = r'E:\automating_reports_V2\saved_dir\test\1400-1401\splitted.xlsx'


@log_the_func('log_path')
def run_it(df=None, path=None, index=None, *args, **kwargs):
    if path is not None:
        saving_dir = os.path.join(path, 'backup', index + '.xlsx')
    else:
        saving_dir = r'E:\automating_reports_V2\saved_dir\test\eghtesadi_data'

    x = Scrape()
    x.scrape_codeghtesadi(path=path_codeghtesadi, return_df=False,
                          data_gathering=False, pred_captcha=False,
                          codeeghtesadi={'state': True,
                                         'params': {'set_important_corps': SET_IMPORTANT_CORPS,
                                                    'getdata': GET_DATA,
                                                    'get_dadrasi': GET_DADRASI,
                                                    'merge': MERGE,
                                                    'adam': ADAM,
                                                    'set_hoze': SET_HOZE,
                                                    'set_user_permissions': SET_USER_PERMISSION,
                                                    'del_prev_files': DEL_PREV_FILES,
                                                    'df_toadam': 'ss',
                                                    'df_set_hoze': df,
                                                    'get_info': GET_INFO,
                                                    'saving_dir': saving_dir,
                                                    'df': df,
                                                    }},
                          headless=False,
                          soratmoamelat={'state': False})


def schedule_tasks(time_to_run='15:30', run_all=RUN_ALL, run_it_params={'df': None, 'path': None, 'index': None}):

    schedule.every().day.at(time_to_run).do(
        run_it, run_it_params['df'], run_it_params['path'], run_it_params['index'])

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
