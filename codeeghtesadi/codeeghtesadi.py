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
from concurrent.futures import ThreadPoolExecutor, wait, ProcessPoolExecutor, as_completed
from functools import partial
import argparse
import click
PROMPT = False
SAVING_PATH = 'codeghtesadi'


@click.command()
@click.option('--saving_path', prompt=PROMPT, default='codeghtesadi')
@click.option('--set_important_corps', prompt=PROMPT, default=False)
@click.option('--getdata', prompt=PROMPT, default=False)
@click.option('--get_dadrasi', prompt=PROMPT, default=False)
@click.option('--get_dadrasi_new', prompt=PROMPT, default=False)
@click.option('--get_amlak', prompt=PROMPT, default=False)
@click.option('--merge', prompt=PROMPT, default=False)
@click.option('--adam', prompt=PROMPT, default='')
@click.option('--set_hoze', prompt=PROMPT, default=False)
@click.option('--set_user_permissions', prompt=PROMPT, default=False)
@click.option('--del_prev_files', prompt=PROMPT, default=False)
@click.option('--get_info', prompt=PROMPT, default=False)
@click.option('--get_sabtenamcodeeghtesadidata', prompt=PROMPT, default=False)
@click.option('--down_url', prompt=PROMPT, default='')
@click.option('--set_enseraf', prompt=PROMPT, default=False)
@click.option('--set_arzesh', prompt=PROMPT, default=False)
@click.option('--find_hoghogh_info', prompt=PROMPT, default=False)
# If Scrape URLs then find_hoghogh_info_from_scratch = False else True
@click.option('--find_hoghogh_info_from_scratch', prompt=PROMPT, default=False)
@click.option('--dar_jarian_dadrasi', prompt=PROMPT, default=False)
@click.option('--ravand_residegi', prompt=PROMPT, default=False)
@click.option('--set_chargoon_info', prompt=PROMPT, default=False)
@click.option('--set_vosol_ejra', prompt=PROMPT, default=False)
@click.option('--num_splits', prompt=PROMPT, default=1)
def get_params(saving_path, set_important_corps, getdata, get_dadrasi, get_dadrasi_new, get_amlak, merge, adam, set_hoze, set_user_permissions, del_prev_files, get_info, get_sabtenamcodeeghtesadidata, down_url, set_enseraf, set_arzesh, find_hoghogh_info, find_hoghogh_info_from_scratch, dar_jarian_dadrasi, ravand_residegi, set_chargoon_info, set_vosol_ejra, num_splits):
    return (saving_path, set_important_corps, getdata, get_dadrasi, get_dadrasi_new, get_amlak, merge, adam, set_hoze, set_user_permissions, del_prev_files, get_info, get_sabtenamcodeeghtesadidata, down_url, set_enseraf, set_arzesh, find_hoghogh_info, find_hoghogh_info_from_scratch, dar_jarian_dadrasi, ravand_residegi, set_chargoon_info, set_vosol_ejra, num_splits)


SAVING_PATH, SET_IMPORTANT_CORPS, GET_DATA, GET_DADRASI, GET_DADRASI_NEW, GET_AMLAK, MERGE, ADAM, SET_HOZE, SET_USER_PERMISSION, DEL_PREV_FILES, GET_INFO, GET_SABTENAMCODEEGHTESADIDATA, DOWN_URL, SET_ENSERAF, SET_ARZESH, FIND_HOGHOGH_INFO, FIND_HOGHOGH_INFO_FROM_SCRATCH, DAR_JARIAN_DADRASI, RAVAND_RESIDEGI, SET_CHARGOON_INFO, SET_VOSOL_EJRA, NUM_SPLITS = get_params.main(
    standalone_mode=False)

# Always initialize df_ as an empty DataFrame
df_ = pd.DataFrame()

ADAM_s = 0

if ADAM:

    df_ = connect_to_sql(
        sql_query="SELECT * FROM tbladam WHERE [last_date_modified] IS NULL", sql_con=get_sql_con(database='testdbV2'),
        read_from_sql=True, return_df=True)

if SET_ARZESH:

    df_ = connect_to_sql(
        sql_query="SELECT * FROM [TestDb].[dbo].[tblSetArzesh] where done IS NULL",
        sql_con=get_sql_con(database='TestDb'),
        read_from_sql=True, return_df=True)

if SET_CHARGOON_INFO:

    df_ = connect_to_sql(
        sql_query="SELECT * FROM [testdbV2].[dbo].[tblchargoon] where done IS NULL",
        sql_con=get_sql_con(database='testdbV2'),
        read_from_sql=True, return_df=True)

if SET_ENSERAF:

    df_ = connect_to_sql('SELECT * FROM tblmoavaghat WHERE [done] IS NULL',
                         read_from_sql=True,
                         return_df=True,)

if SET_VOSOL_EJRA:

    df_ = connect_to_sql('SELECT * FROM tblmoavaghat WHERE [done] IS NULL',
                         read_from_sql=True,
                         return_df=True,)

if FIND_HOGHOGH_INFO:

    df_ = connect_to_sql('SELECT * FROM tblhoghoghUrls WHERE [done] IS NULL',
                         read_from_sql=True,
                         return_df=True,)


# Ensure df_ is a DataFrame before splitting
if isinstance(df_, pd.DataFrame) and not df_.empty:
    chunks = np.array_split(df_, NUM_SPLITS)

    if FIND_HOGHOGH_INFO_FROM_SCRATCH:
        chunks = [1]
else:
    # Handle the case where df_ is not a DataFrame or is empty

    if not FIND_HOGHOGH_INFO_FROM_SCRATCH:
        chunks = np.array_split(df_, NUM_SPLITS)
    else:

        chunks = [1]

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
          'get_dadrasi_new': GET_DADRASI_NEW,
          'set_enseraf': SET_ENSERAF,
          'set_arzesh': SET_ARZESH,
          'find_hoghogh_info': FIND_HOGHOGH_INFO,
          'find_hoghogh_info_from_scratch': FIND_HOGHOGH_INFO_FROM_SCRATCH,
          'dar_jarian_dadrasi': DAR_JARIAN_DADRASI,
          'ravand_residegi': RAVAND_RESIDEGI,
          'get_amlak': GET_AMLAK,
          'merge': MERGE,
          'adam': ADAM,
          'set_hoze': SET_HOZE,
          'set_user_permissions': SET_USER_PERMISSION,
          'del_prev_files': DEL_PREV_FILES,
          'df_toadam': df_,
          'get_sabtenamCodeEghtesadiData': GET_SABTENAMCODEEGHTESADIDATA,
          'down_url': DOWN_URL,
          'df_set_hoze': df_,
          'get_info': GET_INFO,
          'saving_dir': path_codeghtesadi,
          'df': df_,
          'df_set_arzesh': df_,
          'set_chargoon_info': SET_CHARGOON_INFO,
          'set_vosol_ejra': SET_VOSOL_EJRA
          }


def code_eghtesadi(*args, **kwargs):

    for k, v in PARAMS.items():
        if k in kwargs.keys():
            PARAMS[k] = kwargs[k]

    x = Scrape()
    x.scrape_codeghtesadi(path=path_codeghtesadi, return_df=False,
                          data_gathering=False, pred_captcha=False,
                          codeeghtesadi={'state': True,
                                         'params': PARAMS},
                          headless=False,
                          soratmoamelat={'state': False})


def schedule_tasks(*args, **kwargs):

    schedule.every().day.at(kwargs['time_to_run']).do(
        partial(code_eghtesadi, df=kwargs['df'], path=kwargs['path'], index=kwargs['index']))

    while True:

        if kwargs['run_all']:
            schedule.run_all(delay_seconds=10)
            break
        else:
            schedule.run_pending()
            time.sleep(1)


if __name__ == '__main__':

    print('Starting the schedule...')

    with ProcessPoolExecutor(max_workers=len(chunks)) as executor:
        futures = [
            executor.submit(partial(schedule_tasks, time_to_run=TIME_TO_RUN,
                            run_all=True, df=chunk, path=None, index=None))
            for i, chunk in enumerate(chunks)
        ]
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"Error processing chunk: {e}")
