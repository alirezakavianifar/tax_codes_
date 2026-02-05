import functools as ft
import os
import pandas as pd
import numpy as np
from automation.helpers import maybe_make_dir, connect_to_sql, get_sql_con
from automation.scrape import Scrape
from concurrent.futures import ProcessPoolExecutor, as_completed
import schedule
import click
import time

PROMPT = False
SAVING_PATH = 'codeghtesadi'


@click.command()
@click.option('--saving_path', prompt=PROMPT, default='codeghtesadi')
@click.option('--set_important_corps', prompt=PROMPT, default=False)
@click.option('--getdata', prompt=PROMPT, default=False)
@click.option('--get_dadrasi', prompt=PROMPT, default=False)
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
@click.option('--mode', prompt=PROMPT, default='parallel')  # Added mode option
def get_params(saving_path, set_important_corps, getdata, get_dadrasi, get_amlak, merge, adam, set_hoze, set_user_permissions, del_prev_files, get_info, get_sabtenamcodeeghtesadidata, down_url, set_enseraf, set_arzesh, mode):
    return (saving_path, set_important_corps, getdata, get_dadrasi, get_amlak, merge, adam, set_hoze, set_user_permissions, del_prev_files, get_info, get_sabtenamcodeeghtesadidata, down_url, set_enseraf, set_arzesh, mode)


SAVING_PATH, SET_IMPORTANT_CORPS, GET_DATA, GET_DADRASI, GET_AMLAK, MERGE, ADAM, SET_HOZE, SET_USER_PERMISSION, DEL_PREV_FILES, GET_INFO, GET_SABTENAMCODEEGHTESADIDATA, DOWN_URL, SET_ENSERAF, SET_ARZESH, MODE = get_params.main(
    standalone_mode=False)

DF_ADAM = ''
ADAM_s = 0
if ADAM != '':
    ADAM_s = bool(ADAM.split('_')[0])
    part = int(ADAM.split('_')[1])

    DF_ADAM = connect_to_sql(
        sql_query="SELECT * FROM tbladam WHERE [message] IS NULL", sql_con=get_sql_con(database='testdbV2'),
        read_from_sql=True, return_df=True)

    DF_ADAM = np.array_split(DF_ADAM, 10)[part]

df_set_arzesh = connect_to_sql(
    sql_query="SELECT * FROM [TestDb].[dbo].[tblSetArzesh] where done IS NULL",
    sql_con=get_sql_con(database='TestDb'),
    read_from_sql=True,
    return_df=True
)

project_root = os.path.dirname(os.path.dirname(__file__))
path_codeghtesadi = os.path.join(project_root, r'saved_dir\%s' % SAVING_PATH)
maybe_make_dir([path_codeghtesadi])

PARAMS = {'set_important_corps': SET_IMPORTANT_CORPS,
          'getdata': GET_DATA,
          'get_dadrasi': GET_DADRASI,
          'set_enseraf': SET_ENSERAF,
          'set_arzesh': SET_ARZESH,
          'get_amlak': GET_AMLAK,
          'merge': MERGE,
          'adam': ADAM_s,
          'set_hoze': SET_HOZE,
          'set_user_permissions': SET_USER_PERMISSION,
          'del_prev_files': DEL_PREV_FILES,
          'df_toadam': DF_ADAM,
          'get_sabtenamCodeEghtesadiData': GET_SABTENAMCODEEGHTESADIDATA,
          'down_url': DOWN_URL,
          'df_set_hoze': df_set_arzesh,
          'get_info': GET_INFO,
          'saving_dir': path_codeghtesadi,
          'df': df_set_arzesh,
          'df_set_arzesh': df_set_arzesh,
          }


def process_chunk(chunk, index, path):
    """
    Process a single chunk of the DataFrame.
    """
    params = {'df_set_arzesh': chunk}
    saving_dir = os.path.join(path, f'backup_chunk_{index}.xlsx')

    x = Scrape()
    x.scrape_codeghtesadi(
        path=path,
        return_df=False,
        data_gathering=False,
        pred_captcha=False,
        codeeghtesadi={'state': True, 'params': params},
        headless=False,
        soratmoamelat={'state': False}
    )
    print(f"Chunk {index} processed and saved at {saving_dir}")


def parallel_processing(df, num_splits, path):
    """
    Split the DataFrame and process each chunk in parallel.
    """
    chunks = np.array_split(df, num_splits)
    with ProcessPoolExecutor() as executor:
        futures = [
            executor.submit(process_chunk, chunk, i, path)
            for i, chunk in enumerate(chunks)
        ]
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"Error processing chunk: {e}")


def serial_processing(df, num_splits, path):
    """
    Split the DataFrame and process each chunk serially.
    """
    chunks = np.array_split(df, num_splits)
    for i, chunk in enumerate(chunks):
        try:
            process_chunk(chunk, i, path)
        except Exception as e:
            print(f"Error processing chunk {i}: {e}")


if __name__ == '__main__':
    num_splits = 8
    print(f"Starting processing in {MODE} mode...")
    if MODE.lower() == 'parallel':
        parallel_processing(df_set_arzesh, num_splits, path_codeghtesadi)
    elif MODE.lower() == 'serial':
        serial_processing(df_set_arzesh, num_splits, path_codeghtesadi)
    else:
        print(f"Invalid mode: {MODE}. Please use 'serial' or 'parallel'.")
