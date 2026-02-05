import functools as ft
import os
import pandas as pd
import numpy as np
from helpers import maybe_make_dir, count_num_files, \
    insert_into_tbl, connect_to_sql, final_most, rename_duplicate_columns, \
    drop_into_db, connect_to_sql, is_int, get_update_date, log_the_func
from scrape import Scrape
from constants import get_sql_con
import schedule
from concurrent.futures import ThreadPoolExecutor, wait

TIME_TO_RUN = '10:10'
RUN_ALL = True
project_root = os.path.dirname(os.path.dirname(__file__))
path = r'E:\automating_reports_V2\saved_dir\hoghogh'


@log_the_func('log_path')
def run_it(df=None, path=path, index=None, *args, **kwargs):

    x = Scrape()
    x.scrape_hoghogh(path=path, return_df=True)


def schedule_tasks(time_to_run='15:30', run_all=RUN_ALL, run_it_params={}):

    schedule.every().day.at(time_to_run).do(run_it)

    while True:

        if run_all:
            schedule.run_all(delay_seconds=10)
            break
        else:
            schedule.run_pending()
            time.sleep(1)


if __name__ == '__main__':

    schedule_tasks(time_to_run=TIME_TO_RUN)
