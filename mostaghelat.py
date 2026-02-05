import functools as ft
import os
import pandas as pd
import schedule
from helpers import maybe_make_dir, insert_into_tbl, connect_to_sql, final_most, rename_duplicate_columns, \
    drop_into_db, connect_to_sql, is_int, get_update_date, log_the_func, final_most
from scrape import Scrape
from constants import get_sql_con
import time

project_root = os.path.dirname(os.path.dirname(__file__))
# project_root = os.path.join(project_root, 'test')


TIME_TO_RUN = '09:32'
RUN_ALL = True
HEADLESS = False
READ_FROM_REPO = False
SCRAPE = True

path_mostaghelat = os.path.join(project_root, r'saved_dir\mostaghelat')
path_mostaghelat_tashkhis = os.path.join(
    project_root, r'saved_dir\mostaghelat\mostaghelat_tashkhis')
path_mostaghelat_ghatee = os.path.join(
    project_root, r'saved_dir\mostaghelat\mostaghelat_ghatee')
path_mostaghelat_amade_ghatee = os.path.join(
    project_root, r'saved_dir\mostaghelat\mostaghelat_amade_ghatee')


maybe_make_dir([path_mostaghelat])
maybe_make_dir([path_mostaghelat_tashkhis])
maybe_make_dir([path_mostaghelat_ghatee])
maybe_make_dir([path_mostaghelat_amade_ghatee])


@log_the_func('none')
def run_it():
    x = Scrape()

    x.scrape_mostaghelat(path=path_mostaghelat_tashkhis, scrape=SCRAPE,
                         report_type='Tashkhis', table_name='tblTashkhisMost', read_from_repo=READ_FROM_REPO,
                         drop_to_sql=True, append_to_prev=False,
                         del_prev_files=True, headless=HEADLESS)

    x.scrape_mostaghelat(path=path_mostaghelat_ghatee, scrape=SCRAPE,
                         report_type='Ghatee', table_name='tblGhateeMost', read_from_repo=READ_FROM_REPO,
                         drop_to_sql=True, append_to_prev=False,
                         del_prev_files=True, headless=HEADLESS)

    x.scrape_mostaghelat(path=path_mostaghelat_amade_ghatee, scrape=SCRAPE, read_from_repo=READ_FROM_REPO,
                         report_type='AmadeGhatee', table_name='tblAmadeGhateeMost',
                         drop_to_sql=True, append_to_prev=False,
                         del_prev_files=True, headless=HEADLESS)

    tashkhis, ghatee, amade_ghatee, agg_most = final_most()


def schedule_tasks(time_to_run='15:30', run_all=RUN_ALL):

    schedule.every().day.at(time_to_run).do(run_it)

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
