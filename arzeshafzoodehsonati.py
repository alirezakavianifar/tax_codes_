import functools as ft
import os
import pandas as pd
import schedule
from sanim import get_tashkhis_ghatee_sanim, get_badvi_sanim
from mashaghelsonati import get_tashkhis_ghatee_sonati
from helpers import maybe_make_dir, insert_into_tbl, connect_to_sql, final_most, rename_duplicate_columns, \
    drop_into_db, connect_to_sql, is_int, get_update_date, log_the_func
from scrape import Scrape
from constants import get_sql_con
from sql_queries import get_sql_residegi99, get_sql_noresidegi_arzeshafoodehSanim
import time

project_root = os.path.dirname(os.path.dirname(__file__))
# project_root = os.path.join(project_root, 'test')

TIME_TO_RUN = '13:59'
RUN_ALL = True

path_arzeshafzoodeh = os.path.join(
    project_root, r'saved_dir\arzeshafzoodeh_sonati')

maybe_make_dir([path_arzeshafzoodeh])


@log_the_func('none')
def run_it(*args, **kwargs):
    x = Scrape()
    x.scrape_arzeshafzoodeh(path=path_arzeshafzoodeh,
                            del_prev_files=True,
                            scrape=False,
                            headless=False,
                            merge_df=True,
                            inser_to_db=True,
                            insert_sabtenamArzesh=False,
                            insert_codeEghtesad=False,
                            insert_gash=False,
                            field=kwargs['field'])


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
