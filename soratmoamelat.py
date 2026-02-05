import functools as ft
import os
import pandas as pd
from sanim import get_tashkhis_ghatee_sanim, get_badvi_sanim
from mashaghelsonati import get_tashkhis_ghatee_sonati
from helpers import maybe_make_dir, insert_into_tbl, connect_to_sql, final_most, rename_duplicate_columns, drop_into_db, connect_to_sql, is_int, get_update_date, log_the_func
from scrape import Scrape
from constants import get_sql_con
from sql_queries import get_sql_residegi99, get_sql_noresidegi_arzeshafoodehSanim
import schedule


TIME_TO_RUN = '02:27'
RUN_ALL = True

project_root = os.path.dirname(os.path.dirname(__file__))
# project_root = os.path.join(project_root, 'test')

path_soratmoamelat = os.path.join(project_root, r'saved_dir\soratmoamelat')
log_path = os.path.join(path_soratmoamelat, 'soratmoamelat.txt')

maybe_make_dir([path_soratmoamelat])


@log_the_func(log_path)
def run_it(*args, **kwargs):
    x = Scrape()
    if 'field' in kwargs:
        x.scrape_codeghtesadi(path=path_soratmoamelat,
                              soratmoamelat={'state': True,
                                             'params': {
                                                 'scrape': {'general': True, 'gomrok': False,
                                                            'sorat': {'scrape': True, 'years': ['1397']},
                                                            'sayer': False},
                                                 'unzip': True,
                                                 'dropdb': True,
                                                 'gen_report': False}
                                             },
                              headless=False,
                              field=kwargs['field'])
    else:
        x.scrape_codeghtesadi(path=path_soratmoamelat,
                              soratmoamelat={'state': True,
                                             'params': {
                                                 'scrape': {'general': True, 'gomrok': False,
                                                            'sorat': {'scrape': True, 'years': ['1397']}, 'sayer': False},
                                                 'unzip': True,
                                                 'dropdb': True,
                                                 'gen_report': True}
                                             },
                              headless=False,
                              driver=None,
                              info={}
                              )


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
