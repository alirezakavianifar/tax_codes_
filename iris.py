import time
import click
import os
import pandas as pd
import numpy as np
from iris_helpers import create_directories, connect_to_sql, drop_into_db
from iris_scraper import IrisScraper
import schedule
from concurrent.futures import ProcessPoolExecutor, wait
from enum import Enum

class Role(Enum):
    manager_phase1 = 1
    manager_phase2 = 3
    employee = 2

INIT = False
DF_USERS = connect_to_sql('SELECT * FROM tbltempheiatusers',
                          read_from_sql=True,
                          return_df=True,)
TABLE_NAME = 'tbltempheiat'
TIME_TO_RUN = '05:00'
PROMPT = True
END_TIME = 20
RUN_ALL = True
MULTI_THREADING = False
NUM_USERS = len(DF_USERS)
ROLE = Role.manager_phase1.name
MULTI_THREADING_PATH = r'C:\Users\alkav\Documents\file\files'
HEADLESS = False
TIME_OUT = 300
SHENASE_DADRASI = 'no'
INFO = {'success': True, 'keep_alive': True}

project_root = os.path.dirname(os.path.dirname(__file__))

PATH_CODEGHTSADI = os.path.join(project_root, r'saved_dir\codeghtesadi')

create_directories([PATH_CODEGHTSADI])

PATH = r'C:\Users\alkav\Documents\file\files'
BACKUP_PATH = r'C:\Users\alkav\Documents\file\files\backup'

DATA_FILE_PATH = r'C:\Users\alkav\Documents\file\files\file0.xlsx'
DATA_FILE_PATH_FINAL = r'C:\Users\alkav\Documents\file\file.xlsx'
USERS_FILE_PATH = r'C:\Users\alkav\Documents\file\users.xlsx'


def run_it(df=None,
           path=PATH,
           time_out=TIME_OUT,
           role=ROLE,
           end_time=END_TIME,
           shenase_dadrasi=SHENASE_DADRASI,
           info=INFO,
           headless=HEADLESS,
           table_name=TABLE_NAME,
           *args, **kwargs):

    x = IrisScraper()
    driver, info = x.scrape_iris(path=PATH,
                                 df=df,
                                 del_prev_files=False,
                                 headless=headless,
                                 time_out=time_out,
                                 role=role,
                                 end_time=end_time,
                                 shenase_dadrasi=shenase_dadrasi,
                                 table_name=table_name,
                                 info=info)


def schedule_tasks(time_to_run='15:30', run_all=RUN_ALL, run_it_params={}):

    schedule.every().day.at(time_to_run).do(
        run_it,
        run_it_params['df'],
        run_it_params['path'],
        run_it_params['time_out'],
        run_it_params['role'],
        run_it_params['end_time'],
        run_it_params['shenase_dadrasi'],
        run_it_params['info'],
        run_it_params['headless'],
        run_it_params['table_name'],
    )

    while True:

        if run_all:
            schedule.run_all(delay_seconds=10)
            break
        else:
            schedule.run_pending()
            time.sleep(1)

@click.command()
@click.option('--num_threads', prompt=PROMPT, default=NUM_USERS)
@click.option('--init', prompt=PROMPT, default=INIT)
@click.option('--time_to_run', prompt=PROMPT, default=TIME_TO_RUN)
@click.option('--time_out', prompt=PROMPT, default=TIME_OUT)
@click.option('--headless', prompt=PROMPT, default=HEADLESS)
@click.option('--role', prompt=PROMPT, default=ROLE)
@click.option('--end_time', prompt=PROMPT, default=END_TIME)
@click.option('--shenase_dadrasi', prompt=PROMPT, default=SHENASE_DADRASI)
def Get_Params(*args, **kwargs):
    return kwargs

def get_dfs_data(num_threads, init, df_users, max_users, table_name):

    dfs = []
    dfs_data = []
    start_index = 0
    end_index = max_users
    if num_threads != max_users:
        if len(str(num_threads)) == 1:
            num_threads = f"{int(num_threads):02}"
        num_threads = str(num_threads)
        start_index = int(num_threads[0])
        end_index = int(num_threads[1:])

    if init:
        df = pd.read_excel(r'C:\Users\alkav\Documents\file\files\file.xlsx')
        df = df.astype('str')
        drop_into_db(table_name=table_name,
                     columns=df.columns.tolist(),
                     values=df.values.tolist(),
                     db_name='TestDb')
    df_data = connect_to_sql(f'SELECT * FROM {table_name}',
                             read_from_sql=True,
                             return_df=True,)
    dfs_users = np.array_split(df_users, max_users)
    df_data = np.array_split(df_data, max_users)
    # df_data = np.array_split(df_data, 1)

    for ind, item in enumerate(df_data):
        dfs_data.append([item, dfs_users[ind]])

    return dfs_data[start_index:end_index]


if __name__ == '__main__':

    args = Get_Params.main(standalone_mode=False)

    dfs_data = get_dfs_data(
        args['num_threads'], args['init'], DF_USERS, len(DF_USERS), table_name=TABLE_NAME)
    executor = ProcessPoolExecutor(len(dfs_data))

    with ProcessPoolExecutor(len(dfs_data)) as executor:
        # Get the underlying multiprocessing pool
        jobs = [executor.submit(schedule_tasks, args['time_to_run'],
                                RUN_ALL, {'df': item,
                                          'path': PATH,
                                          'time_out': args['time_out'],
                                          'role': args['role'],
                                          'end_time': args['end_time'],
                                          'shenase_dadrasi': args['shenase_dadrasi'],
                                          'info': INFO,
                                          'headless': args['headless'],
                                          'table_name': TABLE_NAME,
                                          })
                for index, item in enumerate(dfs_data)]

        wait(jobs)
