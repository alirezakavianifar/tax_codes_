from threading import Thread
from time import sleep
import functools as ft
import threading
import time
import os
import glob
import pandas as pd
import numpy as np
from automation.helpers import maybe_make_dir
from automation.constants import Role
from automation.iris.iris_scrape_helpers import *
import schedule
from concurrent.futures import ThreadPoolExecutor, wait, ProcessPoolExecutor

TIME_TO_RUN = '10:10'
RUN_ALL = True
MULTI_THREADING = False
NUM_THREADS = 3
ROLE = Role.manager.name
MULTI_THREADING_PATH = r'C:\Users\alkav\Documents\file\files'
HEADLESS = False
TIME_OUT = 70

project_root = os.path.dirname(os.path.dirname(__file__))

path_codeghtesadi = os.path.join(project_root, r'saved_dir\codeghtesadi')

maybe_make_dir([path_codeghtesadi])

path = r'C:\Users\alkav\Documents\file\files'
backup_path = r'C:\Users\alkav\Documents\file\files\backup'

data_file_path = r'C:\Users\alkav\Documents\file\files\file0.xlsx'
data_file_path_final = r'C:\Users\alkav\Documents\file\file.xlsx'
users_file_path = r'C:\Users\alkav\Documents\file\users.xlsx'

df_users = pd.read_excel(users_file_path)

if MULTI_THREADING:
    maybe_make_dir([MULTI_THREADING_PATH])
    dfs_users = np.array_split(df_users, NUM_THREADS)

    dfs = []
    dfs_data = []

    # for item in zip(dfs_data, dfs_users):
    #     dfs.append(item)

    # Split data into multiple files
    file_list = glob.glob(path + "/*" + 'xlsx')
    file_list_backup = glob.glob(backup_path + "/*" + 'xlsx')
    if len(file_list) == 0:
        df_data = pd.read_excel(data_file_path)
        df_data = np.array_split(df_data, NUM_THREADS)
        for ind, item in enumerate(df_data):
            item.to_excel(
                r'C:\Users\alkav\Documents\file\files\file%s.xlsx' % ind, index=False)
            dfs_data.append([item, dfs_users[ind]])

        file_list = glob.glob(path + "/*" + 'xlsx')
    else:
        for ind, item in enumerate(file_list):
            dfs_data.append([pd.read_excel(item), dfs_users[ind]])

else:
    df_data = pd.read_excel(data_file_path)
    dfs_data = []
    dfs_data.append([df_data, df_users])


def run_it(df=None, path=path, file_name=None, backup_file=None, time_out=TIME_OUT, *args, **kwargs):

    scrape_iris(path=path, df=df, del_prev_files=False,
                headless=HEADLESS, file_name=file_name, backup_file=backup_file, time_out=TIME_OUT, role=ROLE)


# @wrap_it_with_params(3, 1000000000)
def schedule_tasks(time_to_run='15:30', run_all=RUN_ALL, run_it_params={}):

    schedule.every().day.at(time_to_run).do(
        run_it, run_it_params['df'], run_it_params['path'], run_it_params['file_name'],
        run_it_params['backup_file'], run_it_params['time_out'], run_it_params['role'])

    while True:

        if run_all:
            schedule.run_all(delay_seconds=10)
            break
        else:
            schedule.run_pending()
            time.sleep(1)


if __name__ == '__main__':
    if MULTI_THREADING:
        executor = ProcessPoolExecutor(NUM_THREADS)

        with ProcessPoolExecutor(NUM_THREADS) as executor:
            jobs = [executor.submit(schedule_tasks, TIME_TO_RUN,
                                    True, {'df': item, 'path': path, 'file_name': file_list[index],
                                           'backup_file': file_list_backup[index], 'time_out': TIME_OUT, 'role': ROLE})
                    for index, item in enumerate(dfs_data)]
            wait(jobs)

    else:
        schedule_tasks(time_to_run=TIME_TO_RUN, run_it_params={
            'df': dfs_data[0], 'path': path, 'file_name': data_file_path_final,
            'backup_file': data_file_path_final, 'time_out': TIME_OUT, 'role': ROLE})
