from time import sleep
from concurrent.futures import ThreadPoolExecutor
import os
import pandas as pd
import numpy as np
from concurrent.futures import ThreadPoolExecutor, wait
from codeeghtesadi import run_it as get_data


# path = r'E:\automating_reports_V2\saved_dir\test\eghtesadi_data'

file_path = r'E:\automating_reports_V2\saved_dir\test\1400-1401.xlsx'
df = pd.read_excel(file_path)
df.sort_values(by=['شناسه ملی'], inplace=True)
path = r'E:\automating_reports_V2\saved_dir\test\1400-1401'

dfs = np.array_split(df, 5)

if __name__ == '__main__':

    result = []
#     get_data(dfs[0], path, str(1))
    executor = ThreadPoolExecutor(len(dfs))
    jobs = [executor.submit(get_data, item, path, str(index))
            for index, item in enumerate(dfs)]
    print('done')
    wait(jobs)
