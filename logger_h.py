from selenium import webdriver
from selenium.webdriver.common.by import By
import time
import glob
import numpy as np
import os.path
import pandas as pd
from datetime import datetime
import argparse
import jdatetime
import xlwings as xw
import shutil
import pandas as pd
import openpyxl
import datetime as dt
from datetime import datetime
from functools import wraps
import math
import pyodbc
from helpers import get_update_date, remove_excel_files


log_folder_name = 'C:\ezhar-temp'
log_excel_name = '2000-haghighi-File2-sajad-final-Remain1.xlsx'
log_dir = os.path.join(log_folder_name, log_excel_name)


def log_it(func):
    
    @wraps(func)
    def try_it(*args, **kwargs):
        
        print('log_it initialized')
        d1 = datetime.now()
        type_of = func.__name__
        
            
        result = func(*args, **kwargs)
        rahgiri = args[0]
        success = args[1]
        
        df_1 = pd.DataFrame([[rahgiri, success]], columns=['rahgiri', 'success'])
                 
        # create excel file for logging if it does not already exist
        if not os.path.exists(log_dir):
            
            df_1.to_excel(log_dir)
            
        else:
            
            df_2 = pd.read_excel(log_dir, index_col=0)
               
            df_3 = pd.concat([df_1,df_2])
               
            remove_excel_files([log_dir])
            
            df_3.to_excel(log_dir)
                
        d2 = datetime.now()
        d3 = (d2 - d1).total_seconds() / 60
        
          
        print('it took %s minutes for the file to be logged' % ("%.2f" % d3 ))
            
       
        return result
                 
    return try_it