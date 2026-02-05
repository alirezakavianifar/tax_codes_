from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
import time
import glob
import re
from contextlib import contextmanager
# import tensorflow as tf
import numpy as np
import os.path
import pandas as pd
from datetime import datetime
import argparse
import jdatetime
import xlwings as xw
import shutil
import pandas as pd
import zipfile
import datetime as dt
from datetime import datetime
from functools import wraps
import functools as ft
import math
from tqdm import tqdm
import pyodbc
import selenium
from pathlib import Path
from openpyxl import Workbook
from openpyxl.styles import Font, Color, colors, fills
from openpyxl.utils.dataframe import dataframe_to_rows
import keyboard
# from tensorflow import keras
# from tensorflow.keras import layers
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from automation.constants import geck_location, set_gecko_prefs, get_remote_sql_con, get_sql_con, get_str_help, get_comm_reports, \
    get_heiat, get_lst_reports, get_all_years, get_common_years, get_str_years, get_years, get_common_reports, \
    get_comm_years, get_heiat_reports, get_server_namesV2
from automation.sql_queries import get_sql_mashaghelsonati, get_sql_mashaghelsonati_ghatee, get_sql_mashaghelsonati_tashkhisEblaghNoGhatee, \
    get_sql_mashaghelsonati_ghateeEblaghNashode, get_sql_mashaghelsonati_tashkhisEblaghNashode, \
    get_sql_mashaghelsonati_amadeghatee, get_sql_mashaghelsonati_amadeersalbeheiat, \
    sql_delete, create_sql_table, insert_into, get_tblupdateDate
from dateutil.parser import parse
from concurrent.futures import ThreadPoolExecutor, wait
from urllib.request import urlretrieve
from automation.helpers import init_driver, login_sanim


INFO = {}

if __name__ == '__main__':
    with init_driver(pathsave="E:\automating_reports_V2\saved_dir\test\sample") as driver:
        driver, info = login_sanim(driver=driver, info=INFO)

        print('f')
