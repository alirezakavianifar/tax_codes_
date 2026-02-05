try:
    import streamlit as st
except ModuleNotFoundError:
    import subprocess
    subprocess.check_call(["python", "-m", "pip", "install", "streamlit"])
    import streamlit as st

import pandas as pd
import numpy as np
import os
import schedule
import time
from functools import partial
from concurrent.futures import ProcessPoolExecutor, as_completed
from automation.helpers import maybe_make_dir, connect_to_sql
from automation.scrape import Scrape
from automation.constants import get_sql_con

# Streamlit UI
st.title("Code Eghtesadi Scraper")

# User input parameters
saving_path = st.text_input("Saving Path", "codeghtesadi")
set_important_corps = st.checkbox("Set Important Corps")
get_data = st.checkbox("Get Data")
get_dadrasi = st.checkbox("Get Dadrasi")
get_amlak = st.checkbox("Get Amlak")
merge = st.checkbox("Merge Data")
adam = st.text_input("Adam", "")
set_hoze = st.checkbox("Set Hoze")
set_user_permissions = st.checkbox("Set User Permissions")
del_prev_files = st.checkbox("Delete Previous Files")
get_info = st.checkbox("Get Info")
get_sabtenamcodeeghtesadidata = st.checkbox("Get Sabtenam Code Eghtesadi Data")
down_url = st.text_input("Download URL", "")
set_enseraf = st.checkbox("Set Enseraf")
set_arzesh = st.checkbox("Set Arzesh")
find_hoghogh_info = st.checkbox("Find Hoghogh Info")
num_splits = st.number_input("Number of Splits", min_value=1, value=1, step=1)

# Fetch data based on selection
df_ = pd.DataFrame()
if find_hoghogh_info:
    df_ = connect_to_sql(
        'SELECT * FROM tblhoghoghUrls WHERE [done] IS NULL', read_from_sql=True, return_df=True)
elif set_arzesh:
    df_ = connect_to_sql("SELECT * FROM [TestDb].[dbo].[tblSetArzesh] WHERE done IS NULL",
                         sql_con=get_sql_con(database='TestDb'), read_from_sql=True, return_df=True)
elif set_enseraf:
    df_ = connect_to_sql(
        'SELECT * FROM tblenseraf_heait WHERE [done] IS NULL', read_from_sql=True, return_df=True)

chunks = np.array_split(df_, num_splits) if not df_.empty else [pd.DataFrame()]

# Set up paths
project_root = os.getcwd()
saving_dir = os.path.join(project_root, 'saved_dir', saving_path)
maybe_make_dir([saving_dir])

# Parameters Dictionary
PARAMS = {
    'set_important_corps': set_important_corps,
    'getdata': get_data,
    'get_dadrasi': get_dadrasi,
    'set_enseraf': set_enseraf,
    'set_arzesh': set_arzesh,
    'find_hoghogh_info': find_hoghogh_info,
    'get_amlak': get_amlak,
    'merge': merge,
    'adam': adam,
    'set_hoze': set_hoze,
    'set_user_permissions': set_user_permissions,
    'del_prev_files': del_prev_files,
    'get_info': get_info,
    'get_sabtenamCodeEghtesadiData': get_sabtenamcodeeghtesadidata,
    'down_url': down_url,
    'saving_dir': saving_dir,
    'df': df_,
}


def code_eghtesadi():
    x = Scrape()
    x.scrape_codeghtesadi(path=saving_dir, return_df=False, data_gathering=False, pred_captcha=False,
                          codeeghtesadi={'state': True, 'params': PARAMS}, headless=False, soratmoamelat={'state': False})


def schedule_tasks():
    schedule.every().day.at("10:10").do(code_eghtesadi)
    while True:
        schedule.run_pending()
        time.sleep(60)


if st.button("Start Scraping"):
    with ProcessPoolExecutor(max_workers=len(chunks)) as executor:
        futures = [executor.submit(code_eghtesadi) for _ in chunks]
        for future in as_completed(futures):
            try:
                future.result()
                st.success("Scraping completed successfully.")
            except Exception as e:
                st.error(f"Error processing chunk: {e}")
