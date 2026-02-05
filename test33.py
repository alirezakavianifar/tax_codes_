import glob
import pandas as pd
from tqdm import tqdm
from automation.helpers import drop_into_db, leading_zero
import numpy as np

df = pd.read_excel(r'E:\automating_reports_V2\saved_dir\moavaghat.xlsx')

# Ensure all columns are strings initially
df = df.astype(str)

# Process 'شناسه ملی' column
column_name = 'شناسه ملی'
if column_name in df.columns:
    df[column_name] = pd.to_numeric(
        df[column_name], errors='coerce').astype('Int64')
else:
    df[column_name] = 'NULL'

df['کلاسه ملی'] = df['کلاسه ملی'].apply(
    lambda x: leading_zero(x, sanim_based=False))

df = df.astype(str)


drop_into_db(table_name='tblmoavaghat',
             columns=df.columns.tolist(), values=df.values.tolist())
