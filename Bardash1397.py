import os
import pandas as pd
from functools import reduce
from helpers import list_files

dir = r'E:\automating_reports_V2\saved_dir\bardasht1397'

# Function to replace 'ي' with 'ی' in all columns


def replace_arabic_with_persian(df):
    return df.applymap(lambda x: x.replace('ي', 'ی') if isinstance(x, str) else x)


# Get list of directories and nested directories
dirs = [it.path for it in os.scandir(dir) if it.is_dir()]
final_dirs = dirs[:]
for item in dirs:
    final_dirs.extend([it.path for it in os.scandir(item) if it.is_dir()])

# Find all .xlsx files in all directories
lst_final = []
for item in final_dirs:
    lst_final.extend(list_files(item, 'xlsx'))

# Define the index columns
indices = ['نام', 'شناسه ملی']

# Read and process each .xlsx file
dfs = []
for i, file in enumerate(lst_final):
    df = pd.read_excel(file, sheet_name='1')
    df = replace_arabic_with_persian(df)
    df.set_index(indices, inplace=True)
    # Rename columns to prevent conflicts during merge
    df.columns = [f"{col}_{i}" for col in df.columns]
    dfs.append(df)

# Merge all dataframes
df_merged = reduce(lambda left, right: pd.merge(
    left, right, left_index=True, right_index=True, how='outer'), dfs)
df_merged = df_merged.reset_index()

# Define a function to split the dataframe and write to multiple sheets if necessary


def save_large_excel(df, file_path, sheet_name='Sheet1', max_rows=1048576):
    with pd.ExcelWriter(file_path, engine='xlsxwriter') as writer:
        for start_row in range(0, df.shape[0], max_rows):
            end_row = min(start_row + max_rows, df.shape[0])
            sheet = f"{sheet_name}_{start_row // max_rows + 1}"
            df[start_row:end_row].to_excel(
                writer, sheet_name=sheet, index=False)


# Save the final merged dataframe to an Excel file
output_path = r'E:\automating_reports_V2\saved_dir\bardasht1397\final.xlsx'
save_large_excel(df_merged, output_path)
