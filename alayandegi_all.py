import glob
import os
import pandas as pd
from functools import reduce


def replace_arabic_with_persian(df):
    return df.applymap(lambda x: x.replace('ي', 'ی') if isinstance(x, str) else x)


def find_excel_files(directory):
    # Define patterns for Excel file extensions
    excel_file_patterns = ["**/*.xlsx", "**/*.xls"]

    # Collect all matching Excel files
    excel_files = []
    for pattern in excel_file_patterns:
        excel_files.extend(glob.glob(os.path.join(
            directory, pattern), recursive=True))

    return excel_files


# Replace with the path to your folder
directory_path = r"E:\automating_reports_V2\saved_dir\1403"
excel_files = find_excel_files(directory_path)

df_pivots = []

# Output list of Excel files
for file in excel_files:
    print(file)

    df = pd.read_excel(file, skiprows=0)
    df = replace_arabic_with_persian(df)
    name = file.split('\\')[-1].split('.')[0]

    # Create a pivot table
    pivot_table = pd.pivot_table(
        df,
        values='سهم شهر',
        index=['شهرستان', 'شهر', 'نوع'],
        aggfunc='sum'
    )

    # Rename the 'سهم شهر' column with the file name
    pivot_table.columns = [name]
    df_pivots.append(pivot_table)

df_merged = reduce(lambda left, right: pd.merge(left, right, left_index=True, right_index=True,
                                                how='outer'), df_pivots).reset_index()

df_merged.to_excel(os.path.join(directory_path, 'file.xlsx'), index=False)
