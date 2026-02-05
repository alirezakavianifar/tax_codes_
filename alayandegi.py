import os
import pandas as pd
from functools import reduce
from helpers import merge_multiple_excel_files, list_files

dir = r'E:\automating_reports_V2\saved_dir\واریزی های عوارض و آلایندگی  1402دیوان محاسبات\1403'


# Function to replace 'ي' with 'ی' in all columns
def replace_arabic_with_persian(df):
    return df.applymap(lambda x: x.replace('ي', 'ی') if isinstance(x, str) else x)


dirs = [it.path for it in os.scandir(dir) if it.is_dir()]

# Step 2: Find subdirectories within each subdirectory (nested directories)
final_dirs = dirs
for item in dirs:
    final_dirs.extend([it.path for it in os.scandir(item) if it.is_dir()])

lst_final = []
# Step 3: Iterate through all found directories
for item in final_dirs:
    # Step 4: Find all zip files in the current directory
    files = lst_final.extend(list_files(item, 'xlsx'))

indices = ['Row Labels', 'شهر', 'نوع']
dfs = []
for file in lst_final:
    df = pd.read_excel(file, sheet_name='1')
    # Apply the function
    df = replace_arabic_with_persian(df)
    df['Row Labels'] = df['Row Labels'].replace(
        ['آغا جاری', 'آغاجاری'], 'آغاجاری')
    df.set_index(indices, inplace=True)
    dfs.append(df)

df_merged = reduce(lambda left, right: pd.merge(left, right, left_index=True, right_index=True,
                                                how='outer'), dfs)
df_merged = df_merged.reset_index()


df_merged.to_excel(
    r'E:\automating_reports_V2\saved_dir\واریزی های عوارض و آلایندگی  1402دیوان محاسبات\1403\final.xlsx', index=False)
