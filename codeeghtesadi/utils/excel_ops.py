import os
import glob
import time
import math
import numpy as np
import pandas as pd
import xlwings as xw
from tqdm import tqdm
from openpyxl import Workbook
from openpyxl.styles import Font, Color, colors, fills
from openpyxl import styles
from openpyxl.utils.dataframe import dataframe_to_rows
from openpyxl.utils import get_column_letter
import openpyxl.utils as utils
from concurrent.futures import ThreadPoolExecutor, wait

# Internal imports
from codeeghtesadi.utils.decorators import log_the_func, measure_time, check_if_up_to_date
from codeeghtesadi.utils.common import maybe_make_dir, get_filename, get_update_date
from codeeghtesadi.utils.database import drop_into_db, connect_to_sql, n_retries # n_retries might be needed or handled in db
# n_retries is global in helpers, locally used in connect_to_sql. Here we don't need it unless we reimplement retry logic.

@measure_time
def read_multiple_excel_sheets(path, sheet_name=None):

    # Read all sheets from the Excel file into a dictionary of DataFrames
    df_dict = pd.read_excel(path, sheet_name=None)

    # Initialize an empty list to store individual DataFrames
    df_list = []

    # Iterate through each sheet in the dictionary
    for key in df_dict:
        # Append each DataFrame to the list
        df_list.append(df_dict[key])

    # Concatenate all DataFrames in the list into a single DataFrame
    concatenated_df = pd.concat(df_list)

    # Return the final concatenated DataFrame
    return concatenated_df

@log_the_func('none')
def remove_excel_files(files=[],      # List to store file paths to be removed
                       pathsave=None,  # Unused parameter, can be removed if not needed
                       file_path=None,  # Path where files are located
                       # File types to be removed (default is 'html')
                       postfix=['html'],
                       *args,  # Additional positional arguments (unused)
                       **kwargs):  # Additional keyword arguments (unused)
    
    if files is None:
        files = []

    # If a specific file_path is provided, find files with specified postfix in that path
    if file_path is not None:
        for item in postfix:
            files.extend(glob.glob(file_path + "/*" + item))

    # Iterate through the list of files to be removed
    for f in files:
        # Check if the file exists
        if os.path.exists(f):
            # If the file exists, remove it
            os.remove(f)

def merge_multiple_excel_sheets(path,
                                dest,
                                return_df=False,
                                delete_after_merge=False,
                                postfix='.xls',
                                db_save=False,
                                table=None,
                                multithread=False):

    # List to store DataFrames from individual sheets
    lst = []

    # Get a list of Excel files in the specified path with the given file extension
    file_list = glob.glob(path + "/*" + postfix)

    # Iterate through each Excel file
    for item in file_list:
        # Open Excel file
        xl = pd.ExcelFile(item)

        # Read the first sheet into a DataFrame
        df = pd.read_excel(item, sheet_name=xl.sheet_names[0])
        df = df.astype('str')

        # Save the DataFrame to the database (if specified)
        drop_into_db(table_name=table,
                     columns=df.columns.tolist(),
                     values=df.values.tolist(),
                     append_to_prev=False)

        # Function to read each sheet (starting from the second) and save to the database
        def df_to_db(sheet):
            df = pd.read_excel(item, sheet_name=sheet)
            df = df.astype('str')
            drop_into_db(table_name=table,
                         columns=df.columns.tolist(),
                         values=df.values.tolist(),
                         append_to_prev=True,
                         del_tbl='no',
                         create_tbl='no')

        # Process sheets sequentially or using multithreading
        if not multithread:
            # Iterate through sheets and save to the database
            for sheet in xl.sheet_names[1:]:
                df_to_db(sheet)
        else:
            # Use ThreadPoolExecutor for multithreading
            # res variable was undefined in original code! defaulting to 10?
            res = 10 
            executor = ThreadPoolExecutor(res - 1)
            jobs = [executor.submit(df_to_db, sheet)
                    for sheet in xl.sheet_names[1:]]
            wait(jobs)

    # Optionally return the merged DataFrame
    if return_df:
        # Combine DataFrames from all sheets into a single DataFrame
        # lst was empty? original code: lst = []; ... loops ... pd.concat(lst)
        # It seems original code returns empty DF if return_df=True?
        merged_df = pd.concat(lst, ignore_index=True)
        return merged_df

    # Optionally delete original Excel files after merging
    if delete_after_merge:
        for item in file_list:
            os.remove(item)

def rename_files(path,
                 dest,
                 prefix='.xls',
                 postfix='.html',
                 file_list=None,
                 *args,
                 **kwargs):
    # If file_list is not provided, use glob to get all files matching the prefix in the source directory
    if file_list is None:
        file_list = glob.glob(path + "/*" + prefix)

    # Iterate through the list of files and rename them
    for i, item in enumerate(file_list):
        # Check if 'years' is specified in keyword arguments
        if 'years' in kwargs:
            # If the current file is in the 'years' dictionary, update the index
            if item in kwargs['years']:
                i = kwargs['years'][item]

        # Create the destination path for the renamed file
        dest1 = os.path.join(dest, '%s%s' % (i, postfix))

        # Rename the file
        os.rename(item, dest1)

def merge_multiple_html_files(path=None,
                              return_df=False,
                              delete_after_merge=True,
                              drop_into_sql=False,
                              drop_to_excel=False,
                              file_list=None,
                              add_extraInfoToDf=False):

    try:
        # If file_list is not provided, fetch all HTML files in the specified path
        if file_list is None:
            file_list = glob.glob(os.path.join(path, "*.html"))

        merge_excels = []
        # Iterate through each HTML file and read it into a DataFrame
        for item in file_list:
            df = pd.read_html(item)[0]
            # If add_extraInfoToDf is True, add extra information to the DataFrame
            if add_extraInfoToDf:
                filename = get_filename(item)
                df['سال عملکرد'] = filename
            merge_excels.append(df)

        # Concatenate all DataFrames into a final DataFrame
        final_df = pd.concat(merge_excels)
        final_df = final_df.astype('str')

        # Add the update date to the DataFrame
        final_df['آخرین بروزرسانی'] = get_update_date()

        # If drop_into_sql is True, drop the DataFrame into a SQL table
        if drop_into_sql:
            drop_into_db('tblArzeshAfzoodeSonati',
                         final_df.columns.tolist(),
                         final_df.values.tolist(),
                         append_to_prev=False)

        # If drop_to_excel is True, save the DataFrame to an Excel file
        if drop_to_excel:
            excel_path = os.path.join(path, 'final_df.xlsx')
            final_df.to_excel(excel_path)

        # If delete_after_merge is True, remove the source HTML files
        if delete_after_merge:
            remove_excel_files(files=file_list)

        # If return_df is True, return the merged DataFrame; otherwise, delete it
        if return_df:
            return final_df
        else:
            del final_df

    except Exception as e:
        # Print any exception that occurs during the process
        print(e)
        return

def read_multiple_excel_files(path, postfix='xls', *args, **kwargs):

    final_file = []
    dirs = []
    dirs.extend([it.path for it in os.scandir(path) if it.is_dir()])
    final_dirs = dirs
    for item in dirs:
        final_dirs.extend([it.path for it in os.scandir(item) if it.is_dir()])
    for item in final_dirs:
        # files = list_files(item, postfix) # list_files is usually in file_ops but here we might need it
        # Assuming list_files logic:
        files = [it.path for it in os.scandir(item) if it.is_file() and it.name.endswith('.' + postfix)]
        for file in files:
            yield file

def merge_multiple_excel_files(path,
                               dest,
                               excel_name='merged',
                               table_name='default',
                               delete_after_merge=True,
                               return_df=False,
                               postfix='xlsx',
                               postfix_after_merge='xlsx',
                               drop_to_sql=False,
                               append_to_prev=False):
    # Construct the destination path
    dest = os.path.join(dest, excel_name)
    maybe_make_dir([dest])
    dest = os.path.join(dest, table_name + '.' + postfix_after_merge)

    # Get the list of excel files in the specified path
    excel_files = glob.glob(os.path.join(path, "*.%s" % postfix))
    merge_excels = []

    # Read each excel file into a DataFrame and append to the list
    for f in excel_files:
        try:
            df = pd.read_excel(f)
            merge_excels.append(df)
        except Exception as e:
            save_excel(f, log=False)
            df = pd.read_excel(f)
            merge_excels.append(df)

    # Merge all DataFrames into one
    final_df_all_fine_grained = pd.concat(merge_excels)
    final_df_all_fine_grained = final_df_all_fine_grained.astype('str')
    final_df_all_fine_grained['تاریخ بروزرسانی'] = get_update_date()

    # Optionally, delete original excel files
    if delete_after_merge:
        remove_excel_files(files=excel_files)

    # Optionally, drop merged data into a SQL database
    if drop_to_sql:
        drop_into_db(table_name=table_name,
                     columns=final_df_all_fine_grained.columns.tolist(),
                     values=final_df_all_fine_grained.values.tolist(),
                     append_to_prev=True)

    # Save the merged DataFrame to an excel file
    final_df_all_fine_grained.to_excel(dest, index=False)

    print('Merging was done successfully')

    # Optionally, return the merged DataFrame
    if return_df:
        return final_df_all_fine_grained
    else:
        # Delete the merged DataFrame if not returned
        del final_df_all_fine_grained

@check_if_up_to_date
def is_updated_to_save(path):
    return True

def save_row(row, tqdm_pandas, csv_name, csv_exists):
    tqdm_pandas.update(1)
    # Append the row to the CSV file
    row.to_frame().transpose().to_csv(csv_name, mode='a', header=not csv_exists)
    return row

@measure_time
def read_excel_withtime(path):
    df = pd.read_excel(path)
    return df

@measure_time
def save_as_csv_file(excel_file):
    irismash = xw.Book(excel_file)
    # Replace 'Sheet1' with the actual sheet name
    sheet = irismash.sheets['Sheet1']
    # Read data from the sheet into a pandas DataFrame
    data = sheet.range('A1').expand().options(pd.DataFrame).value
    # Save the DataFrame to a CSV file
    csv_name = f"{excel_file.split('.')[0]}.csv"
    data.to_csv(csv_name, index=False)
    irismash.app.quit()

def cleanupdf(data):
    data.fillna(0, inplace=True)
    data.replace('nan', '0', inplace=True)
    data.replace('None', '0', inplace=True)
    data.replace('NaN', '0', inplace=True)
    data.replace('NaT', '0', inplace=True)
    # Identify numeric columns
    numeric_columns = data.apply(lambda x: pd.to_numeric(
        x, errors='coerce').notna().all())
    numeric_columns = data.columns[numeric_columns].tolist()
    data[numeric_columns] = data[numeric_columns].apply(
        np.float64).apply(np.int64)
    data = data.astype('str')
    data.replace('0', '', inplace=True)
    return data

@measure_time
def save_excel(excel_file, log=True, only_save=True, save_as_csv=False,
               save_into_sql=False, table_name=None, db_name=None, multi_process=False, append_to_prev=False):
    # irismash = xw.Book(excel_file, visible=False)
    # Open the Excel application
    app = xw.App(visible=False)
    # Open the Excel file
    irismash = app.books.open(excel_file)
    if only_save:
        irismash.save()
        irismash.app.quit()
        time.sleep(8)
        return
    # Save the DataFrame to a CSV file
    csv_name = f"{excel_file.split('.')[0]}.csv"
    # Replace 'Sheet1' with the actual sheet name
    sheet = irismash.sheets['Sheet1']
    # Read data from the sheet into a pandas DataFrame
    data = sheet.range('A1').expand().options(pd.DataFrame).value
    irismash.app.quit()
    data["آخرین بروزرسانی"] = get_update_date()
    data = cleanupdf(data)
    if save_as_csv:
        data.to_csv(csv_name, index=False)
    if save_into_sql:
        if multi_process:
            data = np.array_split(data, 40)
            drop_into_db(table_name=table_name,
                         columns=data[0].columns.tolist(),
                         values=data[0].values.tolist(),
                         append_to_prev=append_to_prev,
                         db_name=db_name)
            append_to_prev = True
            with ThreadPoolExecutor(39) as executor:
                jobs = [executor.submit(drop_into_db, table_name,
                                        item.columns.tolist(),
                                        item.values.tolist(),
                                        append_to_prev,
                                        db_name
                                        )
                        for index, item in tqdm(enumerate(data[1:]))]
                wait(jobs)

        else:
            data = np.array_split(data, 10000)
            append_to_prev = False
            for item in tqdm(data):
                drop_into_db(table_name=table_name,
                             columns=item.columns.tolist(),
                             values=item.values.tolist(),
                             append_to_prev=append_to_prev,
                             db_name=db_name)
                append_to_prev = True


def read_excel_and_drop_into_db(file, table_name,
                                append_to_prev=False, db_name='TestDb'):

    df = read_excel_withtime(file)
    df = cleanupdf(df)
    df['آخرین بروزرسانی'] = get_update_date()
    drop_into_db(table_name=table_name,
                 columns=df.columns.tolist(),
                 values=df.values.tolist(),
                 append_to_prev=append_to_prev,
                 db_name='TestDb')
    append_to_prev = True


def open_and_save_excel_files(path, report_type=None, year=None,
                              merge=False, only_save=True, save_as_csv=False,
                              save_into_sql=False, table_name=None,
                              db_name=None, postfixes=['xlsx', 'xls', 'html'], append_to_prev=False,
                              multi_process=False, *args, **kwargs):
    # Get a list of all Excel files in the specified path
    excel_files = []

    for postfix in postfixes:
        excel_files.extend(glob.glob(os.path.join(path, f"*.{postfix}")))

    # Iterate through each Excel file in the list
    for f in excel_files:
        # Call the save_excel function to save the Excel file
        save_excel(f, only_save=only_save, save_as_csv=save_as_csv,
                   save_into_sql=save_into_sql, multi_process=multi_process,
                   table_name=table_name, db_name=db_name, append_to_prev=append_to_prev)

def is_updated_to_download(table_name=None, type_of='download'):

    # Construct SQL query to retrieve the latest update for the specified table and type
    sql_query = """SELECT * FROM tblLog WHERE table_name = '%s'
                    and update_date = (select MAX(update_date) FROM tblLog 
                    WHERE table_name = '%s')
                    and type_of = '%s'""" % (table_name, table_name, type_of)

    # Connect to the database and execute the SQL query
    df = connect_to_sql(
        sql_query, sql_con=None, read_from_sql=True, return_df=True)

    # Check if there are rows returned by the query
    if int(df.count().iloc[0]) > 0:
        # Check if the update date of the first row matches the current update date
        if df['update_date'].loc[0] == get_update_date():
            # The table has been updated and is ready for download
            return True

    # The table has not been updated or does not meet the criteria
    return False

def check_if_col_exists(df, col):

    if col in df:
        return True
    else:
        return False

@measure_time
def create_df(excel_files, year=None, report_type=None, type_of_report=None):

    merge_excels = []
    # Convert excel files into dataframes
    for f in excel_files:
        if f == 'C:\\ezhar-temp\\1392\\amar_sodor_gharar_karshenasi\\گزارش آمار صدور قرار کارشناسی.xlsx':
            df = pd.read_excel(f)
            merge_excels.append(df)

        if f == r'C:\ezhar-temp\1392\imp_parvand\صد و دویست پرونده انتخابی دادستانی.xlsx':
            df = pd.read_excel(f)
            merge_excels.append(df)
        if f == r'C:\ezhar-temp\%s\%s\Excel.xlsx' % (year, report_type):
            df = pd.read_excel(f)

            if (check_if_col_exists(df, "منبع مالیاتی") == False):
                df.insert(11,
                          column='منبع مالیاتی',
                          value='مالیات بر درآمد شرکت ها')
            merge_excels.append(df)

        if f == r'C:\ezhar-temp\%s\%s\Excel(1).xlsx' % (year, report_type):
            df = pd.read_excel(f)
            if (check_if_col_exists(df, "منبع مالیاتی") == False):
                df.insert(11,
                          column='منبع مالیاتی',
                          value='مالیات بر درآمد مشاغل')
            merge_excels.append(df)

        if f == r'C:\ezhar-temp\%s\%s\Excel(2).xlsx' % (year, report_type):
            df = pd.read_excel(f)
            if (check_if_col_exists(df, "منبع مالیاتی") == False):
                df.insert(11,
                          column='منبع مالیاتی',
                          value='مالیات بر ارزش افزوده')
            merge_excels.append(df)

        if f in [r'C:\ezhar-temp\%s\%s\جزئیات اعتراضات و شکایات.html' % (year, report_type),
                 r'C:\ezhar-temp\%s\%s\گزارش شکايات در جريان دادرسی در هيات بدوی، تجديد نظر و هم عرض.html' % (
                     year, report_type),
                 r'C:\ezhar-temp\%s\%s\جستجو.html' % (year, report_type)]:

            df = pd.read_html(f, flavor="html5lib")[0]
            merge_excels.append(df)

    # Merge all dataframes into one
    final_df_all_fine_grained = pd.concat(merge_excels)
    final_df_all_fine_grained.fillna(0, inplace=True)
    final_df_all_fine_grained.replace('nan', '0', inplace=True)
    final_df_all_fine_grained.replace('None', '0', inplace=True)
    final_df_all_fine_grained.replace('NaN', '0', inplace=True)
    final_df_all_fine_grained.replace('NaT', '0', inplace=True)
    # Find columns with dtype float
    float_columns = final_df_all_fine_grained.select_dtypes(
        include='float64').columns
    # Convert float columns to int
    final_df_all_fine_grained[float_columns] = final_df_all_fine_grained[float_columns].apply(
        np.int64)

    # Clean the Dataframe
    final_df_all_fine_grained = final_df_all_fine_grained.fillna(value=0)
    final_df_all_fine_grained = final_df_all_fine_grained.astype('str')
    final_df_all_fine_grained.replace('0', '', inplace=True)
    final_df_all_fine_grained.replace(0, '', inplace=True)

    final_df_all_fine_grained['تاریخ بروزرسانی'] = get_update_date()

    return final_df_all_fine_grained.values.tolist(
    ), final_df_all_fine_grained.columns

def highlight_excel_cells(df, rows_range, cols_range=None, saved_path=None, hex_color='4d4dff'):

    # Create a new Excel workbook and select the active sheet
    wb = Workbook()
    ws = wb.active

    # Write the DataFrame to the Excel sheet
    for r in dataframe_to_rows(df, index=False, header=True):
        ws.append(r)

    # If cols_range is not provided, generate default columns (A-Z)
    if cols_range is None:
        max_cols = math.floor((ws.max_column / 26))
        lst_alphabet = list(get_column_letter(i) for i in range(1, 27))

        cols_range = []
        cols_range.extend(lst_alphabet)

        for item in lst_alphabet[:max_cols]:
            for w in lst_alphabet:
                cols_range.append(f'{item}{w}')

    # Highlight specified cells in the given rows and columns with the specified hex color
    for i in range(1, rows_range + 1):  # Note: Adjusted range to include the last row
        for item in cols_range:
            cell = ws['%s%s' % (item, i)]
            # Uncomment the line below to change font color (example: red)
            # cell.font = styles.Font(color="FF0000")
            cell.fill = styles.fills.PatternFill(
                patternType='solid', fgColor=styles.Color(rgb=hex_color))

    # Save the workbook to the specified path, if provided
    if saved_path is not None:
        wb.save(saved_path)

def df_to_excelsheet(saved_path, dict_df, index, *args, **kwargs):

    # Use pd.ExcelWriter to efficiently write multiple DataFrames to an Excel file
    with pd.ExcelWriter(saved_path) as writer:
        for key, dataframe in dict_df.items():
            # Handle tuple keys for composite sheet names
            if isinstance(key, tuple):
                sheet_name = ''
                for item in key:
                    # Remove specified names from the sheet name
                    if 'names' in kwargs:
                        for name in kwargs['names']:
                            if name in item:
                                item = item.replace(name, '')
                    sheet_name += item + '-'
            else:
                # Convert non-tuple keys to string for sheet name
                sheet_name = str(key)

            # Write DataFrame to Excel sheet
            dataframe.to_excel(writer, sheet_name=sheet_name, index=index)


def df_to_excelworkbooks(df, g_cols, saving_path=None,
                         zipped_output={'zipped': True,
                                        'dir': r'E:\automating_reports_V2\saved_dir\arzeshafzoodeh_sonati\test\final'},
                         *args, **kwargs):
    from codeeghtesadi.utils.file_ops import zipdir # late import

    # Set default saving path if not provided
    if saving_path is None:
        saving_path = r'E:\automating_reports_V2\saved_dir\arzeshafzoodeh_sonati\test\final\test'

    # Group the DataFrame based on specified columns
    grouped_df = df.groupby(g_cols)

    # Iterate through each group and save as separate Excel workbook
    for key, item in grouped_df:
        saved_dir = os.path.join(saving_path, key)
        if not os.path.exists(saved_dir):
            os.makedirs(saved_dir)
        name = key + '.xlsx'
        item.to_excel(os.path.join(saved_dir, name))

    # Check if zipping is enabled
    if zipped_output['zipped']:
        # Create the directory for zipped output if it doesn't exist
        if not os.path.exists(zipped_output['dir']):
            os.makedirs(zipped_output['dir'])
        # Create a zip file and add the contents of the saving path
        file_name = os.path.join(zipped_output['dir'], 'final.zip')
        import zipfile
        with zipfile.ZipFile(file_name, 'w', zipfile.ZIP_DEFLATED) as zipf:
            zipdir(saving_path, zipf)
