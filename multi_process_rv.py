import pandas as pd
import glob
import os
import numpy as np
import xlsxwriter
from concurrent.futures import ProcessPoolExecutor

from helpers import drop_into_db

dir = r'E:\automating_reports_V2\saved_dir\codeghtesadi\ravand'


def leading_zero(x):
    '''
    This function pads zero at the beginning of strings
    '''
    x = x.replace('.0', '')
    if len(str(x)) == 8 or len(str(x)) == 12:
        return f'00{x}'
    elif len(str(x)) == 9 or len(str(x)) == 13:
        return f'0{x}'
    elif len(str(x)) == 12:
        return f'00{x}'
    else:
        return str(x)


files = glob.glob(os.path.join(dir, '*.csv'))

# first_row = pd.read_csv(files[0], nrows=1).astype('str')

# drop_into_db(table_name='tblRavand',
#              columns=first_row.columns.tolist(),
#              values=first_row.values.tolist(),
#              append_to_prev=False,
#              db_name='testdbV2')


def process_file_in_chunks(item, chunksize=80_000):
    """
    Function to process a single CSV file in chunks.
    """

    for chunk in pd.read_csv(item, chunksize=chunksize):
        # Fill NaN and replace invalid values
        chunk.fillna(0, inplace=True)
        chunk.replace(['nan', 'None', 'NaN', 'NaT'], 0, inplace=True)

        # Convert float columns to integers
        float_columns = chunk.select_dtypes(include='float64').columns
        chunk[float_columns] = chunk[float_columns].apply(np.int64)

        # Apply leading_zero to شناسه مودی column
        chunk['شناسه مودی'] = chunk['شناسه مودی'].apply(
            lambda x: leading_zero(str(x)))

        # Drop unnecessary columns
        chunk.drop(columns=['شماره تشخیص', 'شماره قطعی'],
                   inplace=True, errors='ignore')

        drop_into_db(table_name='tblRavand',
                     columns=chunk.columns.tolist(),
                     values=chunk.values.tolist(),
                     append_to_prev=True,
                     db_name='testdbV2')

        # Write each chunk to the Excel file in append mode, and update the start row


if __name__ == "__main__":
    print('START')

    # Use ProcessPoolExecutor to process files in parallel with limited workers
    with ProcessPoolExecutor(max_workers=4) as executor:
        executor.map(process_file_in_chunks, files)

    print('All files processed.')

    print('finish')
