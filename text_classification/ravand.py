import pandas as pd
import glob
import os
import numpy as np
import xlsxwriter

def leading_zero(x):
    '''
    Optimized version of adding leading zeros using zfill.
    '''
    x = x.replace('.0', '')
    return x.zfill(12) if len(x) < 12 else x

dict1 = {
    'progress_v2_1395_header': ['روند رسیدگی 1395 سنیم'],
    'progress_v2_1396_header': ['روند رسیدگی 1396 سنیم'],
    'progress_v2_1397_header': ['روند رسیدگی 1397 سنیم'],
    'progress_v2_1398_header': ['روند رسیدگی 1398 سنیم'],
    'progress_v2_1399_header': ['روند رسیدگی 1399 سنیم'],
    'progress_v2_1400_header': ['روند رسیدگی 1400 سنیم'],
    'progress_v2_1401_header': ['روند رسیدگی 1401 سنیم'],
    'progress_v2_1402_header': ['روند رسیدگی 1402 سنیم'],
}

dir = r'E:\ravand'
files = glob.glob(os.path.join(dir, '*.csv'))
print('START')

for item in files:
    name = item.rsplit('\\', 1)[1].split('.')[0]
    print(f'Start_{name}')
    
    # Reading CSV with optimized dtypes
    df = pd.read_csv(item, dtype={'شناسه مودی': str})
    
    # Filling NaNs efficiently in one step
    df.fillna(0, inplace=True)
    
    # Find columns with dtype float
    float_columns = df.select_dtypes(include='float64').columns
    if not float_columns.empty:
        df[float_columns] = df[float_columns].apply(np.int64)
    
    # Apply leading zero padding
    df['شناسه مودی'] = df['شناسه مودی'].apply(leading_zero)
    
    # Drop unnecessary columns
    df.drop(columns=['شماره تشخیص', 'شماره قطعی'], inplace=True, errors='ignore')
    
    print(f'Read_{name}')
    
    # Optimized path construction
    path = os.path.join(dir, 'Excel', f'{dict1[name][0]}.xlsx')

    # Writing to Excel with optimized writer
    with pd.ExcelWriter(path, engine='xlsxwriter') as writer:
        workbook = writer.book
        workbook.use_zip64()
        
        df.drop_duplicates(inplace=True)
        df.to_excel(writer, sheet_name="sheet1", index=False)
        
        worksheet = writer.sheets['sheet1']
        border_fmt = workbook.add_format(
            {'bottom': 1, 'top': 1, 'left': 1, 'right': 1})
        
        worksheet.conditional_format(
            xlsxwriter.utility.xl_range(0, 0, len(df), len(df.columns)),
            {'type': 'no_errors', 'format': border_fmt}
        )
        worksheet.right_to_left()
    
    print(f'Write_{name}')
