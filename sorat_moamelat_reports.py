import os
from helpers import unzip_files, read_multiple_excel_files, maybe_make_dir
import pandas as pd
# unzip_files(
#     r'E:\automating_reports_V2\saved_dir\soratmoamelat\reports', remove=False)

# print(f)

mapped_cols = {
    'ارز': 'معادل ریالی ',
    'تأمين کنندگان ارز': 'معادل ریالی ',
    'اموال تمليکي': 'مبلغ خالص ',
    'پيمانکار قرارداد': 'مبلغ کل قرارداد',
    'تدارکات الکترونيکي': 'مالیات ارزش افزوده',
    'تسهيلات بانکي': 'مبلغ سود  ',
    'تعهدات بانکي': 'مبلغ اصل',
    'خريدار بورس': ' ارزش قرارداد کالا(هزار ریال)',
    'درآمد دفاتر خدمات الکترونيک قضايي': 'جمع درآمد سالانه',
    'دریافت کنندگان ارز': 'معادل ریالی',
    'سکه و طلا و جواهرات،کارشناسان رسمي': 'مبلغ کارکرد برای کارشناسان/قیمت کل برای سکه،طلا و جواهرات ',
    'فروشنده بورس': 'ارزش قرارداد کالا(هزار ریال)',
    'وکيل بورس': 'ارزش قرارداد (هزار ریال)',
    'کارفرما قرارداد': 'مبلغ کل قرارداد',
    'وکالت نامه - سردفتر': 'حق التحرير',
    'وکالت نامه - دفتریار': 'حق التحرير',
    'مبایعه نامه های صادره بنگاه های معاملات املاک': 'ارزش قرارداد کالا(هزار ریال)',
    'وکالت نامه - طرفین سند حقوقی': 'حق التحرير'
}

all_df = []

files = read_multiple_excel_files(
    r'E:\automating_reports_V2\saved_dir\soratmoamelat\reports', postfix='xlsx')

while (True):
    try:

        file = next(files)

        try:
            col_name = file.split('\\')[-2]
            year = file.split('\\')[-4]
            base_dir = r'E:\automating_reports_V2\saved_dir\soratmoamelat\final_dir'
            final_dir = os.path.join(base_dir, year, col_name)
            maybe_make_dir([final_dir])
            final_file = final_dir + '.xlsx'
            selected_col = mapped_cols[col_name]

            df = pd.read_excel(file, skiprows=1)

            c_df = df[selected_col].loc[df[selected_col] > 500_000_000].count()
            if c_df == 0:
                continue
            s_df = df[selected_col].loc[df[selected_col] > 500_000_000].sum()

            df_new = pd.DataFrame({'نوع': [col_name], 'سال': [
                                  year], 'تعداد': [c_df], 'جمع': s_df})
            all_df.append(df_new)
        except Exception as e:
            print(e)
            continue
    except:
        all_final_df = pd.concat(all_df).to_excel(
            r'E:\automating_reports_V2\saved_dir\soratmoamelat\final_dir\final.xlsx')
        break
