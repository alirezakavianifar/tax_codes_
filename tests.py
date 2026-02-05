from helpers import rename_files, merge_multiple_html_files, merge_multiple_excel_sheets, merge_multiple_html_files
import glob
import pandas as pd


def wrap(func):

    def tryit(*args, **kwargs):

        print(args)
        print('1')
        result = None
        result = func(*args, **kwargs)
        print('3')
        return result
    return tryit


def param1(log=True):
    def wrap(func):
        def tryit(*args, **kwargs):
            if log == False:
                print(args)
            result = func(*args, **kwargs)
            print('4')
            return result
        return tryit
    return wrap


@wrap
@param1(log=False)
def test(name):
    print(name)


test('name')

# merge_multiple_excel_sheets(r'E:\automating_reports_V2\saved_dir\codeghtesadi',
# r'E:\automating_reports_V2\saved_dir\codeghtesadi')

df_codeeghtesadi = pd.read_excel(
    r'E:\automating_reports_V2\saved_dir\codeghtesadi\codeeghtesadi.xlsx')

# df_arzesh = merge_multiple_html_files(
#     r'E:\automating_reports_V2\saved_dir\arzeshafzoodeh_sonati\temp', return_df=True)

df_arzesh = pd.read_csv(
    r'E:\automating_reports_V2\saved_dir\arzeshafzoodeh_sonati\temp\final_df.csv')

df_sabtarzesh = pd.read_excel(
    r'E:\automating_reports_V2\saved_dir\arzeshafzoodeh_sonati\temp\ثبت نام ارزش افزوده.xlsx')

df_arzesh_selectedColumns = ['شماره پرونده',  'شماره اقتصادی',
                             'نام شرکت/فروشگاه/کارگاه', 'نام و نام خانوادگی', 'نوع', 'کد ملی',
                             'واحد مالیاتی', 'واحد مالیات برارزش افزوده',
                             'شماره ثبت/شناسنامه', 'شهرستان'
                             ]

df_merged_selectedColumns = ['کد پستی']
df_merged_selectedColumns = df_merged_selectedColumns + df_arzesh_selectedColumns

df_arzesh = df_arzesh[df_arzesh_selectedColumns]
df_merged = df_arzesh.merge(
    df_sabtarzesh, how='inner', left_on='شماره پرونده', right_on='شناسه')

# df_merged['gasht'] = df_merged['كدپستي'].astype('str').str.slice(0, 5)

df_gasht = pd.read_excel(
    r'E:\automating_reports_V2\saved_dir\arzeshafzoodeh_sonati\temp\گشت پستی استان.xlsx')

df_merged_1 = df_merged.merge(df_codeeghtesadi, how='left',
                              left_on='کدرهگیری', right_on='کد رهگیری')


df_merged_1['کد پستی'] = df_merged_1['کد پستی'].str.slice(0, 5)
df_gasht['گشت پستی'] = df_gasht['گشت پستی'].str.slice(0, 5)

df_merged_3 = df_merged_1.merge(df_gasht, how='left',
                                left_on='کد پستی', right_on='گشت پستی')


final_selected_columns = ['شماره پرونده_x', 'شماره اقتصادی_x', 'نام شرکت/فروشگاه/کارگاه',
                          'نام و نام خانوادگی', 'نوع_x', 'کد ملی', 'واحد مالیاتی',
                          'واحد مالیات برارزش افزوده_x', 'شماره ثبت/شناسنامه', 'شهرستان_x',
                          'رديف', 'شناسه ملی_x', 'کد اداره امور مالیاتی']

df_merged_4 = df_merged_3[final_selected_columns]

df_merged_3.to_excel('test.xlsx')


df_merged = df_merged[df_merged_selectedColumns]

print('dd')


# df = pd.read_excel(
# r'E:\automating_reports_V2\saved_dir\codeghtesadi\codeeghtesadi.xlsx', usecols='G,L')


# df.to_csv(r'E:\automating_reports_V2\saved_dir\codeghtesadi\codeeghtesadi.csv')
