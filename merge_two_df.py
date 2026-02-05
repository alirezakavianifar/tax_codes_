import pandas as pd
import os
from helpers import df_to_excelsheet, read_multiple_excel_files, connect_to_sql, merge_multiple_excel_files
from sql_queries import get_sql_all_tashkhis_mash, get_sql_all_ghatee_mash
from constants import get_sql_con
import re

path_ezhar_87_95 = r'E:\automating_reports_V2\saved_dir\arzeshafzoodeh_sonati\test\final\ezhar87-95\ezhar87-95\arzesh87-95.csv'
path_ezhar_noresidegi_87_95 = r'E:\automating_reports_V2\saved_dir\arzeshafzoodeh_sonati\temp\final_df.xlsx'
path_ghatee_hoghoghi = r'E:\automating_reports_V2\saved_dir\arzeshafzoodeh_sonati\test\final\ghatee87-95\final_ghatee.xlsx'

ff = r'E:\automating_reports_V2\saved_dir\arzeshafzoodeh_sonati'

x = read_multiple_excel_files(ff, postfix='xlsx')
lst = []

is_true = True

while is_true:

    try:
        path = next(x)
        df = pd.read_excel(path)
        lst.append(df)

    except:
        dff = pd.concat(lst)
        dff.to_excel(os.path.join(
            ff, 'final_ghatee.xlsx'))
        is_true = False

print('d')


df_ezhar = pd.read_csv(path_ezhar_87_95, on_bad_lines='skip', sep='؛')
df_ezhar_no_residegi = pd.read_excel(path_ezhar_noresidegi_87_95)

cols = df_ezhar.columns.tolist()
cols.extend(['_merge'])
df_merge = df_ezhar.merge(
    df_ezhar_no_residegi, how='left', left_on=['FollowCode'],
    right_on=['شماره پرونده', 'سال عملکرد'], indicator=True)

df_merge = df_merge[cols]
df_merge = df_merge.loc[df_merge['_merge'] == 'left_only']

df_merge.to_excel(
    r'E:\automating_reports_V2\saved_dir\arzeshafzoodeh_sonati\test\final\ezhar87-95\ezhar87-95\final_residegi_.xlsx')

counts = df.groupby(['شماره پرونده ارزش افزوده',
                    'سال عملكرد']).size().reset_index()

counts.to_excel(
    r'E:\automating_reports_V2\saved_dir\arzeshafzoodeh_sonati\test\final\ezhar87-95\ezhar87-95\final_group.xlsx')


def convert_it(item):
    if len(str(item)) == 8:
        return '00' + str(item)
    elif len(str(item)) == 9:
        return '0' + str(item)
    else:
        return str(item)


df_ezhar['کد ملی صحیح'] = df_ezhar['كد ملی'].apply(
    lambda item: convert_it(item))

df_ezhar.drop_duplicates(
    subset=['شماره پرونده ارزش افزوده'], inplace=True)

lst_all = []
str_code_mellis = []
for index, row in df_ezhar.iterrows():
    if len(row["کد ملی صحیح"]) == 10:
        str_code_mellis.append("""'%s'""" % str(row['کد ملی صحیح']))

str_code_melliss = ''

for item in str_code_mellis:
    str_code_melliss += str(item) + ','

str_code_mellisss = str_code_melliss[:-1]

sql_query = get_sql_all_tashkhis_mash(
    '1387', '1395', str_code_mellisss)

# sql_query = get_sql_all_ghatee_mash(
#     '1387', '1395', str_code_mellisss)

sql_query = re.sub("(\(')(?!.*\1)", '(', sql_query)
sql_query = re.sub("('\))(?!.*\1)", ')', sql_query)

orig_text = "(16439','16649','16718)"
rep_text = "('16439','16649','16718')"

sql_query = sql_query.replace(orig_text, rep_text)

df = connect_to_sql(
    sql_query, sql_con=get_sql_con(server='10.52.0.50\AHWAZ',
                                   database='MASHAGHEL_TAJMI',
                                   username='mash',
                                   password='123456'),
    read_from_sql=True, return_df=True)

cols = df_ezhar.columns.tolist()
cols.extend(['ماليات تشخيص', '_merge'])
df_ezhar = df_ezhar.astype('str')
df = df.astype('str')

merged_tashkhis_hoghoghi = df_ezhar.merge(
    df, how='left', left_on=['کد ملی صحیح', 'سال عملكرد'],
    right_on=['کد ملی', 'عملکرد'], indicator=True)

merged_tashkhis_hoghoghi = merged_tashkhis_hoghoghi[cols]

merged_tashkhis_hoghoghi.to_excel(
    r'E:\automating_reports_V2\saved_dir\arzeshafzoodeh_sonati\test\final\ezhar87-95\ezhar-87-95_new__tashkhis__.xlsx')

merged_gharee_hoghoghi = df_ezhar_hoghoghi.merge(
    df_ghatee_hoghoghi, how='left', left_on=['شناسه ملی', 'سال عملكرد'], right_on='شناسه ملی')

merged_tashkhis_hoghoghi['percentage'] = (
    merged_tashkhis_hoghoghi['جمع فروش همه دوره ها']+0.00000000001) / (merged_tashkhis_hoghoghi['مالیات کل'] + 0.00000000001)

merged_gharee_hoghoghi['percentage'] = (
    merged_gharee_hoghoghi['جمع فروش همه دوره ها']+0.00000000001) / (merged_gharee_hoghoghi['مالیات کل'] + 0.00000000001)

cols = df_ezhar_hoghoghi.columns.tolist()
cols.extend(['percentage', 'درآمد مشمول',
            'جمع فروش همه دوره ها'])

merged_tashkhis_hoghoghi = merged_tashkhis_hoghoghi[cols]
merged_gharee_hoghoghi = merged_gharee_hoghoghi[cols]


merged_tashkhis_hoghoghi.to_excel(
    r'E:\automating_reports_V2\saved_dir\arzeshafzoodeh_sonati\test\hoghoghi\arzesh-93-95\merged_tashkhis_hoghoghi.xlsx')

merged_gharee_hoghoghi.to_excel(
    r'E:\automating_reports_V2\saved_dir\arzeshafzoodeh_sonati\test\hoghoghi\arzesh-93-95\merged_gharee_hoghoghi.xlsx')

# print('f')
# path = r'E:\automating_reports_V2\saved_dir\arzeshafzoodeh_sonati\test\hoghoghi\tashkhis\final\final_tashkhis_without_duplicates.xlsx'
# files = pd.read_excel(path)
# files.drop_duplicates(
#     subset=['شناسه ثبت نام ارزش افزوده'], keep='first', inplace=True)
# files.to_excel(
#     r'E:\automating_reports_V2\saved_dir\arzeshafzoodeh_sonati\test\hoghoghi\tashkhis\final\final_tashkhis_without_duplicates_.xlsx')

# lst_all = []
# try:
#     while True:
#         file = next(files)
#         df_hoghoghi = pd.read_excel(file)
#         lst_all.append(df_hoghoghi)

# except Exception as e:
#     print(e)
#     df = pd.concat(lst_all)
#     df.to_excel(
#         r'E:\automating_reports_V2\saved_dir\arzeshafzoodeh_sonati\test\hoghoghi\tashkhis\final\final_tashkhis.xlsx')

ezhar_arzesh_path = r'E:\automating_reports_V2\saved_dir\arzeshafzoodeh_sonati\test\ezhar_Arzesh.xlsx'
hoghoghi_ghatee_path = r'E:\automating_reports_V2\saved_dir\arzeshafzoodeh_sonati\test\hoghoghi\ghatee'
hoghoghi_tashkhis_path = r'E:\automating_reports_V2\saved_dir\arzeshafzoodeh_sonati\test\hoghoghi\tashkhis'
hoghoghi_arzesh_path = r'E:\automating_reports_V2\saved_dir\arzeshafzoodeh_sonati\test\hoghoghi\ezhar_Arzesh__hoghoghi.xlsx'
haghighi_arzesh_path = r'E:\automating_reports_V2\saved_dir\arzeshafzoodeh_sonati\test\hoghoghi\arzesh-93-95\ezhar_arzesh_haghighi.xlsx'
haghighi_tashkhis_path = r'E:\automating_reports_V2\saved_dir\arzeshafzoodeh_sonati\test\haghighi\haghighi_tashkhis_all.xlsx'


# df_haghighi_arzesh = pd.read_excel(haghighi_arzesh_path)
# df_haghighi_taskhis = pd.read_excel(haghighi_tashkhis_path)
# df_haghighi_arzesh.drop_duplicates(
#     subset=['شناسه ثبت نام ارزش افزوده', 'سال'], inplace=True)

# merged = df_haghighi_arzesh.merge(
#     df_haghighi_taskhis, how='inner', left_on=['کدملی', 'سال'], right_on=['کد ملی', 'سال'], indicator=True)
# merged['percentage'] = (
#     merged['جمع فروش همه دوره ها']+0.00000000001) / (merged['ماليات کل'] + 0.00000000001)

# merged = merged[cols]
# haghighi_tashkhis_path = r'E:\automating_reports_V2\saved_dir\arzeshafzoodeh_sonati\test\haghighi\haghighi_tashkhis_all.xlsx'
# merged.to_excel(r'E:\automating_reports_V2\saved_dir\arzeshafzoodeh_sonati\test\haghighi\haghighi_tashkhis_final.xlsx')

# cols = df_haghighi_arzesh.columns.tolist()
# cols.extend(['percentage', 'ماليات کل',
#             'جمع فروش همه دوره ها'])
# merged['percentage'] = (
#     merged['جمع فروش همه دوره ها']+0.00000000001) / (merged['ماليات کل'] + 0.00000000001)
# merged = df_haghighi_arzesh.merge(
#     df_haghighi_taskhis, how='inner', left_on=['کد ملی', 'سال'], right_on=['کد ملی', 'سال'], indicator=True)


# # merged = merged[cols]
# merged.to_excel(
#     r'E:\automating_reports_V2\saved_dir\arzeshafzoodeh_sonati\test\haghighi\final_mash.xlsx')


df_arzesh_haghighi = pd.read_excel(haghighi_arzesh_path)


def convert_it(item):
    if len(str(item)) == 8:
        return '00' + str(item)
    elif len(str(item)) == 9:
        return '0' + str(item)
    else:
        return str(item)


df_arzesh_haghighi['کد ملی صحیح'] = df_arzesh_haghighi['كد ملی'].apply(
    lambda item: convert_it(item))


lst_all = []
str_code_mellis = ''
for index, row in df_arzesh_haghighi.iterrows():
    if len(row["کد ملی صحیح"]) == 10:
        str_code_mellis += str(row["کد ملی صحیح"]) + ','

str_code_mellis = str_code_mellis[10:]


sql_query = get_sql_all_tashkhis_mash(
    '1393', '1395', str_code_mellis)
df = connect_to_sql(
    sql_query, sql_con=get_sql_con(server='10.52.0.50\AHWAZ',
                                   database='MASHAGHEL_TAJMI',
                                   username='mash',
                                   password='123456'), read_from_sql=True, return_df=True)

lst_all.append(df)

df = pd.concat(lst_all)
df.to_excel(r'E:\automating_reports_V2\saved_dir\arzeshafzoodeh_sonati\test\hoghoghi\arzesh-93-95\tashkhis_amalkard_93_95_haghighi.xlsx')

# cols = df_arzesh_haghighi.columns.tolist()
# cols.extend(['percentage', 'مالیات کل',
#             'جمع فروش همه دوره ها'])


# Hoghoghi Part
files = read_multiple_excel_files(
    r'E:\automating_reports_V2\saved_dir\arzeshafzoodeh_sonati\test\final\ezhar87-95\New folder (2)', postfix='xlsx')

lst_all = []
try:
    while True:
        file = next(files)

        df_hoghoghi = pd.read_excel(file)
        lst_all.append(df_hoghoghi)
except Exception as e:
    print('e')

df = pd.concat(lst_all)
df.to_excel(r'E:\automating_reports_V2\saved_dir\arzeshafzoodeh_sonati\test\hoghoghi\ghatee\final\hoghoghi_all_ghatee.xlsx')


# df_arzesh_hoghoghi = pd.read_excel(hoghoghi_arzesh_path)

# cols = df_arzesh_hoghoghi.columns.tolist()
# cols.extend(['percentage', 'مالیات کل',
#             'جمع فروش همه دوره ها'])

try:
    while True:
        file = next(files)

        df_hoghoghi = pd.read_excel(file)
        year = int(file.split('\\')[-1].split('.')[0])
        file_name = 'hoghoghi%s.xlsx' % str(year)
        df_arzesh_hoghoghi_ = df_arzesh_hoghoghi.loc[df_arzesh_hoghoghi['سال'] == year]
        merged = df_arzesh_hoghoghi_.merge(
            df_hoghoghi, how='inner', left_on='شناسه ملی', right_on='شناسه ملی', indicator=True)
        merged['percentage'] = (
            merged['جمع فروش همه دوره ها']+0.00000000001) / (merged['مالیات کل'] + 0.00000000001)

        merged = merged[cols]
        merged.drop_duplicates(subset='شناسه ملی',
                               keep="first", inplace=True)

        merged.to_excel(
            r'E:\automating_reports_V2\saved_dir\arzeshafzoodeh_sonati\test\hoghoghi\tashkhis\%s' % file_name)

except Exception as e:
    print(e)
    print('f')

df_ag = merged.groupby('نام اداره')

df = df_to_excelsheet(
    r'E:\automating_reports_V2\saved_dir\tmp\final_new.xlsx', df_ag, 1)


# x = read_multiple_excel_files(path_ghatee_hoghoghi, postfix='xlsx')
# lst = []

# is_true = True

# while is_true:

#     try:
#         path = next(x)
#         year = '13' + str(path.split('\\')[-1].split('.')[0])
#         df = pd.read_excel(path)
#         df['سال عملکرد'] = year
#         lst.append(df)

#     except:
#         dff = pd.concat(lst)
#         dff.to_excel(os.path.join(
#             path_ghatee_hoghoghi, 'final_ghatee.xlsx'))
#         is_true = False

# print('d')

def if_hoghoghi(x):
    if len(x) > 8:
        return 'حقوقی'
    else:
        return 'حقیقی'


df_ezhar = pd.read_excel(path_ezhar_87_95)
df_ezhar['نوع مودی'] = df_ezhar['شناسه ملی'].apply(
    lambda x: if_hoghoghi(str(x)))

df_ezhar = df_ezhar.loc[df_ezhar['نوع مودی'] == 'حقوقی']

df_ag = df_ezhar.groupby(['شناسه ملی', 'سال عملكرد'])
lst = []

for key, item in df_ag:
    item['جمع فروش همه دوره ها'] = item['فروش ابرازی'].sum()
    # item.drop_duplicates(subset=['شناسه ملی', 'سال'], inplace=True)
    lst.append(item)

df_ezhar_hoghoghi = pd.concat(lst)
df_ezhar_hoghoghi.to_excel(
    r'E:\automating_reports_V2\saved_dir\arzeshafzoodeh_sonati\test\final\ezhar87-95\ezhar-87-95_new.xlsx')
# df_ezhar_hoghoghi = pd.read_excel(path_ezhar_hoghoghi)
