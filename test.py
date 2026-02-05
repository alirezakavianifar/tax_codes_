from helpersV2 import df_to_excelsheet
import os
from helpers import drop_into_db, get_update_date, georgian_to_persian, \
    return_start_end, list_files, open_and_save_excel_files
import time
import threading
from concurrent.futures import ThreadPoolExecutor, wait
from watchdog_186 import watch_over, is_downloaded
from helpers import unzip_files
import pandas as pd
from helpers import merge_multiple_excel_files, connect_to_sql

sql_query = "select top 1 * from tblbadvi_darjarian_dadrasi_hamarz"

df = connect_to_sql(sql_query, read_from_sql=True, return_df=True)

for index, col in enumerate(df.columns.tolist()[1:]):
    try:
        sql_query = "update tblbadvi_darjarian_dadrasi_hamarz set [%s]='' where [%s] = '0'" % (
            col, col)
        connect_to_sql(sql_query)
        print(index)
    except:
        continue


def func1(*args, **kwargs):
    print(args)
    print(kwargs)


func1(field=['field'])
print('done')


path = r'E:\automating_reports_V2\saved_dir\test\important'
dest = r'E:\automating_reports_V2\saved_dir\test\important'
dest_name = os.path.join(dest, 'final_grouped.xlsx')

# open_and_save_excel_files(path=path)

# df = merge_multiple_excel_files(path=path,
#                                 dest=dest,
#                                 delete_after_merge=False,
#                                 return_df=True)


# def is_ghatee(num):
#     if len(num) > 6:
#         return 'yes'
#     else:
#         return 'no'


# df['ghatee'] = df['شماره برگ قطعی'].apply(lambda x: is_ghatee(x))

# df.to_excel(r'E:\automating_reports_V2\saved_dir\test\important\final.xlsx')
# cols = ['سال', 'منبع', 'اداره', 'درصد قطعی شده', 'تعداد', 'تعداد قطعی شده']
# df_g = df.groupby(['سال عملکرد', 'منبع مالیاتی', 'نام اداره فعلی'])
# lst = []

# df_to_excelsheet(dest_name, df_g, index='', names=[
#                  'اداره ی امور مالیاتی', 'مالیات بر درآمد'])

# for key, item in df_g:
#     ls = []
#     count = item['ghatee'].count()
#     yes_count = item['ghatee'][item['ghatee'] == 'yes'].count()
#     ls.append(key[0])
#     ls.append(key[1])
#     ls.append(key[2])
#     ls.append(yes_count/count)
#     ls.append(count)
#     ls.append(yes_count)
#     lst.append(ls)

# new_df = pd.DataFrame(lst, columns=cols)


def tt():
    hello()

    def hello():
        print('hello')


tt()

df_r = pd.read_excel(
    r'E:\automating_reports_V2\saved_dir\test\important\final_aggs.xlsx')
p_df_1 = pd.pivot_table(df_r, values=['درصد قطعی شده', 'تعداد قطعی شده', 'تعداد'], aggfunc=sum,
                        index='اداره', columns=['سال', 'منبع'],
                        fill_value=0, margins=True, margins_name='جمع کلی')

p_df_1.to_excel(
    r'E:\automating_reports_V2\saved_dir\test\important\final_aggs_piv.xlsx')
print('f')


# unzip_files(r'H:\automatingReports\test\saved_dir\soratmoamelat')

print(os.cpu_count())

path = r'E:\automating_reports_V2\saved_dir\soratmoamelat'

list_files(path, '.zip')

r = return_start_end(0, 1000, 100)

print('f')


def tt(index='1'):
    file = os.path.join(path, index)
    print('start')
    time.sleep(2)
    with open(file, 'w') as f:
        f.write('Created a new docment!')


for i in range(4):
    t1 = threading.Thread(target=tt, args=(str(i), ))
    t2 = threading.Thread(target=watch_over, args=(path, 5))
    t1.start()
    t2.start()
    t1.join()
    t2.join()

print('f')


def decorator(log_type=None, num_runs=5):

    def func1(func):

        def inner_func(*args, **kwargs):

            def tryit(num_runs=num_runs):
                try:
                    result = func(*args, **kwargs)
                    return result
                except Exception as e:
                    # print(e)
                    num_runs -= 1
                    x = num_runs > 0
                    if x:
                        tryit(num_runs=num_runs)

            if log_type == 'logit':

                result = tryit()
            else:
                result = 'nolog'

            return result

        return inner_func

    return func1


i = 1


@decorator('logit', num_runs=8)
def hell(name):
    print(name)
    raise Exception


sql_query = 'SELECT * INTO V_USERS FROM [TestDb].[dbo].[V_USERS]'
drop_into_db('V_USERS',
             append_to_prev=False,
             db_name='testdbV2',
             create_tbl='no',
             sql_query=sql_query)


def func1(message, message2):
    print(message + '' + message2)
    time.sleep(3)
    print('%s ended \n' % message)


def main_func():
    print('all threads ended \n')


names = ['ali', 'hasan']
executor = ThreadPoolExecutor(len(names))

jobs = [executor.submit(func1, item, 'jasem') for item in names]
wait(jobs)
main_func()

# if __name__ == "main":
for i in range(3):

    job_thread = threading.Thread(target=func1, args=('func%s' % i))
    job_thread.start()
    job_thread.join()

main_func()


def test(message, *args, **kwargs):
    print(message)
    print(args[0])
    print(args[1])


test('hasan', 1, 10)


df = pd.read_excel(r'E:\temp\tt.xlsx')
df_gh = pd.read_excel(r'E:\temp\gg.xlsx')

df_gh = df_gh.groupby(df['منبع مالیاتی'])

for key, edare in df_gh:
    print('for %s it is %s' % (key,
                               (edare['مالیات قطعی'].sum() /
                                edare['مالیات ابرازی'].sum())))
