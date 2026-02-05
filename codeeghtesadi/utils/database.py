import time
import pandas as pd
import pyodbc
import functools as ft
from codeeghtesadi.constants import get_sql_con
from codeeghtesadi.sql_queries import sql_delete, create_sql_table, insert_into
# Decorators
from codeeghtesadi.utils.decorators import time_it, measure_time
from codeeghtesadi.utils.common import get_update_date, is_int

n_retries = 0

@time_it(log=False)
def connect_to_sql(sql_query,
                   sql_con=None,  # Will default to get_sql_con() if None passed, but arg default was get_sql_con call
                   df_values=None,
                   read_from_sql=False,
                   connect_type=None,
                   return_df=False,
                   chunk_size=None,
                   return_df_as='dataframe',
                   num_runs=12,
                   *args,
                   **kwargs):
    
    if sql_con is None:
        sql_con = get_sql_con()

    global n_retries 

    def retry():
        cnxn = pyodbc.connect(sql_con)
        cursor = cnxn.cursor()

        if read_from_sql:
            df = pd.read_sql(sql_query, cnxn, chunksize=chunk_size)
            return df

        if df_values is None:
            cursor.execute(sql_query)
            cursor.execute('commit')
        else:
            cursor.executemany(sql_query, df_values)
            cursor.execute('commit')

        cnxn.close()

    try:
        if return_df:
            x = retry()

            if return_df_as == 'json':
                x = x.to_json(orient='records', force_ascii=False)[
                    1:-1].replace('},{', '} {')
            elif return_df_as == 'dict':
                x = x.to_dict()

            return x

        else:
            retry()

    except Exception as e:
        if n_retries < num_runs:
            n_retries += 1
            print(e)
            print('trying again')
            time.sleep(4)
            retry()

@measure_time
def drop_into_db(table_name=None,
                 columns=None,
                 values=None,
                 append_to_prev=False,
                 db_name='TestDb',
                 del_tbl=None,
                 create_tbl=None,
                 sql_query=None,
                 drop=True):
    if not append_to_prev:
        def del_tbl_func():
            delete_table = sql_delete(table_name)
            connect_to_sql(sql_query=delete_table,
                           sql_con=get_sql_con(database=db_name),
                           connect_type='dropping sql table')

        if del_tbl is None:
            del_tbl_func()

        def create_tbl_func():
            sql_create_table = create_sql_table(table_name, columns)
            connect_to_sql(sql_create_table,
                           sql_con=get_sql_con(database=db_name),
                           connect_type='creating sql table')

        if (create_tbl is None) or (create_tbl == 'yes'):
            create_tbl_func()

    if drop:
        if sql_query is None:
            sql_query = insert_into(table_name, columns)

        connect_to_sql(sql_query,
                       sql_con=get_sql_con(database=db_name),
                       df_values=values,
                       connect_type='inserting into sql table')

def insert_into_tbl(sql_query, tbl_name, convert_to_str=False):

    df = connect_to_sql(sql_query,
                        read_from_sql=True,
                        return_df=True,
                        connect_type='Read from table')

    if convert_to_str:
        df = df.astype('str')

    cols = df.columns.tolist()
    if 'ID' in cols:
        df.drop(['ID'], axis=1, inplace=True)

    cols = df.columns.tolist()
    for item in cols:
        df[item] = df[item].apply(lambda x: is_int(x))

    df['تاریخ بروزرسانی'] = get_update_date()

    if convert_to_str:
        df = df.astype('str')

    drop_into_db(table_name=tbl_name,
                 columns=df.columns.tolist(),
                 values=df.values.tolist(),
                 db_name='testdbV2')

def get_edare_shahr():
    sql_query_edare_shahr = "SELECT [city],[edare],[IrisCode] FROM [tax].[dbo].[tblEdareShahr]"
    df_edare_shahr = connect_to_sql(sql_query_edare_shahr,
                                    sql_con=get_sql_con(database='tax'),
                                    read_from_sql=True,
                                    connect_type='read from tblEdateShahr',
                                    return_df=True)
    return df_edare_shahr

def find_edares(df=None, colname_tomerge='واحد مالیاتی', persian_name=None, table_name='None', report_typ=None):
    df_edare_shahr = get_edare_shahr()

    if df is None:
        sql_query_most = "SELECT * FROM %s" % table_name
        df = connect_to_sql(sql_query_most,
                            sql_con=get_sql_con(database='testdbV2'),
                            read_from_sql=True,
                            connect_type='read from tblMost',
                            return_df=True)
        df = df.drop(columns=['ID'], axis=1)

    df_merged_1 = df.merge(df_edare_shahr,
                           how='inner',
                           left_on=df[colname_tomerge].astype(
                               'str').str.slice(0, 4),
                           right_on='edare')

    df_merged_2 = df.merge(df_edare_shahr,
                           how='inner',
                           left_on=df[colname_tomerge].astype(
                               'str').str.slice(0, 5),
                           right_on=df_edare_shahr['edare'].str.slice(
                               0, 5)).drop(['key_0'], axis=1)

    df_f = pd.concat([df_merged_1, df_merged_2])
    if report_typ is not None:
        df_f.drop_duplicates(subset=[report_typ], keep=False, inplace=True)

    dff_final = pd.concat([df_f, df_merged_2])

    dff_final.rename(columns={
        'edare': 'کد اداره',
        'city': 'شهرستان'
    },
        inplace=True)

    return dff_final

def process_mostaghelat(date=140106,
                        report_type='Tashkhis',
                        persian_name='تشخیص',
                        sodor='صدور',
                        msg='تشخیص ابلاغ نشده',
                        table_name=None,
                        report_typ='شماره برگ تشخیص',
                        drop_to_sql=True):

    dff_final = find_edares(table_name=table_name, report_typ=report_typ)

    dff_final['ماه %s' % sodor] = dff_final['تاریخ %s' % sodor].str.replace(
        '/', '').str.slice(0, 6).astype('int64')
    if date is not None:
        dff_final = dff_final.loc[dff_final['ماه %s' % sodor] < 140106]
    dff_final.rename(columns={
        'edare': 'کد اداره',
        'city': 'شهرستان'
    },
        inplace=True)
    dff_final_agg = dff_final.groupby(
        ['کد اداره', 'شهرستان', 'تاریخ بروزرسانی']).size().reset_index()
    dff_final_agg.rename(
        columns={0: 'تعداد %s مستغلات' % msg}, inplace=True)
    
    if drop_to_sql:
        drop_into_db(table_name,
                     dff_final.columns.tolist(),
                     dff_final.values.tolist(),
                     append_to_prev=False,
                     db_name='testdbV2')
    return dff_final, dff_final_agg

def final_most(date=140106):
    tashkhis, tashkhis_agg = process_mostaghelat(date=date,
                                                 table_name='tblTashkhisMost', report_typ='شماره برگ تشخیص')
    ghatee, ghatee_agg = process_mostaghelat(date=date,
                                             report_type='Ghatee',
                                             persian_name='قطعی',
                                             msg='قطعی ابلاغ نشده',
                                             report_typ='شماره برگ قطعی',
                                             table_name='tblGhateeMost')
    amade_ghatee, amade_ghatee_agg = process_mostaghelat(
        date=None,
        report_type='AmadeGhatee',
        persian_name='تشخیص',
        sodor='ابلاغ',
        msg='آماده قطعی',
        report_typ='شماره برگ تشخیص',
        table_name='tblAmadeGhateeMost')

    lst_agg = [tashkhis_agg, ghatee_agg, amade_ghatee_agg]

    merged_agg = ft.df_to_excelsheete(
        lambda left, right: pd.merge(
            left, right, how='outer', on='کد اداره'),
        lst_agg)

    merged_agg['شهرستان'].fillna(
        merged_agg['شهرستان_y'], inplace=True)
    merged_agg['شهرستان'].fillna(
        merged_agg['شهرستان_x'], inplace=True)
    merged_agg['تاریخ بروزرسانی'].fillna(merged_agg['تاریخ بروزرسانی_x'],
                                         inplace=True)
    merged_agg['تاریخ بروزرسانی'].fillna(merged_agg['تاریخ بروزرسانی_y'],
                                         inplace=True)
    merged_agg['شهرستان'].fillna(
        merged_agg['شهرستان_x'], inplace=True)
    merged_agg.fillna(0, inplace=True)

    selected_columns = [
        'کد اداره', 'تعداد تشخیص ابلاغ نشده مستغلات',
        'تعداد قطعی ابلاغ نشده مستغلات', 'شهرستان', 'تاریخ بروزرسانی',
        'تعداد آماده قطعی مستغلات'
    ]

    merged_agg = merged_agg[selected_columns]
    merged_agg = merged_agg.rename(
        columns={'کد اداره': 'نام اداره سنتی'})
    merged_agg['نام اداره سنتی'] = merged_agg['نام اداره سنتی'].str.slice(
        0, 5)
    merged_agg = merged_agg.iloc[:, [0, 3, 1, 2, 5, 4]]

    return tashkhis, ghatee, amade_ghatee, merged_agg


def get_sqltable_colnames(db_name, tbl_name):
    import numpy as np
    # Construct SQL query to retrieve column names
    sql_query = f"""SELECT COLUMN_NAME 
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE 
                TABLE_CATALOG = '{db_name}'
            AND TABLE_NAME = '{tbl_name}'
            """

    # Connect to SQL and execute the query
    df_cols = connect_to_sql(
        sql_query, sql_con=get_sql_con(database=db_name), read_from_sql=True, return_df=True)

    # Reshape the result to a flat list of column names
    df_cols = list(np.reshape(df_cols.to_numpy(), (-1, 1)).flatten())

    # Return the list of column names
    return df_cols

