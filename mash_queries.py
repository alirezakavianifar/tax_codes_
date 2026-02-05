from helpers import connect_to_sql, get_sql_con
from sql_queries import get_sql_alltables, get_sql_if_column_in_table


def get_most(value1, value2):
    sql_query = get_sql_alltables('MASHAGHEL')

    df = connect_to_sql(sql_query=sql_query, sql_con=get_sql_con(
        database='MASHAGHEL', server='10.52.0.50\AHWAZ', username='mash', password='123456'),
        read_from_sql=True, return_df=True, return_df_as='df')

    for index, row in df.iterrows():

        table_name = '[MASHAGHEL].[dbo].[{}]'.format(row[0])
        table_name = "{}".format(row[0])
        if table_name == 'tashkhis_inf':
            print('5')
        column = "modi_seq"

        query = "UPDATE {} SET {} = {} WHERE {} = {}".format(
            table_name, column, value1, column, value2)

        table_name_new = r"'{}'".format(row[0])
        column_new = r"'modi_seq'"

        sql_query = get_sql_if_column_in_table(
            query=query, table=table_name_new, column=column_new)
        try:
            connect_to_sql(sql_query, sql_con=get_sql_con(
                database='MASHAGHEL', server='10.52.0.50\AHWAZ', username='mash', password='123456'), num_runs=0)
        except Exception as e:
            print(e)
            continue


get_most(value1=r"161755679718", value2='160521000055')
