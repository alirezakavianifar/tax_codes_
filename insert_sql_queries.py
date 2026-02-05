import numpy as np
import pandas as pd
from automation.helpers import drop_into_db, connect_to_sql, get_update_date
from automation.sql_queries import get_sql_query_farayand_residegi_1401, get_sql_query_GhateeEblaghNashodeSanim, \
    get_sql_query_TashkhisEblaghNashdeSanim, get_sql_query_AmadeErsalBeHeiatSanim, \
    get_sql_query_EterazMotevaghefShode238Sanim, \
    get_sql_query_EjraEblaghShodeDarayeCaseBazVaMandeBedehiSanim, \
    get_sql_query_EjraEblaghNaShodeDarayeMandeBedehiEbraziSanim, \
    get_sql_query_EjraEblaghNaShodeDarayeMandeBedehiGhateeSanim, \
    get_sql_query_EjraEblaghNaShodeDarayeMandeBedehiSanim, \
    get_sql_query_AmadeGhateesaziSanim, get_sql_query_BadviEblaghNashodeSanim, \
    get_sql_query_TajdidnazarEblaghNashodeSanim

from automation.constants import get_sql_con

dict_sql_queries = {
    'tblFarayandResidegiSanim1401': get_sql_query_farayand_residegi_1401(),
    'tblGhateeEblaghNashodeSanim': get_sql_query_GhateeEblaghNashodeSanim(),
    'tblTashkhisEblaghNashdeSanim': get_sql_query_TashkhisEblaghNashdeSanim(),
    'tblAmadeErsalBeHeiatSanim': get_sql_query_AmadeErsalBeHeiatSanim(),
    'tblEterazMotevaghefShode238Sanim': get_sql_query_EterazMotevaghefShode238Sanim(),
    'tblEjraEblaghShodeDarayeCaseBazVaMandeBedehiSanim': get_sql_query_EjraEblaghShodeDarayeCaseBazVaMandeBedehiSanim(),
    'tblEjraEblaghNaShodeDarayeMandeBedehiEbraziSanim': get_sql_query_EjraEblaghNaShodeDarayeMandeBedehiEbraziSanim(),
    'tblEjraEblaghNaShodeDarayeMandeBedehiGhateeSanim': get_sql_query_EjraEblaghNaShodeDarayeMandeBedehiGhateeSanim(),
    'tblEjraEblaghNaShodeDarayeMandeBedehiSanim': get_sql_query_EjraEblaghNaShodeDarayeMandeBedehiSanim(),
    'tblAmadeGhateesaziSanim': get_sql_query_AmadeGhateesaziSanim(),
    # 'tblBadviEblaghNashodeSanim': get_sql_query_BadviEblaghNashodeSanim(),
    # 'tblTajdidnazarEblaghNashodeSanim': get_sql_query_TajdidnazarEblaghNashodeSanim(),
}


def insert_sql_queries(sql_query, table_name):

    df = connect_to_sql(
        sql_query, sql_con=get_sql_con(database='testdbV2'), read_from_sql=True, return_df=True)
    df = df.T.drop_duplicates().T
    df.fillna(0, inplace=True)
    # Identify numeric columns
    numeric_columns = df.apply(lambda x: pd.to_numeric(
        x, errors='coerce').notna().all())
    numeric_columns = df.columns[numeric_columns].tolist()
    try:
        df[numeric_columns] = df[numeric_columns].apply(
            np.float64).apply(np.int64)
    except:
        print(e)
    df['تاریخ بروزرسانی'] = get_update_date()
    drop_into_db(table_name=table_name,
                 columns=df.columns.tolist(),
                 values=df.values.tolist(),
                 append_to_prev=False,
                 db_name='testdbV2')


for table_name, sql_query in dict_sql_queries.items():
    try:
        insert_sql_queries(sql_query, table_name)
    except Exception as e:
        print(e)
