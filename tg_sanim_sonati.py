import pandas as pd
from helpers import connect_to_sql, get_tblreports_date
from constants import get_years, get_server_namesV2, get_sql_con, get_months
from sql_queries import get_sql_mashaghelsonati, get_sql_mashaghelsonati_ghatee,\
    get_sql_mashaghelsonati_ghateeEblaghNashode, get_sql_mashaghelsonati_tashkhisEblaghNashode, get_sql_mashaghelsonati_amadeersalbeheiatV2

months = get_months()
years_tuple = get_years()
years = years_tuple[3:8]
tashkhis_ghatee_types = ['تاریخ صدور برگه تشخیص', 'تاریخ ابلاغ تشخیص',
                         'تاریخ برگ قطعی صادر شده', 'تاریخ ابلاغ برگ قطعی']
lst_tbl = ['tblTashkhisSaderShode', 'tblTashkhisEblaghShode',
           'tblGhateeSaderShode', 'tblGhateeEblaghShode']
names = ['صادره', 'ابلاغی']
eblagh_electronic = ['خیر', 'بله']

lst = get_tblreports_date(lst_tbl)


def tashkhis_ghatee(name, tblname, report_type, selected_year, selected_month=months[5], eblagh_el=eblagh_electronic[0]):
    dfs = []
    for year in years:
        if report_type == tashkhis_ghatee_types[0]:
            sql_query = """
            SELECT [منبع مالیاتی],[نام اداره], [کد اداره],  count(*) as [تعداد تشخیص %s], SUBSTRING([%s], 1, 9) as [ماه صدور] 
            FROM [TestDb].[dbo].[%s%s]
            GROUP BY [منبع مالیاتی],[نام اداره], [کد اداره],SUBSTRING([%s], 1, 9)
        """ % (name, report_type, tblname, selected_year,  report_type)

        elif report_type == tashkhis_ghatee_types[1]:
            sql_query = """
            SELECT [منبع مالیاتی],[نام اداره], [کد اداره], [ابلاغ الکترونیک]  ,count(*) as [تعداد تشخیص %s] FROM [TestDb].[dbo].[%s%s]
            WHERE SUBSTRING([%s], 1, 9) = '%s / %s '
            GROUP BY [منبع مالیاتی],[نام اداره], [کد اداره], [ابلاغ الکترونیک]
        """ % (name, tblname, year[0], report_type, selected_year, selected_month)

        elif report_type == tashkhis_ghatee_types[2]:
            sql_query = """
            SELECT [منبع مالیاتی],[نام اداره], [کد اداره],  count(*) as [تعداد قطعی %s] FROM [TestDb].[dbo].[%s%s]
            WHERE SUBSTRING([%s], 1, 7) = '%s/%s'
            GROUP BY [منبع مالیاتی],[نام اداره], [کد اداره]
        """ % (name, tblname, year[0], report_type, selected_year, selected_month)

        elif report_type == tashkhis_ghatee_types[3]:
            sql_query = """
           SELECT [منبع مالیاتی],[نام اداره], [کد اداره], [ابلاغ الکترونیک]  ,count(*) as [تعداد قطعی %s] FROM [TestDb].[dbo].[%s%s]
            WHERE SUBSTRING([%s], 1, 7) = '%s/%s'
            GROUP BY [منبع مالیاتی],[نام اداره], [کد اداره], [ابلاغ الکترونیک]
        """ % (name, tblname, year[0], report_type, selected_year, selected_month)

        df = connect_to_sql(sql_query=sql_query, read_from_sql=True,
                            connect_type='',  return_df=True)

        dfs.append(df)

    final_df = pd.concat(dfs)

    if name == 'صادره':
        gf = final_df.groupby(
            ['نام اداره', 'منبع مالیاتی', 'کد اداره']).agg('sum')

    elif name == 'ابلاغی':
        gf = final_df.groupby(
            ['نام اداره', 'منبع مالیاتی', 'کد اداره', 'ابلاغ الکترونیک']).agg('sum')

        if not (gf.empty):
            gf = gf.reset_index(level=3)
            gf = gf.loc[gf['ابلاغ الکترونیک'] == 'خیر']
            gf.drop('ابلاغ الکترونیک', axis=1, inplace=True)

    return gf, sql_query


tashkhis_sadere_sanim, qq = tashkhis_ghatee(
    names[0], lst_tbl[0], years_tuple[9][0], tashkhis_ghatee_types[1])
tashkhis_eblaghi_sanim = tashkhis_ghatee(
    names[1], lst_tbl[1], tashkhis_ghatee_types[1], years_tuple[9][0], months[5])
ghatee_sadere_sanim = tashkhis_ghatee(
    names[0], lst_tbl[2], tashkhis_ghatee_types[2], years_tuple[9][0], months[5])
ghatee_eblaghi_sanim = tashkhis_ghatee(
    names[1], lst_tbl[3], tashkhis_ghatee_types[3], years_tuple[9][0], months[5])
