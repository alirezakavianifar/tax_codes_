import time
import jdatetime
import pandas as pd


def get_update_date():
    x = jdatetime.date.today()
    if len(str(x.month)) == 1:
        month = '0%s' % str(x.month)

    else:
        month = str(x.month)
    if len(str(x.day)) == 1:
        day = '0%s' % str(x.day)

    else:
        day = str(x.day)

    year = str(x.year)

    update_date = year + month + day

    return update_date


def replace_last(phrase, strToReplace=',', replacementStr=''):

    # Search for the last occurrence of substring in string
    pos = phrase.rfind(strToReplace)
    if pos > -1:
        # Replace last occurrences of substring 'is' in string with 'XX'
        phrase = phrase[:pos] + replacementStr + \
            phrase[pos + len(strToReplace):]

    return phrase


def get_table_names(report_type, year):

    tables = []
    if (report_type == 'ezhar'):
        tables.append('tblEzharSanim%s' % year)

    elif (report_type == 'tashkhis_sader_shode'):
        tables.append('tblTashkhisSaderShode%s' % year)

    elif (report_type == 'tashkhis_eblagh_shode'):
        tables.append('tblTashkhisEblaghShode%s' % year)

    elif (report_type == 'ghatee_sader_shode'):
        tables.append('tblGhateeSaderShode%s' % year)

    elif (report_type == 'ghatee_eblagh_shode'):
        tables.append('tblGhateeEblaghShode%s' % year)

    elif (report_type == '1000_parvande'):
        tables.append('tbl1000Parvande%s' % year)

    elif (report_type == 'badvi_darjarian_dadrasi'):
        tables.append('tblbadvidarjariandadrasi%s' % year)

    elif (report_type == 'badvi_takmil_shode'):
        tables.append('tblbadvitakmilshode%s' % year)

    elif (report_type == 'tajdidnazer_darjarian_dadrasi'):
        tables.append('tbltajdidnazerdarjariandadrasi%s' % year)

    elif (report_type == 'badvi_darjarian_dadrasi_hamarz'):
        tables.append('tblbadvi_darjarian_dadrasi_hamarz')

    elif (report_type == 'amar_sodor_gharar_karshenasi'):
        tables.append('tblamar_sodor_gharar_karshenasi')

    elif (report_type == 'amar_sodor_ray'):
        tables.append('tblamar_sodor_ray')

    elif (report_type == 'imp_parvand'):
        tables.append('tblimp_parvand')

    elif (report_type == 'tajdidnazar_takmil_shode'):
        tables.append('tbltajdidnazartakmilshode%s' % year)

    return tables


def sql_delete(table):

    return """
                    BEGIN TRANSACTION
                            BEGIN TRY 
                        
                                IF Object_ID('%s') IS NOT NULL DROP TABLE %s
                        
                                COMMIT TRANSACTION;
                                
                            END TRY
                            BEGIN CATCH
                                ROLLBACK TRANSACTION;
                            END CATCH
                    """ % (table, table)


def create_sql_table(table, columns):

    temp = ''

    for c in columns:
        temp += '[%s] NVARCHAR(MAX) NULL,\n' % c

    sql_query = """
    BEGIN TRANSACTION
    BEGIN TRY 

        IF Object_ID('%s') IS NULL
        
        CREATE TABLE %s
        (
         [ID] [int] IDENTITY(1,1) NOT NULL,
         PRIMARY KEY (ID),
         %s
                                  
       
         )

        COMMIT TRANSACTION;
        
    END TRY
    BEGIN CATCH
        ROLLBACK TRANSACTION;
    END CATCH
    """ % (table, table, temp)

    # time.sleep(400)
    return sql_query


def insert_into(table, columns=None):

    temp = ''
    values = ''

    for c in columns:
        temp += '[%s],' % c

        values += '?,'

    values = replace_last(values)
    temp = replace_last(temp)
    # temp = temp.replace('[تاریخ بروزرسانی],', '[تاریخ بروزرسانی]', 1)

    sql_insert = """
    BEGIN TRANSACTION
    BEGIN TRY 

        INSERT INTO %s
        (
        %s         
            )
        
        VALUES
        (%s)

        COMMIT TRANSACTION;
        
    END TRY
    BEGIN CATCH
        ROLLBACK TRANSACTION;
    END CATCH
    """ % (table, temp, values)

    return sql_insert


def insert_into_tblAnbareKoliHist(year):

    sql_query = """
    
        
    
INSERT INTO tblAnbareKoliHist%s
SELECT 
%s AS [تاریخ بروزرسانی]
,Base.*
,hesabrasi.[سال عملکرد]
,hesabrasi.[tashkhisSadere]
,hesabrasi.[eblaghTashkhis]
,hesabrasi.[ghateeSadere]
,hesabrasi.[eblaghGhatee]

 FROM 

(SELECT 
[کد اداره]
,[نام اداره]
,ISNULL([مالیات بر درآمد شرکت ها],0)AS[مالیات بر درآمد شرکت ها]
,ISNULL([مالیات بر درآمد مشاغل],0)AS[مالیات بر درآمد مشاغل]
,ISNULL([مالیات بر ارزش افزوده],0)AS[مالیات بر ارزش افزوده]

 FROM
(SELECT 
[کد اداره]
,[نام اداره]
,[منبع مالیاتی]
,count([منبع مالیاتی]) AS [tedad]
 FROM

(SELECT main.*
,[tblTashkhisSaderShode%s].[تاریخ صدور برگه تشخیص]
,[dbo].[tblTashkhisEblaghShode%s].[تاریخ ابلاغ تشخیص] as [تاریخ ابلاغ تشخیص]
,[dbo].[tblGhateeSaderShode%s].[تاریخ برگ قطعی صادر شده] as [تاریخ برگ قطعی]
,[dbo].[tblGhateeEblaghShode%s].[تاریخ ابلاغ برگ قطعی]
,1 as tedad
FROM
(SELECT * FROM [dbo].[tblGhateeSazi%s]
WHERE 
[سال عملکرد]=%s
and [منبع مالیاتی] IN(N'مالیات بر درآمد شرکت ها',N'مالیات بر درآمد مشاغل')
and [نوع ریسک اظهارنامه] IN (
N'اظهارنامه برآوردی صفر'
,
N'انتخاب شده بدون اعمال معیار ریسک'
,
N'رتبه ریسک بالا'
,
N'مودیان مهم با ریسک بالا'
)
UNION
SELECT * FROM [dbo].[tblGhateeSazi%s]
WHERE [منبع مالیاتی]=N'مالیات بر ارزش افزوده' and [سال عملکرد]=%s
and [شناسه ملی / کد ملی (TIN)] IN
(SELECT [شناسه ملی / کد ملی (TIN)] FROM [dbo].[tblGhateeSazi%s] WHERE [سال عملکرد]=%s and [منبع مالیاتی] IN(N'مالیات بر درآمد شرکت ها',N'مالیات بر درآمد مشاغل')and [نوع ریسک اظهارنامه] IN (N'اظهارنامه برآوردی صفر',N'انتخاب شده بدون اعمال معیار ریسک',N'رتبه ریسک بالا',N'مودیان مهم با ریسک بالا'))
) as main
LEFT JOIN
[dbo].[tblTashkhisSaderShode%s]
ON 
[dbo].[tblTashkhisSaderShode%s].[شناسه اظهارنامه]=main.[شناسه اظهارنامه] and [tblTashkhisSaderShode%s].[سال عملکرد]=%s
LEFT JOIN
[dbo].[tblTashkhisEblaghShode%s]
ON
[dbo].[tblTashkhisEblaghShode%s].[شناسه اظهارنامه]=main.[شناسه اظهارنامه] and [tblTashkhisEblaghShode%s].[سال عملکرد]=%s
LEFT JOIN
[dbo].[tblGhateeSaderShode%s]
ON 
[dbo].[tblGhateeSaderShode%s].[شناسه اظهارنامه]=main.[شناسه اظهارنامه]and [tblGhateeSaderShode%s].[سال عملکرد]=%s
LEFT JOIN
[dbo].[tblGhateeEblaghShode%s]
ON
[dbo].[tblGhateeEblaghShode%s].[شناسه اظهارنامه]=main.[شناسه اظهارنامه]and [tblGhateeEblaghShode%s].[سال عملکرد]=%s
)as a
group by 

[کد اداره]
,[نام اداره]
,[منبع مالیاتی]

)
 src
pivot
(
  sum(tedad)
  for [منبع مالیاتی] in ([مالیات بر درآمد شرکت ها],[مالیات بر درآمد مشاغل],[مالیات بر ارزش افزوده])
) piv) AS base

LEFT JOIN


(SELECT 
[کد اداره]
,[نام اداره]
,[سال عملکرد]
,count([تاریخ صدور برگه تشخیص]) AS  [tashkhisSadere]
,count([تاریخ ابلاغ تشخیص]) AS  [eblaghTashkhis]
,count([تاریخ برگ قطعی]) AS [ghateeSadere]
,count([تاریخ ابلاغ برگ قطعی]) AS [eblaghGhatee]
 FROM

(SELECT main.*
,[tblTashkhisSaderShode%s].[تاریخ صدور برگه تشخیص]
,[dbo].[tblTashkhisEblaghShode%s].[تاریخ ابلاغ تشخیص] as [تاریخ ابلاغ تشخیص]
,[dbo].[tblGhateeSaderShode%s].[تاریخ برگ قطعی صادر شده] as [تاریخ برگ قطعی]
,[dbo].[tblGhateeEblaghShode%s].[تاریخ ابلاغ برگ قطعی]
,1 as tedad
FROM
(SELECT * FROM [dbo].[tblGhateeSazi%s]
WHERE 
[سال عملکرد]=%s
and [منبع مالیاتی] IN(N'مالیات بر درآمد شرکت ها',N'مالیات بر درآمد مشاغل')
and [نوع ریسک اظهارنامه] IN (
N'اظهارنامه برآوردی صفر'
,
N'انتخاب شده بدون اعمال معیار ریسک'
,
N'رتبه ریسک بالا'
,
N'مودیان مهم با ریسک بالا'
)
UNION
SELECT * FROM [dbo].[tblGhateeSazi%s]
WHERE [منبع مالیاتی]=N'مالیات بر ارزش افزوده' and [سال عملکرد]=%s
and [شناسه ملی / کد ملی (TIN)] IN
(SELECT [شناسه ملی / کد ملی (TIN)] FROM [dbo].[tblGhateeSazi%s] WHERE [سال عملکرد]=%s and [منبع مالیاتی] IN(N'مالیات بر درآمد شرکت ها',N'مالیات بر درآمد مشاغل')and [نوع ریسک اظهارنامه] IN (N'اظهارنامه برآوردی صفر',N'انتخاب شده بدون اعمال معیار ریسک',N'رتبه ریسک بالا',N'مودیان مهم با ریسک بالا'))
) as main
LEFT JOIN
[dbo].[tblTashkhisSaderShode%s]
ON 
[dbo].[tblTashkhisSaderShode%s].[شناسه اظهارنامه]=main.[شناسه اظهارنامه] and [tblTashkhisSaderShode%s].[سال عملکرد]=%s
LEFT JOIN
[dbo].[tblTashkhisEblaghShode%s]
ON
[dbo].[tblTashkhisEblaghShode%s].[شناسه اظهارنامه]=main.[شناسه اظهارنامه] and [tblTashkhisEblaghShode%s].[سال عملکرد]=%s
LEFT JOIN
[dbo].[tblGhateeSaderShode%s]
ON 
[dbo].[tblGhateeSaderShode%s].[شناسه اظهارنامه]=main.[شناسه اظهارنامه]and [tblGhateeSaderShode%s].[سال عملکرد]=%s
LEFT JOIN
[dbo].[tblGhateeEblaghShode%s]
ON
[dbo].[tblGhateeEblaghShode%s].[شناسه اظهارنامه]=main.[شناسه اظهارنامه]and [tblGhateeEblaghShode%s].[سال عملکرد]=%s
)as a
group by 

[کد اداره]
,[نام اداره]
,[سال عملکرد]
) AS hesabrasi
ON base.[کد اداره]=Hesabrasi.[کد اداره]
    """ % (year, get_update_date(), year, year, year, year, year, year, year, year, year, year, year, year, year, year, year, year, year, year, year, year, year, year, year, year, year, year, year, year, year, year, year, year, year, year, year, year, year, year, year, year, year, year, year, year, year, year, year, year, year, year, year, year)
    return sql_query


def insert_into_tblAnbare99Mashaghel(year):
    sql_query = """
    INSERT INTO AnbareMashaghel%s
SELECT 
%s as [تاریخ بروزرسانی]
,ezhar.*
, [تعداد تشخیص-اظهارنامه برآوردی صفر]
, [تعداد تشخیص-انتخاب شده بدون اعمال معیار ریسک]
, [تعداد تشخیص-رتبه ریسک بالا]
, [تعداد تشخیص-مودیان مهم با ریسک بالا]
FROM
(SELECT 
[کد اداره]
,[نام اداره]
,[سال عملکرد]
,ISNULL([اظهارنامه برآوردی صفر],0) AS [اظهارنامه برآوردی صفر]
,ISNULL([انتخاب شده بدون اعمال معیار ریسک],0) AS [انتخاب شده بدون اعمال معیار ریسک]
,ISNULL([رتبه ریسک بالا],0) AS [رتبه ریسک بالا]
,ISNULL([مودیان مهم با ریسک بالا],0) AS [مودیان مهم با ریسک بالا]
 FROM
(SELECT [کد اداره]
,[نام اداره]
,[سال عملکرد]
,[نوع ریسک اظهارنامه] 
,count(*)as tedad
FROM [dbo].[tblGhateeSazi%s]
WHERE 
[سال عملکرد]=%s
and [منبع مالیاتی]= N'مالیات بر درآمد مشاغل'
and [نوع ریسک اظهارنامه] IN (
N'اظهارنامه برآوردی صفر'
,
N'انتخاب شده بدون اعمال معیار ریسک'
,
N'رتبه ریسک بالا'
,
N'مودیان مهم با ریسک بالا'
)
GROUP BY [کد اداره]
,[نام اداره]
,[سال عملکرد]
,[نوع ریسک اظهارنامه] )
 src
pivot
(
  sum(tedad)
  for [نوع ریسک اظهارنامه] in ([اظهارنامه برآوردی صفر],[انتخاب شده بدون اعمال معیار ریسک],[رتبه ریسک بالا],[مودیان مهم با ریسک بالا])
)piv1)ezhar


LEFT JOIN



 (SELECT 
 [کد اداره]
,[نام اداره]
,ISNULL([اظهارنامه برآوردی صفر],0) AS [تعداد تشخیص-اظهارنامه برآوردی صفر]
,ISNULL([انتخاب شده بدون اعمال معیار ریسک],0) AS [تعداد تشخیص-انتخاب شده بدون اعمال معیار ریسک]
,ISNULL([رتبه ریسک بالا],0) AS [تعداد تشخیص-رتبه ریسک بالا]
,ISNULL([مودیان مهم با ریسک بالا],0) AS [تعداد تشخیص-مودیان مهم با ریسک بالا]
 FROM
 (SELECT 
ezhar.[کد اداره]
,ezhar.[نام اداره]
,ezhar.[نوع ریسک اظهارنامه]
,count([تاریخ صدور برگه تشخیص]) as [tedad]
FROM
(SELECT [کد اداره]
,[سال عملکرد]
,[نام اداره]
,[نوع ریسک اظهارنامه] 
,[شناسه اظهارنامه]
FROM [dbo].[tblGhateeSazi%s] 
WHERE 
[سال عملکرد]=%s
and [منبع مالیاتی]= N'مالیات بر درآمد شرکت ها'
and [نوع ریسک اظهارنامه] IN (
N'اظهارنامه برآوردی صفر'
,
N'انتخاب شده بدون اعمال معیار ریسک'
,
N'رتبه ریسک بالا'
,
N'مودیان مهم با ریسک بالا'
))as ezhar LEFT JOIN
[dbo].[tblTashkhisSaderShode%s] ON ezhar.[شناسه اظهارنامه]=[tblTashkhisSaderShode%s].[شناسه اظهارنامه] and [tblTashkhisSaderShode%s].[سال عملکرد]=%s
group by ezhar.[کد اداره]
,ezhar.[نام اداره]
,ezhar.[نوع ریسک اظهارنامه]
)
 src
pivot
(
  sum(tedad)
  for [نوع ریسک اظهارنامه] in ([اظهارنامه برآوردی صفر],[انتخاب شده بدون اعمال معیار ریسک],[رتبه ریسک بالا],[مودیان مهم با ریسک بالا])
) piv2)tashkhis
ON ezhar.[کد اداره]=tashkhis.[کد اداره]
    """ % (year, get_update_date(), year, year, year, year, year, year, year, year)

    return sql_query


def insert_into_tblAnbare99Sherkatha(year):
    sql_query = """
    
INSERT INTO AnbareSherkatha%s
SELECT 
%s as [تاریخ بروزرسانی]
,ezhar.*
, [تعداد تشخیص-اظهارنامه برآوردی صفر]
, [تعداد تشخیص-انتخاب شده بدون اعمال معیار ریسک]
, [تعداد تشخیص-رتبه ریسک بالا]
, [تعداد تشخیص-مودیان مهم با ریسک بالا]
FROM
(SELECT 
[کد اداره]
,[نام اداره]
,[سال عملکرد]
,ISNULL([اظهارنامه برآوردی صفر],0) AS [اظهارنامه برآوردی صفر]
,ISNULL([انتخاب شده بدون اعمال معیار ریسک],0) AS [انتخاب شده بدون اعمال معیار ریسک]
,ISNULL([رتبه ریسک بالا],0) AS [رتبه ریسک بالا]
,ISNULL([مودیان مهم با ریسک بالا],0) AS [مودیان مهم با ریسک بالا]
 FROM
(SELECT [کد اداره]
,[نام اداره]
,[سال عملکرد]
,[نوع ریسک اظهارنامه] 
,count(*)as tedad
FROM [dbo].[tblGhateeSazi%s]
WHERE 
[سال عملکرد]=%s
and [منبع مالیاتی]= N'مالیات بر درآمد شرکت ها'
and [نوع ریسک اظهارنامه] IN (
N'اظهارنامه برآوردی صفر'
,
N'انتخاب شده بدون اعمال معیار ریسک'
,
N'رتبه ریسک بالا'
,
N'مودیان مهم با ریسک بالا'
)
GROUP BY [کد اداره]
,[نام اداره]
,[سال عملکرد]
,[نوع ریسک اظهارنامه] )
 src
pivot
(
  sum(tedad)
  for [نوع ریسک اظهارنامه] in ([اظهارنامه برآوردی صفر],[انتخاب شده بدون اعمال معیار ریسک],[رتبه ریسک بالا],[مودیان مهم با ریسک بالا])
)piv1)ezhar


LEFT JOIN



 (SELECT 
 [کد اداره]
,[نام اداره]
,ISNULL([اظهارنامه برآوردی صفر],0) AS [تعداد تشخیص-اظهارنامه برآوردی صفر]
,ISNULL([انتخاب شده بدون اعمال معیار ریسک],0) AS [تعداد تشخیص-انتخاب شده بدون اعمال معیار ریسک]
,ISNULL([رتبه ریسک بالا],0) AS [تعداد تشخیص-رتبه ریسک بالا]
,ISNULL([مودیان مهم با ریسک بالا],0) AS [تعداد تشخیص-مودیان مهم با ریسک بالا]
 FROM
 (SELECT 
ezhar.[کد اداره]
,ezhar.[نام اداره]
,ezhar.[نوع ریسک اظهارنامه]
,count([تاریخ صدور برگه تشخیص]) as [tedad]
FROM
(SELECT [کد اداره]
,[نام اداره]
,[نوع ریسک اظهارنامه] 
,[شناسه اظهارنامه]
FROM [dbo].[tblGhateeSazi%s] 
WHERE 
[سال عملکرد]=%s
and [منبع مالیاتی]= N'مالیات بر درآمد شرکت ها'
and [نوع ریسک اظهارنامه] IN (
N'اظهارنامه برآوردی صفر'
,
N'انتخاب شده بدون اعمال معیار ریسک'
,
N'رتبه ریسک بالا'
,
N'مودیان مهم با ریسک بالا'
))as ezhar LEFT JOIN
[dbo].[tblTashkhisSaderShode%s] ON ezhar.[شناسه اظهارنامه]=[tblTashkhisSaderShode%s].[شناسه اظهارنامه] and [tblTashkhisSaderShode%s].[سال عملکرد]=%s
group by ezhar.[کد اداره]
,ezhar.[نام اداره]
,ezhar.[نوع ریسک اظهارنامه]
)
 src
pivot
(
  sum(tedad)
  for [نوع ریسک اظهارنامه] in ([اظهارنامه برآوردی صفر],[انتخاب شده بدون اعمال معیار ریسک],[رتبه ریسک بالا],[مودیان مهم با ریسک بالا])
) piv2)tashkhis
ON ezhar.[کد اداره]=tashkhis.[کد اداره]
    """ % (year, get_update_date(), year, year, year, year, year, year, year, year)

    return sql_query


def insert_into_tblHesabrasiArzeshAfzoode(year):
    sql_query = """
    
INSERT INTO tblHesabrasiArzeshAfzode%s
SELECT
      %s AS  [تاریخ بروزرسانی]
      ,[کد اداره]
      ,[نام اداره]
      ,[سال عملکرد]
	  ,COUNT([شناسه اظهارنامه]) AS [تعداد اظهارنامه]
	  ,count([تاریخ صدور برگه تشخیص]) AS  [tashkhisSadere]
      ,count([تاریخ ابلاغ تشخیص]) AS  [eblaghTashkhis]
      ,count([تاریخ برگ قطعی]) AS [ghateeSadere]
      ,count([تاریخ ابلاغ برگ قطعی]) AS [eblaghGhatee]

 FROM

(SELECT main.*
,[tblTashkhisSaderShode%s].[تاریخ صدور برگه تشخیص]
,[dbo].[tblTashkhisEblaghShode%s].[تاریخ ابلاغ تشخیص] as [تاریخ ابلاغ تشخیص]
,[dbo].[tblGhateeSaderShode%s].[تاریخ برگ قطعی صادر شده] as [تاریخ برگ قطعی]
,[dbo].[tblGhateeEblaghShode%s].[تاریخ ابلاغ برگ قطعی]
 FROM
(SELECT [کد اداره]
      ,[نام اداره]
      ,[سال عملکرد]
      ,[شناسه اظهارنامه]

  FROM [TestDb].[dbo].[tblGhateeSazi%s]
  where  [منبع مالیاتی]=N'مالیات بر ارزش افزوده'
  ) AS main
  LEFT JOIN
[dbo].[tblTashkhisSaderShode%s]
ON 
[dbo].[tblTashkhisSaderShode%s].[شناسه اظهارنامه]=main.[شناسه اظهارنامه] and [tblTashkhisSaderShode%s].[سال عملکرد]=main.[سال عملکرد]
LEFT JOIN
[dbo].[tblTashkhisEblaghShode%s]
ON
[dbo].[tblTashkhisEblaghShode%s].[شناسه اظهارنامه]=main.[شناسه اظهارنامه] and [tblTashkhisEblaghShode%s].[سال عملکرد]=main.[سال عملکرد]
LEFT JOIN
[dbo].[tblGhateeSaderShode%s]
ON 
[dbo].[tblGhateeSaderShode%s].[شناسه اظهارنامه]=main.[شناسه اظهارنامه] and [tblGhateeSaderShode%s].[سال عملکرد]=main.[سال عملکرد]
LEFT JOIN
[dbo].[tblGhateeEblaghShode%s]
ON
[dbo].[tblGhateeEblaghShode%s].[شناسه اظهارنامه]=main.[شناسه اظهارنامه] and [tblGhateeEblaghShode%s].[سال عملکرد]=main.[سال عملکرد]
)as a
 GROUP BY 
[کد اداره]
      ,[نام اداره]
      ,[سال عملکرد]
    """ % (year, get_update_date(), year, year, year, year, year, year, year, year, year, year, year, year, year, year, year, year, year)

    return sql_query


def linked_server():

    return """

if not exists(select * from sys.servers where name = N'10.52.0.114')
BEGIN
EXEC sp_addlinkedserver @server='10.52.0.114'

EXEC sp_addlinkedsrvlogin '10.52.0.114', 'false', NULL, 'sa', '14579Ali'
END

"""


def get_badvi_tables():

    return """

SELECT table_name FROM [10.52.0.114].testdb.INFORMATION_SCHEMA.TABLES
where table_name like 'tblbadvi%'

"""


def get_sql_mashaghelsonati():

    return """
select 
case
when  ghabln_inf.cod_hozeh  =168141 then 'هويزه'
 when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1601 then 'اهواز کد يک'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1602 then 'اهواز کد دو'
 when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,5) =16439 then 'هنديجان'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,5) =16649 then 'لالي'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,5) =16718 then 'رامشير'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1603 then 'اهواز کد سه'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1604 then 'اهواز کد چهار '
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1605 then 'اهواز کد پنج'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1606 then 'اهواز کد شش'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1607 then 'اهواز کد هفت'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1608 then 'اهواز کد هشت'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1609 then 'اهواز کد نه'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1610 then 'اهواز کد 10'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1611 then 'اهواز کد 11'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1615 then 'اهواز کد 15'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1631 then 'آبادان کد 31'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1632 then 'آبادان کد 32'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1633 then 'آبادان کد 33'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1626 then 'دزفول'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1627 then 'دزفول'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1628 then 'دزفول'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1643 then 'ماهشهر کد43'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1644 then 'ماهشهر کد44'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1636 then 'بهبهان'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1639 then 'شوشتر'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1641 then 'گتوند'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1648 then 'بندر امام'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1649 then 'بندر امام'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1651 then 'انديمشک'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1654 then 'خرمشهر'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1659 then 'شادگان'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1661 then 'اميديه'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1663 then 'آغاجاري'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1664 then 'مسجد سليمان'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1668 then 'شوش'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1671 then 'رامهرمز'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1676 then 'ايذه'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1678 then 'هفتگل'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1679 then 'باغملک'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1681 then 'دشت آزاداگان'
else 'نامشخص' end as 'شهرستان'
,
case 
 when ghabln_inf.cod_hozeh  IN (168141) then '168141'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,5) IN ('16439','16649','16718') then SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,5) else SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) end as 'کد اداره'
,
namee as 'نام',
family as 'نام خانوادگي',
ghabln_inf.Sal as 'عملکرد',
ghabln_inf.cod_hozeh as 'کد حوزه',
ghabln_inf.k_parvand as 'کلاسه پرونده',
tashkhis_inf.motamam as 'متمم',
case when KMLink_Inf.have_ezhar=1 then 'داراي اظهارنامه' else 'فاقد اظهارنامه' end as 'وضعيت ارائه اظهارنامه',
Shohr_kasb as 'شهرت کسبي',
tabl_inf.S_des as 'شرح فعاليت',
modi_inf.modi_seq as 'شماره مودي',
modi_inf.cod_meli  'کد ملي',
tashkhis_inf.dar_maliat_mash as 'درآمد ماليات مشاغل',
tashkhis_inf.mafiat101 as 'ميزان معافيت 101',
tashkhis_inf.moaf_sayer as 'ساير معافيت',
tashkhis_inf.asl_maliat as 'ماليات تشخيص',
tashkhis_inf.ghanony_maliat as 'ماليات قانوني',
tashkhis_inf.pardakht_mab as 'مبلغ پرداختي',
tashkhis_inf.mandeh_pardakht as 'مانده پرداختي',
tashkhis_inf.sabt_no as 'شماره ثبت برگ تشخيص',
tashkhis_inf.sabt_date as 'تاريخ ثبت برگ تشخيص',
tashkhis_inf.eblag_date as 'تاريخ ابلاغ برگ تشخيص',

case
when nahve_eblag=1 then 'به مودي' 
when nahve_eblag=2 then 'به بستگان' 
when nahve_eblag=3 then 'به مستخدم' 
when nahve_eblag=4 then 'قانوني' 
when nahve_eblag=5 then 'روزنامه اي' 
when nahve_eblag=6 then 'پست' 
when nahve_eblag=7 then 'غيره' else 'فاقد اطلاعات نحوه ابلاغ'
end as 'نحوه ابلاغ',
tashkhis_inf.Eblagh_Sabt_No as 'شماره ثبت ابلاغ برگ تشخيص',
tashkhis_inf.Eblagh_Sabt_Date as 'تاريخ ثبت ابلاغ برگ تشخيص' ,
 case when tashkhis_inf.aks_modi =1 then 'تمکين' 
 when tashkhis_inf.aks_modi =2 then 'اعتراض' else 'فاقد عکس العمل' end as 'عکس العمل مودي به برگ تشخيص',
 tashkhis_inf.aks_sabt_no as 'شماره ثبت عکس العمل برگ تشخيص',
 tashkhis_inf.aks_sabt_date as 'تاريخ ثبت عکس العمل برگ تشخيص' ,
 tashkhis_inf.Eblagh_Namek as 'ابلاغ کننده',
 tashkhis_inf.Eblagh_NameG as 'گيرنده برگ تشخيص'
 ,
 case when KMLink_Inf.have_ezhar=1
 and exists(Select Rahgiri_No  from Ezhar_Inf ee where ee.Cod_Hozeh=KMLink_Inf.Cod_Hozeh and ee.K_Parvand=KMLink_Inf.K_Parvand and ee.Sal=KMLink_Inf.sal and len(ee.Sabt_Date)>2--and LEN(Rahgiri_No)>2
 ) then 'اظهارنامه به مودي الصاق شده' 
 when (KMLink_Inf.have_ezhar !=1 OR KMLink_Inf.have_ezhar ='' or KMLink_Inf.have_ezhar IS null) 
 and exists(Select Rahgiri_No  from Ezhar_Inf ee where ee.Cod_Hozeh=KMLink_Inf.Cod_Hozeh and ee.K_Parvand=KMLink_Inf.K_Parvand and ee.Sal=KMLink_Inf.sal and len(ee.Sabt_Date)>2--and LEN(Rahgiri_No)>2
 )then 'پرونده داراي اظهارنامه' 
 
   else 'فاقد اظهارنامه' end as 'وضعيت اظهارنامه'
   
from ghabln_inf 
inner join 
KMLink_Inf on
ghabln_inf.Sal=KMLink_Inf.sal and ghabln_inf.cod_hozeh=KMLink_Inf.Cod_Hozeh and ghabln_inf.k_parvand=KMLink_Inf.K_Parvand
inner join modi_inf on
modi_inf.modi_seq=KMLink_Inf.Modi_Seq
inner join tashkhis_inf on
tashkhis_inf.sal=KMLink_Inf.sal and tashkhis_inf.cod_hozeh=KMLink_Inf.Cod_Hozeh and tashkhis_inf.k_parvand=KMLink_Inf.K_Parvand and tashkhis_inf.modi_seq=KMLink_Inf.Modi_Seq
left join [tabl_inf] on
ghabln_inf.Faliat_sharh=tabl_inf.S_code and G_code=1

where 

LEN(modi_inf.modi_seq)>8
and tashkhis_inf.serial=(select max(tt.serial)from tashkhis_inf tt where tashkhis_inf.sal=tt.sal and tashkhis_inf.cod_hozeh=tt.cod_hozeh and tashkhis_inf.k_parvand=tt.k_parvand and tashkhis_inf.modi_seq=tt.modi_seq)
and ghabln_inf.Sal in (1390,1391,1392,1393,1394,1395)

and LEN(sabt_date)>2
and exists (select * from GoResd_Inf tt where tashkhis_inf.sal=tt.sal and tashkhis_inf.cod_hozeh=tt.cod_hozeh and tashkhis_inf.k_parvand=tt.k_parvand and KMLink_Inf.Goresd_seq=tt.Goresd_seq and tt.Mab_Tash=5)

"""


def get_sql_mashaghelsonati_ghatee():

    return """
select 
case
when  ghabln_inf.cod_hozeh  =168141 then 'هويزه'
 when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1601 then 'اهواز کد يک'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1602 then 'اهواز کد دو'
 when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,5) =16439 then 'هنديجان'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,5) =16649 then 'لالي'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,5) =16718 then 'رامشير'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1603 then 'اهواز کد سه'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1604 then 'اهواز چهار '
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1605 then 'اهواز کد پنج'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1607 then 'اهواز کد هفت'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1608 then 'اهواز کد هشت'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1610 then 'اهواز'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1611 then 'اهواز'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1631 then 'آبادان کد 31'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1632 then 'آبادان کد 32'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1633 then 'آبادان کد 33'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1626 then 'دزفول'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1627 then 'دزفول'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1628 then 'دزفول'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1643 then 'ماهشهر کد43'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1644 then 'ماهشهر کد44'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1636 then 'بهبهان'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1639 then 'شوشتر'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1641 then 'گتوند'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1648 then 'بندر امام'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1649 then 'بندر امام'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1651 then 'انديمشک'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1654 then 'خرمشهر'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1659 then 'شادگان'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1661 then 'اميديه'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1663 then 'آغاجاري'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1664 then 'مسجد سليمان'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1668 then 'شوش'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1671 then 'رامهرمز'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1676 then 'ايذه'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1678 then 'هفتگل'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1679 then 'باغملک'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1681 then 'دشت آزاداگان'
else 'نامشخص' end as 'شهرستان'
,
case 
 when ghabln_inf.cod_hozeh  IN (168141) then '168141'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,5) IN ('16439','16649','16718') then SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,5) else SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) end as 'کد اداره'
,
namee as 'نام',
family as 'نام خانوادگی',
ghabln_inf.Sal as 'عملکرد',
ghabln_inf.cod_hozeh as 'کد حوزه',
ghabln_inf.k_parvand as 'کلاسه پرونده',
ghatee_inf.motamam as 'متمم',
case when KMLink_Inf.have_ezhar=1 then 'دارای اظهارنامه' else 'فاقد اظهارنامه' end as 'وضعیت ارائه اظهارنامه',
Shohr_kasb as 'شهرت کسبی',
tabl_inf.S_des as 'شرح فعالیت',
modi_inf.modi_seq as 'شماره مودی',
ghatee_inf.dar_maliat_mash as 'درآمد مالیات مشاغل',
ghatee_inf.mafiat101 as 'میزان معافیت 101',
ghatee_inf.mandeh_pardakht as 'مانده پرداخت',
ghatee_inf.asl_maliat as 'مالیات قطعی',
ghatee_inf.pardakht_mab as 'مبلغ پرداختی',
ghatee_inf.mandeh_pardakht as 'مانده پرداختی',
ghatee_inf.sabt_no as 'شماره ثبت برگ قطعی',
ghatee_inf.sabt_date as 'تاریخ ثبت برگ قطعی',
ghatee_inf.eblag_date as 'تاریخ ابلاغ برگ قطعی',
modi_inf.cod_meli as 'کدملی',
case
when nahve_eblag=1 then 'به مودی' 
when nahve_eblag=2 then 'به بستگان' 
when nahve_eblag=3 then 'به مستخدم' 
when nahve_eblag=4 then 'قانونی' 
when nahve_eblag=5 then 'روزنامه ای' 
when nahve_eblag=6 then 'پست' 
when nahve_eblag=7 then 'غیره' else 'فاقد اطلاعات نحوه ابلاغ'
end as 'نحوه ابلاغ',
ghatee_inf.Eblagh_Sabt_No as 'شماره ثبت ابلاغ برگ قطعی',
ghatee_inf.Eblagh_Sabt_Date as 'تاریخ ثبت ابلاغ برگ قطعی' ,
 case when ghatee_inf.aks_modi =1 then 'تمکین' 
 when ghatee_inf.aks_modi =2 then 'اعتراض' else 'فاقد عکس العمل' end as 'عکس العمل مودی به برگ قطعی',
 ghatee_inf.aks_sabt_no as 'شماره ثبت عکس العمل برگ قطعی',
 ghatee_inf.aks_sabt_date as 'تاریخ ثبت عکس العمل برگ قطعی' ,
 ghatee_inf.Eblagh_Namek as 'ابلاغ کننده',
 ghatee_inf.Eblagh_NameG as 'گیرنده برگ قطعی',
  ghatee_inf.jarimeh_mab as 'مبلغ جریمه برگ قطعی'

from ghabln_inf 
inner join 
KMLink_Inf on
ghabln_inf.Sal=KMLink_Inf.sal and ghabln_inf.cod_hozeh=KMLink_Inf.Cod_Hozeh and ghabln_inf.k_parvand=KMLink_Inf.K_Parvand
inner join modi_inf on
modi_inf.modi_seq=KMLink_Inf.Modi_Seq
inner join ghatee_inf on
ghatee_inf.sal=KMLink_Inf.sal and ghatee_inf.cod_hozeh=KMLink_Inf.Cod_Hozeh and ghatee_inf.k_parvand=KMLink_Inf.K_Parvand and ghatee_inf.modi_seq=KMLink_Inf.Modi_Seq
left join [tabl_inf] on
ghabln_inf.Faliat_sharh=tabl_inf.S_code and G_code=1

where 

LEN(modi_inf.modi_seq)>8
and ghatee_inf.serial=(select max(tt.serial)from ghatee_inf tt where ghatee_inf.sal=tt.sal and ghatee_inf.cod_hozeh=tt.cod_hozeh and ghatee_inf.k_parvand=tt.k_parvand and ghatee_inf.motamam=tt.motamam)


and LEN(sabt_date)>2

"""


def get_sql_mashaghelsonati_amadeersalbeheiat(date):
    return """
select 
case
when  mm.cod_hozeh  =168141 then 'هويزه'
 when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1601 then 'اهواز کد يک'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1602 then 'اهواز کد دو'
 when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,5) =16439 then 'هنديجان'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,5) =16649 then 'لالي'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,5) =16718 then 'رامشير'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1603 then 'اهواز کد سه'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1604 then 'اهواز کد چهار '
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1605 then 'اهواز کد پنج'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1606 then 'اهواز کد شش'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1607 then 'اهواز کد هفت'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1608 then 'اهواز کد هشت'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1609 then 'اهواز کد نه'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1610 then 'اهواز کد 10'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1611 then 'اهواز کد 11'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1615 then 'اهواز کد 15'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1631 then 'آبادان کد 31'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1632 then 'آبادان کد 32'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1633 then 'آبادان کد 33'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1626 then 'دزفول'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1627 then 'دزفول'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1628 then 'دزفول'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1643 then 'ماهشهر کد43'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1644 then 'ماهشهر کد44'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1636 then 'بهبهان'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1639 then 'شوشتر'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1641 then 'گتوند'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1649 then 'بندر امام'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1651 then 'انديمشک'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1654 then 'خرمشهر'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1659 then 'شادگان'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1661 then 'اميديه'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1663 then 'آغاجاري'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1664 then 'مسجد سليمان'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1668 then 'شوش'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1671 then 'رامهرمز'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1676 then 'ايذه'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1678 then 'هفتگل'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1679 then 'باغملک'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1681 then 'دشت آزاداگان'
else 'نامشخص' end as 'شهرستان'
,
case
when mm.cod_hozeh  IN (168141) then '168141'
 when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,5) IN ('16439','16649','16718') then SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,5) else SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) end as 'کد اداره'
,
mm.Cod_Hozeh as 'کد حوزه',
mm.sal as 'عملکرد',
mm.K_Parvand as 'کلاسه پرونده',
TAa.motamam AS 'متمم',
modi_inf.namee AS 'نام',
modi_inf.family AS 'نام خانوادگي',
modi_inf.cod_meli AS 'کد ملی',
modi_inf.modi_seq AS 'شماره مودی',
taa.asl_maliat as 'ماليات تشخيص' ,
taa.dar_maliat_mash as 'درآمد تشخيص',
case
when taa.nahve_eblag=1 then 'به مودي' 
when taa.nahve_eblag=2 then 'به بستگان' 
when taa.nahve_eblag=3 then 'به مستخدم' 
when taa.nahve_eblag=4 then 'قانوني' 
when taa.nahve_eblag=5 then 'روزنامه اي' 
when taa.nahve_eblag=6 then 'پست' 
when taa.nahve_eblag=7 then 'غيره' else 'فاقد اطلاعات نحوه ابلاغ'
end as 'نحوه ابلاغ',
taa.sabt_date as 'تاريخ ثبت برگ تشخيص',
taa.aks_sabt_date as 'تاريخ ثبت عکس العمل',
case when( aks_modi =1) then 'تمکين'   when( aks_modi =2) then 'اعتراض' end  AS 'عکس العمل مودي به برگ تشخيص',

case 
 when exists (select cod_hozeh from tavafogh_mcol ta where ta.cod_hozeh=hh.cod_hozeh and ta.sal=hh.Sal and ta.k_parvand=hh.k_parvand and TA.modi_seq=taa.modi_seq and len(ta.sabt_date)>2  and nazar_mcol=4 and TA.motamam=taa.motamam)then 'عدم توافق با مميز کل'
when  exists (select cod_hozeh from tashkhis_inf ta where ta.cod_hozeh=hh.cod_hozeh and ta.sal=hh.Sal and ta.k_parvand=hh.k_parvand and TA.modi_seq=taa.modi_seq and len(ta.sabt_date)>2  and nahve_eblag in (4 ,5)and TA.motamam=taa.motamam) then ' ابلاغ روزنامه اي و قانوني برگ تشخيص'
when 
( exists (select cod_hozeh from tashkhis_inf ta where ta.cod_hozeh=hh.cod_hozeh 
 and cast(aks_sabt_date as float)<%s
and ta.sal=hh.Sal and ta.k_parvand=hh.k_parvand and TA.modi_seq=taa.modi_seq and len(ta.sabt_date)>2  and aks_modi=2 and TA.motamam=taa.motamam)
and
 not exists (select cod_hozeh from tavafogh_mcol ta where ta.cod_hozeh=hh.cod_hozeh and ta.sal=hh.Sal and ta.k_parvand=hh.k_parvand and TA.modi_seq=taa.modi_seq and len(ta.sabt_date)>2  
 and TA.motamam=taa.motamam)
) then 'اعتراض مودي و عدم ادامه مراحل' end as  'دليل ارسال به هيات'
,modi_inf.tel_hamrah as 'شماره موبايل'
 ,modi_inf.cod_meli as 'کد ملي'
  ,modi_inf.tel_kar as 'تلفن محل کار'
 from ghabln_inf hh inner join KMLink_Inf mm on hh.Sal=mm.sal and hh.cod_hozeh=mm.Cod_Hozeh and hh.k_parvand=mm.K_Parvand
inner join modi_inf on modi_inf.modi_seq=mm.Modi_Seq
inner join tashkhis_inf taa on taa.sal=mm.sal and taa.cod_hozeh=mm.Cod_Hozeh and taa.k_parvand =mm.K_Parvand and taa.modi_seq=mm.Modi_Seq
where
 (CONNECTIONPROPERTY('local_net_address' )!= '10.53.32.130' or (CONNECTIONPROPERTY('local_net_address' )= '10.53.32.130' and hh.cod_hozeh !=168141))
and
 (CONNECTIONPROPERTY('local_net_address' )!= '10.53.48.130' or (CONNECTIONPROPERTY('local_net_address' )= '10.53.48.130' and hh.cod_hozeh =168141))
and
LEN(taa.sabt_date)>2 and LEN(taa.eblag_date)>2

and hh.sal>=1389

and
not exists (select cod_hozeh from adam_faliat_inf ta where ta.cod_hozeh=hh.cod_hozeh and ta.sal=hh.Sal and ta.k_parvand=hh.k_parvand and len(ta.sabt_no)>1  and isnull(ebtal, 0) = 0 )
and not exists (select cod_hozeh from [TaeenNobat_inf] ta where ta.cod_hozeh=hh.cod_hozeh and ta.sal=hh.Sal and ta.k_parvand=hh.k_parvand and TA.modi_seq=taa.modi_seq and TA.motamam=taa.motamam )
AND
not exists (select cod_hozeh from ghatee_inf ta where ta.cod_hozeh=hh.cod_hozeh and ta.sal=hh.Sal and ta.k_parvand=hh.k_parvand and TA.modi_seq=taa.modi_seq and TA.motamam=taa.motamam)
AND
not exists (select cod_hozeh from Badvi_inf ta where ta.cod_hozeh=hh.cod_hozeh and ta.sal=hh.Sal and ta.k_parvand=hh.k_parvand and TA.modi_seq=taa.modi_seq and LEN([davat_date])>3 and TA.motamam=taa.motamam)
AND
(
 exists (select cod_hozeh from tavafogh_mcol ta where ta.cod_hozeh=hh.cod_hozeh and ta.sal=hh.Sal and ta.k_parvand=hh.k_parvand and TA.modi_seq=taa.modi_seq and len(ta.sabt_date)>2  and nazar_mcol=4 and TA.motamam=taa.motamam)
or
 exists (select cod_hozeh from tashkhis_inf ta where ta.cod_hozeh=hh.cod_hozeh and ta.sal=hh.Sal and ta.k_parvand=hh.k_parvand and TA.modi_seq=taa.modi_seq and len(ta.sabt_date)>2  and nahve_eblag in (4 ,5)and TA.motamam=taa.motamam)
or
( exists (select cod_hozeh from tashkhis_inf ta where ta.cod_hozeh=hh.cod_hozeh and ta.sal=hh.Sal and ta.k_parvand=hh.k_parvand and TA.modi_seq=taa.modi_seq and len(ta.sabt_date)>2 
 and cast(aks_sabt_date as float)<%s

 and aks_modi=2 and TA.motamam=taa.motamam)
and
 not exists (select cod_hozeh from tavafogh_mcol ta where ta.cod_hozeh=hh.cod_hozeh and ta.sal=hh.Sal and ta.k_parvand=hh.k_parvand and TA.modi_seq=taa.modi_seq and len(ta.sabt_date)>2   and TA.motamam=taa.motamam)
)
)
and taa.serial=(select max(serial)from tashkhis_inf where taa.cod_hozeh=cod_hozeh and taa.k_parvand=k_parvand and taa.sal=sal and taa.motamam=motamam and taa.modi_seq=modi_seq)


""" % (date, date)


def get_sql_query_tashnoeblagh():
    '''گزارش تشخیص ابلاغ نشده سنیم'''
    return """
SELECT 
[شماره اقتصادی]
,replace(REPLACE(REPLACE([نام مودی], CHAR(13), ''), CHAR(10), ''),'	','') AS [نام مودی]
,[کد رهگيري ثبت نام ]
,[کد اداره]
,[نام اداره]
,[شماره اظهارنامه]
,[سال عملکرد]
,[منبع مالیاتی] 
,[دوره عملکرد]
,[شماره برگ تشخیص]
,[تاریخ صدور برگ تشخیص (تاریخ تایید گزارش حسابرسی)]
,[مالیات تشخیص]
,[تاریخ ابلاغ برگ تشخیص]
,[شماره برگ قطعی]
,[تاريخ ايجاد برگ قطعي]
FROM [TestDbv2].[dbo].[V_PORTAL]
WHERE [منبع مالیاتی] IN (N'مالیات بر درآمد مشاغل',N'مالیات بر درآمد شرکت ها',N'مالیات بر ارزش افزوده' )
      AND SUBSTRING([سال عملکرد],1,4)>='1396'
      AND LEN([تاریخ صدور برگ تشخیص (تاریخ تایید گزارش حسابرسی)])>5
      --AND [تاریخ صدور برگ تشخیص (تاریخ تایید گزارش حسابرسی)]<='1401/05/31'
      AND [تاریخ ابلاغ برگ تشخیص]='0'
"""


def get_sql_tash_ghat_most():
    return """
 SELECT 
    ISNULL(tblTashSadere.IrisCode,ISNULL(tblGhateeSadere.IrisCode,ISNULL(tblTashEblagh.IrisCode,tblGhateeEblagh.IrisCode)))AS IrisCode
    ,[تشخیص صادره مستغلات]
    ,[قطعی صادره مستغلات]
    ,[تشخیص ابلاغ شده مستغلات]
    ,[قطعی ابلاغ شده مستغلات]
    FROM
    (SELECT IrisCode,N'ریسک متوسط' AS [نوع اظهارنامه],Count(*) AS [تشخیص صادره مستغلات] FROM
    ((select ISNULL(tax.dbo.tblEdareShahr.IrisCode,'**') as 'IrisCode',ISNULL(tax.dbo.tblEdareShahr.city,'**') as 'شهرستان-',ISNULL(tax.dbo.tblEdareShahr.edare,'**') as 'اداره',ISNULL(tax.dbo.tblEdareShahr.city,'**')+'-'+ISNULL(tax.dbo.tblEdareShahr.edare,'**') as [اداره-شهر] , tblMain.* from 
    (SELECT [واحد مالیاتی] ,[تاریخ صدور] ,[تاریخ ابلاغ],[ نحوه ابلاغ]  FROM [TestDb].[dbo].[tblTashkhisMost])tblMain
    inner join tax.dbo.tblEdareShahr on substring(tblMain.[واحد مالیاتی],1,5) = substring(tax.dbo.tblEdareShahr.edare,1,5) collate Arabic_CI_AS 
    
    UNION
    
    select ISNULL(tax.dbo.tblEdareShahr.IrisCode,'**') as 'IrisCode',ISNULL(tax.dbo.tblEdareShahr.city,'**') as 'شهرستان-',ISNULL(tax.dbo.tblEdareShahr.edare,'**') as 'اداره',ISNULL(tax.dbo.tblEdareShahr.city,'**')+'-'+ISNULL(tax.dbo.tblEdareShahr.edare,'**') as [اداره-شهر] , tblMain.* from 
    (SELECT [واحد مالیاتی] ,[تاریخ صدور] ,[تاریخ ابلاغ],[ نحوه ابلاغ]  FROM [TestDb].[dbo].[tblTashkhisMost])tblMain
    Left join tax.dbo.tblEdareShahr on substring(tblMain.[واحد مالیاتی],1,4) = tax.dbo.tblEdareShahr.edare collate Arabic_CI_AS
    where isnull(tblMain.[واحد مالیاتی],'i') not in 
    ( SELECT [واحد مالیاتی]
    FROM 
    (SELECT [واحد مالیاتی] ,[تاریخ صدور] ,[تاریخ ابلاغ],[ نحوه ابلاغ]  FROM [TestDb].[dbo].[tblTashkhisMost])tblMain
    inner join tax.dbo.tblEdareShahr on substring(tblMain.[واحد مالیاتی],1,5) = substring(tax.dbo.tblEdareShahr.edare,1,5) collate Arabic_CI_AS ))
    )
    as a
    WHERE [تاریخ صدور] BETWEEN '1402/01/01' AND '1402/12/29'
    GROUP BY IrisCode) AS tblTashSadere
    FULL JOIN
    (SELECT IrisCode,N'ریسک متوسط' AS [نوع اظهارنامه],Count(*) AS [قطعی صادره مستغلات] FROM
    ((select ISNULL(tax.dbo.tblEdareShahr.IrisCode,'**') as 'IrisCode',ISNULL(tax.dbo.tblEdareShahr.city,'**') as 'شهرستان-',ISNULL(tax.dbo.tblEdareShahr.edare,'**') as 'اداره',ISNULL(tax.dbo.tblEdareShahr.city,'**')+'-'+ISNULL(tax.dbo.tblEdareShahr.edare,'**') as [اداره-شهر] , tblMain.* from 
    (SELECT [واحد مالیاتی] ,[تاریخ صدور] ,[تاریخ ابلاغ] FROM [TestDb].[dbo].[tblGhateeMost])tblMain
    inner join tax.dbo.tblEdareShahr on substring(tblMain.[واحد مالیاتی],1,5) = substring(tax.dbo.tblEdareShahr.edare,1,5) collate Arabic_CI_AS 
    
    UNION
    
    select ISNULL(tax.dbo.tblEdareShahr.IrisCode,'**') as 'IrisCode',ISNULL(tax.dbo.tblEdareShahr.city,'**') as 'شهرستان-',ISNULL(tax.dbo.tblEdareShahr.edare,'**') as 'اداره',ISNULL(tax.dbo.tblEdareShahr.city,'**')+'-'+ISNULL(tax.dbo.tblEdareShahr.edare,'**') as [اداره-شهر] , tblMain.* from 
    (SELECT [واحد مالیاتی] ,[تاریخ صدور] ,[تاریخ ابلاغ] FROM [TestDb].[dbo].[tblGhateeMost])tblMain
    Left join tax.dbo.tblEdareShahr on substring(tblMain.[واحد مالیاتی],1,4) = tax.dbo.tblEdareShahr.edare collate Arabic_CI_AS
    where isnull(tblMain.[واحد مالیاتی],'i') not in 
    ( SELECT [واحد مالیاتی]
    FROM 
    (SELECT [واحد مالیاتی] ,[تاریخ صدور] ,[تاریخ ابلاغ] FROM [TestDb].[dbo].[tblGhateeMost])tblMain
    inner join tax.dbo.tblEdareShahr on substring(tblMain.[واحد مالیاتی],1,5) = substring(tax.dbo.tblEdareShahr.edare,1,5) collate Arabic_CI_AS ))
    )
    as a
    WHERE [تاریخ صدور] BETWEEN '1402/01/01' AND '1402/12/29'
    GROUP BY IrisCode) AS tblGhateeSadere
    ON tblTashSadere.IrisCode=tblGhateeSadere.IrisCode
    FULL JOIN
    (SELECT IrisCode,N'ریسک متوسط' AS [نوع اظهارنامه],Count(*) AS [تشخیص ابلاغ شده مستغلات] FROM
    ((select ISNULL(tax.dbo.tblEdareShahr.IrisCode,'**') as 'IrisCode',ISNULL(tax.dbo.tblEdareShahr.city,'**') as 'شهرستان-',ISNULL(tax.dbo.tblEdareShahr.edare,'**') as 'اداره',ISNULL(tax.dbo.tblEdareShahr.city,'**')+'-'+ISNULL(tax.dbo.tblEdareShahr.edare,'**') as [اداره-شهر] , tblMain.* from 
    (SELECT [واحد مالیاتی] ,[تاریخ صدور] ,[تاریخ ابلاغ],[ نحوه ابلاغ]  FROM [TestDb].[dbo].[tblTashkhisMost])tblMain
    inner join tax.dbo.tblEdareShahr on substring(tblMain.[واحد مالیاتی],1,5) = substring(tax.dbo.tblEdareShahr.edare,1,5) collate Arabic_CI_AS 
    
    UNION
    
    select ISNULL(tax.dbo.tblEdareShahr.IrisCode,'**') as 'IrisCode',ISNULL(tax.dbo.tblEdareShahr.city,'**') as 'شهرستان-',ISNULL(tax.dbo.tblEdareShahr.edare,'**') as 'اداره',ISNULL(tax.dbo.tblEdareShahr.city,'**')+'-'+ISNULL(tax.dbo.tblEdareShahr.edare,'**') as [اداره-شهر] , tblMain.* from 
    (SELECT [واحد مالیاتی] ,[تاریخ صدور] ,[تاریخ ابلاغ],[ نحوه ابلاغ]  FROM [TestDb].[dbo].[tblTashkhisMost])tblMain
    Left join tax.dbo.tblEdareShahr on substring(tblMain.[واحد مالیاتی],1,4) = tax.dbo.tblEdareShahr.edare collate Arabic_CI_AS
    where isnull(tblMain.[واحد مالیاتی],'i') not in 
    ( SELECT [واحد مالیاتی]
    FROM 
    (SELECT [واحد مالیاتی] ,[تاریخ صدور] ,[تاریخ ابلاغ],[ نحوه ابلاغ]  FROM [TestDb].[dbo].[tblTashkhisMost])tblMain
    inner join tax.dbo.tblEdareShahr on substring(tblMain.[واحد مالیاتی],1,5) = substring(tax.dbo.tblEdareShahr.edare,1,5) collate Arabic_CI_AS ))
    )
    as a
    WHERE [تاریخ ابلاغ] BETWEEN '1402/01/01' AND '1402/12/29'
    AND [ نحوه ابلاغ]<>N'الکترونیکی'
    GROUP BY IrisCode)tblTashEblagh
    ON tblTashSadere.IrisCode=tblTashEblagh.IrisCode
    FULL JOIN
    (SELECT IrisCode,N'ریسک متوسط' AS [نوع اظهارنامه],Count(*) AS [قطعی ابلاغ شده مستغلات] FROM
    ((select ISNULL(tax.dbo.tblEdareShahr.IrisCode,'**') as 'IrisCode',ISNULL(tax.dbo.tblEdareShahr.city,'**') as 'شهرستان-',ISNULL(tax.dbo.tblEdareShahr.edare,'**') as 'اداره',ISNULL(tax.dbo.tblEdareShahr.city,'**')+'-'+ISNULL(tax.dbo.tblEdareShahr.edare,'**') as [اداره-شهر] , tblMain.* from 
    (SELECT [واحد مالیاتی] ,[تاریخ صدور] ,[تاریخ ابلاغ] FROM [TestDb].[dbo].[tblGhateeMost])tblMain
    inner join tax.dbo.tblEdareShahr on substring(tblMain.[واحد مالیاتی],1,5) = substring(tax.dbo.tblEdareShahr.edare,1,5) collate Arabic_CI_AS 
    
    UNION
    
    select ISNULL(tax.dbo.tblEdareShahr.IrisCode,'**') as 'IrisCode',ISNULL(tax.dbo.tblEdareShahr.city,'**') as 'شهرستان-',ISNULL(tax.dbo.tblEdareShahr.edare,'**') as 'اداره',ISNULL(tax.dbo.tblEdareShahr.city,'**')+'-'+ISNULL(tax.dbo.tblEdareShahr.edare,'**') as [اداره-شهر] , tblMain.* from 
    (SELECT [واحد مالیاتی] ,[تاریخ صدور] ,[تاریخ ابلاغ] FROM [TestDb].[dbo].[tblGhateeMost])tblMain
    Left join tax.dbo.tblEdareShahr on substring(tblMain.[واحد مالیاتی],1,4) = tax.dbo.tblEdareShahr.edare collate Arabic_CI_AS
    where isnull(tblMain.[واحد مالیاتی],'i') not in 
    ( SELECT [واحد مالیاتی]
    FROM 
    (SELECT [واحد مالیاتی] ,[تاریخ صدور] ,[تاریخ ابلاغ] FROM [TestDb].[dbo].[tblGhateeMost])tblMain
    inner join tax.dbo.tblEdareShahr on substring(tblMain.[واحد مالیاتی],1,5) = substring(tax.dbo.tblEdareShahr.edare,1,5) collate Arabic_CI_AS ))
    )
    as a
    WHERE [تاریخ ابلاغ] BETWEEN '1402/01/01' AND '1402/12/29'
    GROUP BY IrisCode) AS tblGhateeEblagh
    ON tblTashSadere.IrisCode=tblGhateeEblagh.IrisCode
"""


def get_sql_hoghigh_sonati_238():
    return """
 SELECT 
 IrisCode
,[عملکرد] AS [توافق 238 عملکرد شرکت ها سنتی]
,[حقوق] AS [توافق 238 حقوق شرکت ها سنتی] 
,[تکليفي] AS [توافق 238 تکلیفی شرکت ها سنتی] 
,[169] AS [توافق 238 ،169 شرکت ها سنتی] 
 FROM
(
SELECT IrisCode,[منبع],N'ریسک متوسط' AS [نوع اظهارنامه],Count(*) AS [238] FROM
((select ISNULL(tax.dbo.tblEdareShahr.IrisCode,'**') as 'IrisCode',ISNULL(tax.dbo.tblEdareShahr.city,'**') as 'شهرستان-',ISNULL(tax.dbo.tblEdareShahr.edare,'**') as 'اداره',ISNULL(tax.dbo.tblEdareShahr.city,'**')+'-'+ISNULL(tax.dbo.tblEdareShahr.edare,'**') as [اداره-شهر] , tblMain.* from 
(SELECT [sal] ,[منبع] ,[نوع اظهارنامه] ,[C11_واحد مالیاتی] AS [واحد مالیاتی] ,[C33_تاریخ] ,[C36_نتیجه توافق] FROM [HReportFiles].[dbo].[V238])tblMain
 inner join tax.dbo.tblEdareShahr on substring(tblMain.[واحد مالیاتی],1,5) = substring(tax.dbo.tblEdareShahr.edare,1,5) collate Arabic_CI_AS 
 
 UNION
 
select ISNULL(tax.dbo.tblEdareShahr.IrisCode,'**') as 'IrisCode',ISNULL(tax.dbo.tblEdareShahr.city,'**') as 'شهرستان-',ISNULL(tax.dbo.tblEdareShahr.edare,'**') as 'اداره',ISNULL(tax.dbo.tblEdareShahr.city,'**')+'-'+ISNULL(tax.dbo.tblEdareShahr.edare,'**') as [اداره-شهر] , tblMain.* from 
(SELECT [sal] ,[منبع] ,[نوع اظهارنامه] ,[C11_واحد مالیاتی] AS [واحد مالیاتی] ,[C33_تاریخ] ,[C36_نتیجه توافق] FROM [HReportFiles].[dbo].[V238])tblMain
 Left join tax.dbo.tblEdareShahr on substring(tblMain.[واحد مالیاتی],1,4) = tax.dbo.tblEdareShahr.edare collate Arabic_CI_AS
 where isnull(tblMain.[واحد مالیاتی],'i') not in 
 ( SELECT [واحد مالیاتی]
 FROM 
(SELECT [sal] ,[منبع] ,[نوع اظهارنامه] ,[C11_واحد مالیاتی] AS [واحد مالیاتی] ,[C33_تاریخ] ,[C36_نتیجه توافق] FROM [HReportFiles].[dbo].[V238])tblMain
 inner join tax.dbo.tblEdareShahr on substring(tblMain.[واحد مالیاتی],1,5) = substring(tax.dbo.tblEdareShahr.edare,1,5) collate Arabic_CI_AS ))
 )
 as a
 WHERE [C33_تاریخ] BETWEEN '14020101' AND '14021229'
 GROUP BY IrisCode,[منبع]
 )src
 pivot
(
 sum([238])
 for [منبع] in ([169],[تکليفي],[حقوق],[عملکرد])
 )piv

"""


def get_sql_hoghoghi_sonati_tash_ghat():
    return """
SELECT 
ISNULL(tashkhisSadere.IrisCode,ISNULL(GHateeSadere.IrisCode,ISNULL(TashkhisEblagh.IrisCode,GhateeEblagh.IrisCode)))AS IrisCode
,[تشخیص صادره عملکرد حقوقی سنتی]
,[تشخیص صادره حقوق حقوقی سنتی]
,[تشخیص صادره تکلیفی حقوقی سنتی]
,[تشخیص صادره 169 حقوقی سنتی]
,[قطعی صادره عملکرد حقوقی سنتی]
,[قطعی صادره حقوق حقوقی سنتی] 
,[قطعی صادره تکلیفی حقوقی سنتی] 
,[قطعی صادره 169 حقوقی سنتی] 
,[تشخیص ابلاغ شده عملکرد حقوقی سنتی]
,[تشخیص ابلاغ شده حقوق حقوقی سنتی] 
,[تشخیص ابلاغ شده تکلیفی حقوقی سنتی] 
,[تشخیص ابلاغ شده 169 حقوقی سنتی] 
,[قطعی ابلاغ شده عملکرد حقوقی سنتی]
,[قطعی ابلاغ شده حقوق حقوقی سنتی] 
,[قطعی ابلاغ شده تکلیفی حقوقی سنتی] 
,[قطعی ابلاغ شده 169 حقوقی سنتی] 

 FROM
(SELECT 
IrisCode
,[عملکرد] AS [تشخیص صادره عملکرد حقوقی سنتی]
,[حقوق] AS [تشخیص صادره حقوق حقوقی سنتی] 
,[تکليفي] AS [تشخیص صادره تکلیفی حقوقی سنتی] 
,[169] AS [تشخیص صادره 169 حقوقی سنتی] 
 FROM
(SELECT IrisCode,[منبع],N'ریسک متوسط' AS [نوع اظهارنامه],Count(*) AS [تشخیص صادره] FROM
((select ISNULL(tax.dbo.tblEdareShahr.IrisCode,'**') as 'IrisCode',ISNULL(tax.dbo.tblEdareShahr.city,'**') as 'شهرستان-',ISNULL(tax.dbo.tblEdareShahr.edare,'**') as 'اداره',ISNULL(tax.dbo.tblEdareShahr.city,'**')+'-'+ISNULL(tax.dbo.tblEdareShahr.edare,'**') as [اداره-شهر] , tblMain.* from 
(SELECT [sal] ,[منبع] ,[نوع اظهارنامه] ,[C8_حوزه مطالبه و وصول] AS [واحد مالیاتی] ,[C26_تاریخ تشخیص] ,[C27_تاریخ ابلاغ] FROM [HReportFiles].[dbo].[VTashkhisALL])tblMain
 inner join tax.dbo.tblEdareShahr on substring(tblMain.[واحد مالیاتی],1,5) = substring(tax.dbo.tblEdareShahr.edare,1,5) collate Arabic_CI_AS 
 
 UNION
 
select ISNULL(tax.dbo.tblEdareShahr.IrisCode,'**') as 'IrisCode',ISNULL(tax.dbo.tblEdareShahr.city,'**') as 'شهرستان-',ISNULL(tax.dbo.tblEdareShahr.edare,'**') as 'اداره',ISNULL(tax.dbo.tblEdareShahr.city,'**')+'-'+ISNULL(tax.dbo.tblEdareShahr.edare,'**') as [اداره-شهر] , tblMain.* from 
(SELECT [sal] ,[منبع] ,[نوع اظهارنامه] ,[C8_حوزه مطالبه و وصول]AS [واحد مالیاتی] ,[C26_تاریخ تشخیص] ,[C27_تاریخ ابلاغ] FROM [HReportFiles].[dbo].[VTashkhisALL])tblMain
 Left join tax.dbo.tblEdareShahr on substring(tblMain.[واحد مالیاتی],1,4) = tax.dbo.tblEdareShahr.edare collate Arabic_CI_AS
 where isnull(tblMain.[واحد مالیاتی],'i') not in 
 ( SELECT [واحد مالیاتی]
 FROM 
(SELECT [sal] ,[منبع] ,[نوع اظهارنامه] ,[C8_حوزه مطالبه و وصول]AS [واحد مالیاتی] ,[C26_تاریخ تشخیص] ,[C27_تاریخ ابلاغ] FROM [HReportFiles].[dbo].[VTashkhisALL])tblMain
  inner join tax.dbo.tblEdareShahr on substring(tblMain.[واحد مالیاتی],1,5) = substring(tax.dbo.tblEdareShahr.edare,1,5) collate Arabic_CI_AS ))
 )
 as a
 WHERE [C26_تاریخ تشخیص] BETWEEN '14020101' AND '14021229'
 GROUP BY IrisCode,[منبع]
 )src
  pivot
(
  sum([تشخیص صادره])
  for [منبع] in ([169],[تکليفي],[حقوق],[عملکرد])
) piv)AS tashkhisSadere
FULL JOIN

(SELECT 
IrisCode
,[عملکرد] AS [قطعی صادره عملکرد حقوقی سنتی]
,[حقوق] AS [قطعی صادره حقوق حقوقی سنتی] 
,[تکليفي] AS [قطعی صادره تکلیفی حقوقی سنتی] 
,[169] AS [قطعی صادره 169 حقوقی سنتی] 
 FROM
(
SELECT IrisCode,[منبع],N'ریسک متوسط' AS [نوع اظهارنامه],Count(*) AS [قطعی صادره] FROM
((select ISNULL(tax.dbo.tblEdareShahr.IrisCode,'**') as 'IrisCode',ISNULL(tax.dbo.tblEdareShahr.city,'**') as 'شهرستان-',ISNULL(tax.dbo.tblEdareShahr.edare,'**') as 'اداره',ISNULL(tax.dbo.tblEdareShahr.city,'**')+'-'+ISNULL(tax.dbo.tblEdareShahr.edare,'**') as [اداره-شهر] , tblMain.* from 
(SELECT  [sal] ,[منبع] ,[نوع اظهارنامه] ,[C11_واحد مالیاتی] AS [واحد مالیاتی],[C28_تاریخ] ,[C32_تاریخ ابلاغ] FROM [HReportFiles].[dbo].[VGhateeALL])tblMain
 inner join tax.dbo.tblEdareShahr on substring(tblMain.[واحد مالیاتی],1,5) = substring(tax.dbo.tblEdareShahr.edare,1,5) collate Arabic_CI_AS 
 
 UNION
 
select ISNULL(tax.dbo.tblEdareShahr.IrisCode,'**') as 'IrisCode',ISNULL(tax.dbo.tblEdareShahr.city,'**') as 'شهرستان-',ISNULL(tax.dbo.tblEdareShahr.edare,'**') as 'اداره',ISNULL(tax.dbo.tblEdareShahr.city,'**')+'-'+ISNULL(tax.dbo.tblEdareShahr.edare,'**') as [اداره-شهر] , tblMain.* from 
(SELECT  [sal] ,[منبع] ,[نوع اظهارنامه] ,[C11_واحد مالیاتی] AS [واحد مالیاتی],[C28_تاریخ] ,[C32_تاریخ ابلاغ] FROM [HReportFiles].[dbo].[VGhateeALL])tblMain
 Left join tax.dbo.tblEdareShahr on substring(tblMain.[واحد مالیاتی],1,4) = tax.dbo.tblEdareShahr.edare collate Arabic_CI_AS
 where isnull(tblMain.[واحد مالیاتی],'i') not in 
 ( SELECT [واحد مالیاتی]
 FROM 
(SELECT  [sal] ,[منبع] ,[نوع اظهارنامه] ,[C11_واحد مالیاتی] AS [واحد مالیاتی],[C28_تاریخ] ,[C32_تاریخ ابلاغ] FROM [HReportFiles].[dbo].[VGhateeALL])tblMain
  inner join tax.dbo.tblEdareShahr on substring(tblMain.[واحد مالیاتی],1,5) = substring(tax.dbo.tblEdareShahr.edare,1,5) collate Arabic_CI_AS ))
 )
 as a
 WHERE [C28_تاریخ] BETWEEN '14020101' AND '14021229'
 GROUP BY IrisCode,[منبع]
 )src
  pivot
(
  sum([قطعی صادره])
  for [منبع] in ([169],[تکليفي],[حقوق],[عملکرد])
) piv) AS GHateeSadere
ON 
tashkhisSadere.IrisCode=GHateeSadere.IrisCode
FULL JOIN

(SELECT 
IrisCode
,[عملکرد] AS [تشخیص ابلاغ شده عملکرد حقوقی سنتی]
,[حقوق] AS [تشخیص ابلاغ شده حقوق حقوقی سنتی] 
,[تکليفي] AS [تشخیص ابلاغ شده تکلیفی حقوقی سنتی] 
,[169] AS [تشخیص ابلاغ شده 169 حقوقی سنتی] 
 FROM
(
SELECT IrisCode,[منبع],N'ریسک متوسط' AS [نوع اظهارنامه],Count(*) AS [تشخیص ابلاغ شده] FROM
((select ISNULL(tax.dbo.tblEdareShahr.IrisCode,'**') as 'IrisCode',ISNULL(tax.dbo.tblEdareShahr.city,'**') as 'شهرستان-',ISNULL(tax.dbo.tblEdareShahr.edare,'**') as 'اداره',ISNULL(tax.dbo.tblEdareShahr.city,'**')+'-'+ISNULL(tax.dbo.tblEdareShahr.edare,'**') as [اداره-شهر] , tblMain.* from 
(SELECT [sal] ,[منبع] ,[نوع اظهارنامه] ,[C8_حوزه مطالبه و وصول] AS [واحد مالیاتی] ,[C26_تاریخ تشخیص] ,[C27_تاریخ ابلاغ] FROM [HReportFiles].[dbo].[VTashkhisALL])tblMain
 inner join tax.dbo.tblEdareShahr on substring(tblMain.[واحد مالیاتی],1,5) = substring(tax.dbo.tblEdareShahr.edare,1,5) collate Arabic_CI_AS 
 
 UNION
 
select ISNULL(tax.dbo.tblEdareShahr.IrisCode,'**') as 'IrisCode',ISNULL(tax.dbo.tblEdareShahr.city,'**') as 'شهرستان-',ISNULL(tax.dbo.tblEdareShahr.edare,'**') as 'اداره',ISNULL(tax.dbo.tblEdareShahr.city,'**')+'-'+ISNULL(tax.dbo.tblEdareShahr.edare,'**') as [اداره-شهر] , tblMain.* from 
(SELECT [sal] ,[منبع] ,[نوع اظهارنامه] ,[C8_حوزه مطالبه و وصول]AS [واحد مالیاتی] ,[C26_تاریخ تشخیص] ,[C27_تاریخ ابلاغ] FROM [HReportFiles].[dbo].[VTashkhisALL])tblMain
 Left join tax.dbo.tblEdareShahr on substring(tblMain.[واحد مالیاتی],1,4) = tax.dbo.tblEdareShahr.edare collate Arabic_CI_AS
 where isnull(tblMain.[واحد مالیاتی],'i') not in 
 ( SELECT [واحد مالیاتی]
 FROM 
(SELECT [sal] ,[منبع] ,[نوع اظهارنامه] ,[C8_حوزه مطالبه و وصول]AS [واحد مالیاتی] ,[C26_تاریخ تشخیص] ,[C27_تاریخ ابلاغ] FROM [HReportFiles].[dbo].[VTashkhisALL])tblMain
  inner join tax.dbo.tblEdareShahr on substring(tblMain.[واحد مالیاتی],1,5) = substring(tax.dbo.tblEdareShahr.edare,1,5) collate Arabic_CI_AS ))
 )
 as a
 WHERE [C27_تاریخ ابلاغ] BETWEEN '14020101' AND '14021229'
 GROUP BY IrisCode,[منبع]
 )src
  pivot
(
  sum([تشخیص ابلاغ شده])
  for [منبع] in ([169],[تکليفي],[حقوق],[عملکرد])
) piv)AS TashkhisEblagh
ON ISNULL(tashkhisSadere.IrisCode,GHateeSadere.IrisCode)=TashkhisEblagh.IrisCode
FULL JOIN

(SELECT 
IrisCode
,[عملکرد] AS [قطعی ابلاغ شده عملکرد حقوقی سنتی]
,[حقوق] AS [قطعی ابلاغ شده حقوق حقوقی سنتی] 
,[تکليفي] AS [قطعی ابلاغ شده تکلیفی حقوقی سنتی] 
,[169] AS [قطعی ابلاغ شده 169 حقوقی سنتی] 
 FROM
(
SELECT IrisCode,[منبع],N'ریسک متوسط' AS [نوع اظهارنامه],Count(*) AS [قطعی ابلاغ شده] FROM
((select ISNULL(tax.dbo.tblEdareShahr.IrisCode,'**') as 'IrisCode',ISNULL(tax.dbo.tblEdareShahr.city,'**') as 'شهرستان-',ISNULL(tax.dbo.tblEdareShahr.edare,'**') as 'اداره',ISNULL(tax.dbo.tblEdareShahr.city,'**')+'-'+ISNULL(tax.dbo.tblEdareShahr.edare,'**') as [اداره-شهر] , tblMain.* from 
(SELECT  [sal] ,[منبع] ,[نوع اظهارنامه] ,[C11_واحد مالیاتی] AS [واحد مالیاتی],[C28_تاریخ] ,[C32_تاریخ ابلاغ] FROM [HReportFiles].[dbo].[VGhateeALL])tblMain
 inner join tax.dbo.tblEdareShahr on substring(tblMain.[واحد مالیاتی],1,5) = substring(tax.dbo.tblEdareShahr.edare,1,5) collate Arabic_CI_AS 
 
 UNION
 
select ISNULL(tax.dbo.tblEdareShahr.IrisCode,'**') as 'IrisCode',ISNULL(tax.dbo.tblEdareShahr.city,'**') as 'شهرستان-',ISNULL(tax.dbo.tblEdareShahr.edare,'**') as 'اداره',ISNULL(tax.dbo.tblEdareShahr.city,'**')+'-'+ISNULL(tax.dbo.tblEdareShahr.edare,'**') as [اداره-شهر] , tblMain.* from 
(SELECT  [sal] ,[منبع] ,[نوع اظهارنامه] ,[C11_واحد مالیاتی] AS [واحد مالیاتی],[C28_تاریخ] ,[C32_تاریخ ابلاغ] FROM [HReportFiles].[dbo].[VGhateeALL])tblMain
 Left join tax.dbo.tblEdareShahr on substring(tblMain.[واحد مالیاتی],1,4) = tax.dbo.tblEdareShahr.edare collate Arabic_CI_AS
 where isnull(tblMain.[واحد مالیاتی],'i') not in 
 ( SELECT [واحد مالیاتی]
 FROM 
(SELECT  [sal] ,[منبع] ,[نوع اظهارنامه] ,[C11_واحد مالیاتی] AS [واحد مالیاتی],[C28_تاریخ] ,[C32_تاریخ ابلاغ] FROM [HReportFiles].[dbo].[VGhateeALL])tblMain
  inner join tax.dbo.tblEdareShahr on substring(tblMain.[واحد مالیاتی],1,5) = substring(tax.dbo.tblEdareShahr.edare,1,5) collate Arabic_CI_AS ))
 )
 as a
 WHERE [C32_تاریخ ابلاغ] BETWEEN '14020101' AND '14021229'
 GROUP BY IrisCode,[منبع]
 )src
  pivot
(
  sum([قطعی ابلاغ شده])
  for [منبع] in ([169],[تکليفي],[حقوق],[عملکرد])
) piv)GhateeEblagh
ON ISNULL(tashkhisSadere.IrisCode,ISNULL(GHateeSadere.IrisCode,TashkhisEblagh.IrisCode))=GhateeEblagh.IrisCode
"""


def get_sql_query_sanimdata(year=None):

    if year is not None:

        return """
            SELECT [نام مودی],[شماره اقتصادی],[دوره عملکرد], CASE [نوع اظهارنامه] 
            WHEN N'رتبه ریسک بالا' THEN N'ريسک بالا' 
            WHEN N'مودیان مهم با ریسک بالا' THEN N'ريسک مهم' 
            WHEN N'انتخاب شده بدون اعمال معیار ریسک' THEN N'ريسک مهم' 
            WHEN N'ریسک متوسط' THEN N'ريسک متوسط' 
            ELSE N'ريسک متوسط'
            END AS [نوع اظهارنامه],
            [منبع مالیاتی],[کد رهگیری اظهارنامه سنیم - آخرین ورژن اعمال شد], 
            [ابلاغ الکترونیک برگ تشخیص], [ابلاغ الکترونیک برگ قطعی], [شناسه حسابرسی],
            [شماره برگ تشخیص], [تاریخ صدور برگ تشخیص (تاریخ تایید گزارش حسابرسی)], [تاريخ ايجاد برگ دعوت بدوی],
            [تاريخ ابلاغ برگ دعوت بدوی], [تاریخ ابلاغ رای تجدید نظر], [توافق], [تاریخ تایید رای],
            [تاریخ ابلاغ برگ تشخیص], [تاريخ ايجاد برگ قطعي], [شماره برگ قطعی], [تاریخ ابلاغ برگ قطعی],
            [سال عملکرد],[نام اداره], [کد اداره], [آخرین وضعیت]
            FROM [testdbV2].[dbo].[V_PORTAL]  
            WHERE [سال عملکرد] = '%s'
            """ % year

    else:
        return """
            SELECT [نام مودی],[شماره اقتصادی],[دوره عملکرد], CASE [نوع اظهارنامه] 
            WHEN N'رتبه ریسک بالا' THEN N'ريسک بالا' 
            WHEN N'مودیان مهم با ریسک بالا' THEN N'ريسک مهم' 
            WHEN N'انتخاب شده بدون اعمال معیار ریسک' THEN N'ريسک مهم' 
            WHEN N'ریسک متوسط' THEN N'ريسک متوسط' 
            ELSE N'ريسک متوسط'
            END AS [نوع اظهارنامه],
            [منبع مالیاتی],[کد رهگیری اظهارنامه سنیم - آخرین ورژن اعمال شد], 
            [ابلاغ الکترونیک برگ تشخیص], [ابلاغ الکترونیک برگ قطعی], [شناسه حسابرسی],
            [شماره برگ تشخیص], [تاریخ صدور برگ تشخیص (تاریخ تایید گزارش حسابرسی)], [تاريخ ايجاد برگ دعوت بدوی],
            [تاريخ ابلاغ برگ دعوت بدوی], [تاریخ ابلاغ رای تجدید نظر], [توافق], [تاریخ تایید رای],
            [تاریخ ابلاغ برگ تشخیص], [تاريخ ايجاد برگ قطعي], [شماره برگ قطعی], [تاریخ ابلاغ برگ قطعی],
            [سال عملکرد],[نام اداره], [کد اداره], [آخرین وضعیت]
            FROM [testdbV2].[dbo].[V_PORTAL]  
            """


def get_sql_mashaghelsonati_tashkhisEblaghNashode():
    return """
            select 
case
when  mm.cod_hozeh  =168141 then 'هويزه'
 when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1601 then 'اهواز کد يک'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1602 then 'اهواز کد دو'
 when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,5) =16439 then 'هنديجان'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,5) =16649 then 'لالي'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,5) =16718 then 'رامشير'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1603 then 'اهواز کد سه'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1604 then 'اهواز کد چهار '
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1605 then 'اهواز کد پنج'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1606 then 'اهواز کد شش'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1607 then 'اهواز کد هفت'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1608 then 'اهواز کد هشت'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1609 then 'اهواز کد نه'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1610 then 'اهواز کد 10'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1611 then 'اهواز کد 11'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1615 then 'اهواز کد 15'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1631 then 'آبادان کد 31'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1632 then 'آبادان کد 32'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1633 then 'آبادان کد 33'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1626 then 'دزفول'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1627 then 'دزفول'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1628 then 'دزفول'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1643 then 'ماهشهر کد43'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1644 then 'ماهشهر کد44'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1636 then 'بهبهان'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1639 then 'شوشتر'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1641 then 'گتوند'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1648 then 'بندر امام'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1649 then 'بندر امام'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1651 then 'انديمشک'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1654 then 'خرمشهر'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1659 then 'شادگان'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1661 then 'اميديه'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1663 then 'آغاجاري'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1664 then 'مسجد سليمان'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1668 then 'شوش'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1671 then 'رامهرمز'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1676 then 'ايذه'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1678 then 'هفتگل'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1679 then 'باغملک'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1681 then 'دشت آزاداگان'
else 'نامشخص' end as 'شهرستان'
,
case 
 when mm.cod_hozeh  IN (168141) then '168141'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,5) IN ('16439','16649','16718') then SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,5) else SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) end as 'کد اداره'
,
mm.Cod_Hozeh as 'کد حوزه',
mm.sal as 'عملکرد',
mm.K_Parvand as 'کلاسه پرونده',
TA.motamam AS 'متمم',
modi_inf.namee AS 'نام',
modi_inf.family AS 'نام خانوادگي',
modi_inf.cod_meli AS 'کد ملی',
modi_inf.modi_seq AS 'شماره مودی',
ta.asl_maliat as 'ماليات تشخيص' ,
ta.dar_maliat_mash as 'درآمد تشخيص',
ta.sabt_date as 'تاريخ ثبت برگ تشخيص'
,ta.sabt_no as 'شماره برگ تشخیص'
--hh.sal as 'عملکرد' ,hh.cod_hozeh as 'کد حوزه' , hh.k_parvand as 'کلاسه پرونده' , ta.motamam as 'متمم', namee as 'نام' , family as 'نام خانوادگي'
 from ghabln_inf hh inner join KMLink_Inf mm on hh.Sal=mm.sal and hh.cod_hozeh=mm.Cod_Hozeh and hh.k_parvand=mm.K_Parvand
inner join modi_inf on modi_inf.modi_seq=mm.Modi_Seq
inner join tashkhis_inf ta on ta.sal=mm.sal and ta.cod_hozeh=mm.Cod_Hozeh and ta.k_parvand =mm.K_Parvand and ta.modi_seq=mm.Modi_Seq
where
 (CONNECTIONPROPERTY('local_net_address' )!= '10.53.32.130' or (CONNECTIONPROPERTY('local_net_address' )= '10.53.32.130' and hh.cod_hozeh !=168141))
and
 (CONNECTIONPROPERTY('local_net_address' )!= '10.53.48.130' or (CONNECTIONPROPERTY('local_net_address' )= '10.53.48.130' and hh.cod_hozeh =168141))
and
LEN(ta.sabt_date)>2 and (len (Eblagh_Sabt_Date) <2  or Eblagh_Sabt_Date is null  or Eblagh_Sabt_Date ='')
--(LEN(ta.eblag_date)<2 or (ta.eblag_date is null) )
and  Vaziat_Falie not in(2) and
not exists (select cod_hozeh from adam_faliat_inf ta where ta.cod_hozeh=hh.cod_hozeh and ta.sal=hh.Sal and ta.k_parvand=hh.k_parvand and len(ta.sabt_no)>1  and isnull(ebtal, 0) = 0 )
and
not exists (select cod_hozeh from tashkhis_inf taaa where taaa.cod_hozeh=hh.cod_hozeh and taaa.sal=hh.Sal and taaa.k_parvand=hh.k_parvand and len(taaa.sabt_date)>1  and taaa.modi_seq=mm.modi_seq  and ta.motamam=taaa.motamam and len(taaa.eblag_date)>2)
and ta.serial=(select max(serial)from tashkhis_inf where ta.cod_hozeh=cod_hozeh and ta.k_parvand=k_parvand and ta.sal=sal and ta.motamam=motamam and ta.modi_seq=modi_seq)
and hh.sal in (1393,1394,1395)
"""


def get_sql_mashaghelsonati_ghateeEblaghNashode():

    return """     
select 
case
when  mm.cod_hozeh  =168141 then 'هويزه'
 when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1601 then 'اهواز کد يک'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1602 then 'اهواز کد دو'
 when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,5) =16439 then 'هنديجان'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,5) =16649 then 'لالي'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,5) =16718 then 'رامشير'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1603 then 'اهواز کد سه'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1604 then 'اهواز کد چهار '
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1605 then 'اهواز کد پنج'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1606 then 'اهواز کد شش'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1607 then 'اهواز کد هفت'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1608 then 'اهواز کد هشت'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1609 then 'اهواز کد نه'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1610 then 'اهواز کد 10'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1611 then 'اهواز کد 11'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1615 then 'اهواز کد 15'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1631 then 'آبادان کد 31'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1632 then 'آبادان کد 32'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1633 then 'آبادان کد 33'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1626 then 'دزفول'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1627 then 'دزفول'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1628 then 'دزفول'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1643 then 'ماهشهر کد43'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1644 then 'ماهشهر کد44'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1636 then 'بهبهان'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1639 then 'شوشتر'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1641 then 'گتوند'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1648 then 'بندر امام'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1649 then 'بندر امام'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1651 then 'انديمشک'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1654 then 'خرمشهر'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1659 then 'شادگان'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1661 then 'اميديه'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1663 then 'آغاجاري'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1664 then 'مسجد سليمان'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1668 then 'شوش'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1671 then 'رامهرمز'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1676 then 'ايذه'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1678 then 'هفتگل'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1679 then 'باغملک'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1681 then 'دشت آزاداگان'
else 'نامشخص' end as 'شهرستان'
,
case 
when mm.cod_hozeh IN (168141) then '168141'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,5) IN ('16439','16649','16718') then SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,5) else SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) end as 'کد اداره'
,
mm.Cod_Hozeh as 'کد حوزه',
mm.sal as 'عملکرد',
mm.K_Parvand as 'کلاسه پرونده',
TA.motamam AS 'متمم',
modi_inf.namee AS 'نام',
modi_inf.family AS 'نام خانوادگي',
modi_inf.cod_meli AS 'کد ملی',
modi_inf.modi_seq AS 'شماره مودی',
ta.asl_maliat as 'ماليات قطعي' ,
ta.dar_maliat_mash as 'درآمد قطعي',
ta.sabt_date as 'تاريخ ثبت برگ قطعي'
,ta.sabt_no as 'شماره برگ قطعی' 
 from ghabln_inf hh inner join KMLink_Inf mm on hh.Sal=mm.sal and hh.cod_hozeh=mm.Cod_Hozeh and hh.k_parvand=mm.K_Parvand
inner join modi_inf on modi_inf.modi_seq=mm.Modi_Seq
inner join ghatee_inf ta on ta.sal=mm.sal and ta.cod_hozeh=mm.Cod_Hozeh and ta.k_parvand =mm.K_Parvand and ta.modi_seq=mm.Modi_Seq
where
 (CONNECTIONPROPERTY('local_net_address' )!= '10.53.32.130' or (CONNECTIONPROPERTY('local_net_address' )= '10.53.32.130' and hh.cod_hozeh !=168141))
and
 (CONNECTIONPROPERTY('local_net_address' )!= '10.53.48.130' or (CONNECTIONPROPERTY('local_net_address' )= '10.53.48.130' and hh.cod_hozeh =168141))
and
LEN(ta.sabt_date)>2 and --(LEN(ta.eblag_date)<2 or (ta.eblag_date is null) ) and
 (len (Eblagh_Sabt_Date) <2  or Eblagh_Sabt_Date is null  or Eblagh_Sabt_Date ='')
and  Vaziat_Falie not in(2) and
not exists (select cod_hozeh from adam_faliat_inf ta where ta.cod_hozeh=hh.cod_hozeh and ta.sal=hh.Sal and ta.k_parvand=hh.k_parvand and len(ta.sabt_no)>1  and isnull(ebtal, 0) = 0 )
and
not exists (select cod_hozeh from ghatee_inf taaa where taaa.cod_hozeh=hh.cod_hozeh and taaa.sal=hh.Sal and taaa.k_parvand=hh.k_parvand and len(taaa.sabt_date)>1  and taaa.modi_seq=mm.modi_seq  and ta.motamam=taaa.motamam and len(taaa.Eblagh_Sabt_Date)>2)
and ta.serial=(select max(serial)from ghatee_inf where ta.cod_hozeh=cod_hozeh and ta.k_parvand=k_parvand and ta.sal=sal and ta.motamam=motamam and ta.modi_seq=modi_seq)
and hh.sal in (1393,1394,1395)
"""


def get_sql_alleterazat(index1=None, index2=None):

    if index1 is None:

        return """

    SELECT
        cr01_internal_id,
        current_office,
        ca02_tax_year,
        ca02_return_id,
        ca02_return_version,
        ca09_tax_type_code,
        cstd_return_type,
        co01_request_no,
        cstd_request_status,
        cstd_request_type,
        co01_request_ref_num,
        cstd_request_ref_type,
        co01_request_ref_date,
        co01_pre_notice_date,
        cstd_withdraw_status,
        co01_withdrawal_to,
        co01_withdraw_received_on,
        co01_request_received_on,
        max_co03_id,
        co03_hearing_date,
        cstd_hearing_status,
        co03_outcome,
        rulling_date,
        rulling_approved,
        closed_date,
        assign_to_name,
        tp_agree,
        inta_attendee_name,
        inta_attendee_type,
        co03_inta_attendee,
        co01_previous_request_no,
        previous_request_type,
        ta_agree,
        agree,
        gharar,
        co03_decision_deadline,
        tadil,
        raf_taroz,
        taid,
        rulling_performer_name,
        co03_decision_assignee1,
        co03_decision_assignee1_name,
        cstd_letter_type_rrn,
        article_169,
        max_co01_request_no,
        cs10_id_assign_to_name,
        cs10_id_inta_attendee_name,
        national_id_ruling_prfrmr_name,
        cs10_id_decision_assigne1_name,
        min_co03_id,
        gharar_reg_cmpl_date,
        max_co03_id_has_otcm,
        outcome_max_co03_id_has_otcm,
        min_co03_hearing_date,
        movafeghat_gharar_sadere_date,
        ca02_return_version_dest,
        cc03_id_ruling_letter,
        co03_decision,
        co01_portal_channel,
        gto_id
    FROM
        reports.v_obj60_khozestan

    """
    else:
        ...


def get_sql_allcodes():
    return """
SELECT
    group_code,
    internal_code,
    code_desc
FROM
    reports.s200_codes_22
"""


def get_sql_allpayment():
    return """
SELECT
    ct01_amount,
    ct01_tax_year,
    cstd_tax_type,
    ct01_period,
    cashdate,
    cstd_liability_type,
    cstd_tran_type,
    taxpayer_name,
    cr01_internal_id,
    gto_id,
    office_full_name,
    cs04_id,
    cs04_name,
    cn08_inta_slip_id,
    cn08_inta_payment_id,
    cr01_tin_id,
    cn08_pmt_instrument_date,
    cn08_create_date
FROM
    reports.v_payment_khozestan
"""


def get_sql_allhesabrasi():
    return """

SELECT
    cr01_internal_id,
    current_office,
    ca02_tax_year,
    ca02_return_id,
    ca02_return_version,
    ca09_tax_type_code,
    cstd_return_type,
    max_cu05_id,
    min_cu05_id,
    count_cu05_id,
    cstd_audit_status,
    cu05_audit_review_flag,
    cu05_audit_type,
    cu05_risk_band,
    cu05_report_num,
    cu05_createdate,
    cu05_rpt_approved_date,
    auditor_name1,
    auditor_name2,
    davat_number,
    davat_sent_date,
    davat_create_date,
    davat_eblagh_date,
    max_case_refrence,
    min_case_refrence,
    max_stage,
    ce06_case_type_id,
    ce02_date_created,
    cstd_case_status,
    ce02_case_status_date,
    general_closure_date,
    general_close_date,
    review_closure_date,
    review_close_date,
    closure_97_date,
    close_97_date,
    closure_evasion_date,
    close_evasion_date,
    auditor_name,
    closure_date,
    closed_date,
    cu05_startdate,
    soratmajles_num,
    end_aud_report_num,
    general_step5_date,
    review_step5_date,
    step5_97_date,
    step5_evasion_date,
    step5_date,
    cu05_invitation_type,
    aditor_name_case,
    cs10_national_id,
    aditor_name_asli,
    cs10_national_id_asli,
    gto_id
FROM
    reports.v_aud35_khozestan

"""


def get_sql_sanimusers(index1=None, index2=None, tblname=None, ostan_txt='_khozestan'):

    if index1 is None:

        return """
        
    SELECT
       *
    FROM
        reports.%s%s
        WHERE ROWNUM = 1
        """ % (tblname, ostan_txt)

    else:

        return """
    SELECT
        *
    FROM
    (SELECT a.*, ROWNUM rnum
    FROM (SELECT * FROM reports.%s%s) a where ROWNUM <= %s)
        
    where rnum >= %s
    """ % (tblname, ostan_txt, index2, index1)


def get_sql_allejra():

    return """
    SELECT
    cr01_internal_id,
    ca02_return_id,
    ce02_case_reference,
    ce02_case_stage,
    cstd_case_status,
    ce02_case_status_date,
    ce06_case_type_id,
    ce02_date_created,
    ce02_received_on_date,
    cs10_allocated_to_user,
    allocated_name,
    cstd_entity,
    ce22_entity_id,
    ca02_tax_year,
    ca02_tax_period,
    ca09_tax_type_code,
    date_makhtoome_vosool_ejra,
    gto_id
FROM
    reports.v_caseejra_khozestan
    
    """


def get_sql_allsbtenam():

    return """



"""


# def get_sql_allsanim():

#     return """
#      SELECT
#     cr01_internal_id,
#     cr01_tin_id,
#     tpname,
#     cr04_father_name,
#     cr01_act_start_date,
#     address,
#     cr25_fixed_phone,
#     cr13_trade_name,
#     cstd_taxpayer_size,
#     cr04_birth_date,
#     cr12_national_id,
#     cstd_gender,
#     cstd_legal_entity_type,
#     cr01_natural_per_flag,
#     cstd_taxpayer_status,
#     cr01_follow_code,
#     isic_code_level_5,
#     desc_activity_level_5,
#     gto_id,
#     directorate,
#     office,
#     cs04_name,
#     tax_return_no,
#     year,
#     ca09_tax_type_code,
#     ca02_tax_period,
#     taxreturn_date,
#     cstd_filing_channel,
#     ca02_due_date,
#     authorise_no,
#     authorise_date,
#     invitation_date,
#     inv_rec_date,
#     aud_soratmajles_date,
#     assess_date,
#     ass_rec_date,
#     co01_request_received_on,
#     cstd_request_type,
#     co03_hearing_date,
#     co03_outcome,
#     rulling_date,
#     rulling_approved,
#     closed_date,
#     assign_to_name,
#     cs10_id_assign_to_name,
#     inta_attendee_name,
#     cs10_id_inta_attendee_name,
#     inta_attendee_type,
#     agree,
#     gharar,
#     tadil,
#     raf_taroz,
#     taid,
#     rulling_performer_name,
#     national_id_ruling_prfrmr_name,
#     badvi_davat_create_date,
#     obj1_result_date,
#     tjdid_davat_create_date,
#     obj2_result_date,
#     final_date,
#     taxreturn_income,
#     taxreturn_tax,
#     assess_income,
#     assess_tax,
#     daramad_eteraz,
#     obj1_result_income,
#     obj2_result_income,
#     final_income,
#     final_tax,
#     taligh_adamfaal,
#     manba_sabtnam,
#     vaziyat_manba_sabtnam,
#     zian,
#     ejra_band3_num,
#     ejra_band3_create_date,
#     ejra_band3_eblagh_date,
#     tashkhis_num,
#     tashkhis_num_169,
#     performer_tash,
#     performer_name_tash,
#     eblagh_type_tash,
#     electronic_notification_tash,
#     performer_tashkhis_num,
#     tamkin_num,
#     tamkin_create_date,
#     eteraz_request_num,
#     eteraz_ray_approve_date,
#     eteraz_namayande_name,
#     cs10_id_eteraz_namayande_name,
#     eteraz_namayande_semat,
#     tavafogh,
#     eteraz_ray_eblagh_date,
#     badvi_meeting_date,
#     badvi_davat_eblagh_date,
#     badvi_ray_eblagh_date,
#     tajdid_meeting_date,
#     tjdid_davat_eblagh_date,
#     tajdid_ray_eblagh_date,
#     ghati_num,
#     performer_ghat,
#     performer_name_ghat,
#     eblagh_type_ghat,
#     electronic_notification_ghat,
#     performer_ghati_num,
#     ghati_eblagh_date,
#     maliyat_eteraz,
#     daramat_eteraz_kasr,
#     maliat_badvi,
#     daramad_badvi_kasr,
#     maliyat_tajdid,
#     daramad_tajdid_kasr,
#     barge_ejra_num,
#     performer_ejra,
#     performer_name_ejra,
#     eblagh_type_ejra,
#     electronic_notification_ejra,
#     barge_ejra_create_date,
#     barge_ejra_delivered_date,
#     ret_pstd,
#     ret_pstd_source,
#     ret_pstd_source_desc,
#     hamarz_request_num,
#     hamarz_meeting_date,
#     hamarz_ray_approve_date,
#     hamarz_davat_create_date,
#     hamarz_davat_eblagh_date,
#     hamarz_ray_eblagh_date,
#     mokarar251_request_num,
#     mokarar251_meeting_date,
#     mokarar251_ray_approve_date,
#     mokarar251_davat_create_date,
#     mokarar251_davat_eblagh_date,
#     mokarar251_ray_eblagh_date,
#     mokarar169_request_num,
#     mokarar169_meeting_date,
#     mokarar169_ray_approve_date,
#     mokarar169_davat_create_date,
#     mokarar169_davat_eblagh_date,
#     mokarar169_ray_eblagh_date,
#     maliyat_hamarz,
#     daramad_hamraz,
#     daramad_hamarz_kasr,
#     maliyat_mokarar251,
#     daramad_mokarar251,
#     daramad_mokarar251_kasr,
#     maliyat_mokarar169,
#     daramad_mokarar169,
#     daramad_mokarar169_kasr,
#     ca02_tax_due,
#     request_received_on_badvi,
#     request_received_on_tajdid,
#     request_received_on_hamarz,
#     request_received_on_nokarar251,
#     request_received_on_mokarar169,
#     maliyat_eslah_sazman,
#     daramad_eslah_sazman,
#     daramad_eslah_sazman_kasr,
#     decision_assignee_badvi,
#     cs10_id_decision_asigne_badvi,
#     decision_assignee_tajdid,
#     cs10_id_decision_asigne_tajdid,
#     cstd_request_channel,
#     cstd_nationality,
#     cstd_reg_country,
#     cr10_old_reg_number,
#     cr19_area_code,
#     rulling_date_to,
#     ruling_desc_to,
#     rulling_date_b1,
#     ruling_desc_b1,
#     min_txp_ca02_status_mod_date,
#     ca02_external_id_txp_max,
#     cstd_return_type,
#     auditor_name,
#     badvi_request_num,
#     tajdid_request_num,
#     manba_ghatiat,
#     ca02_parcel_no,
#     ebrazi_sale,
#     tashkhis_sale,
#     ghati_sale,
#     ebrazi_avarez,
#     tashkhis_avarez,
#     eteraz_avarez,
#     badvi_avarez,
#     tajdid_avarez,
#     ghati_avarez,
#     bakhshodegi,
#     ebrazi_etebar
# FROM
#     reports.v_portal_khozestan"""

def get_sql_allsanim():

    return """
     SELECT
    *
    FROM
    reports.v_portal_khozestan
     where  year>=1396
     """


def get_sql_allbakhshodegi():

    return """
    SELECT
        cr01_internal_id,
        ct01_posting_date,
        ct01_period,
        ct01_tax_year,
        cstd_tax_type,
        ct01_amount,
        cstd_liability_type,
        cstd_tran_type,
        ct01_desc,
        cstd_entity,
        gto_id
    FROM
        reports.v_impunity_khozestan

"""


def get_sql_allareasusp():

    return """
    SELECT
        ca02_return_id,
        aud_suspection_auditor,
        aud_suspection_description,
        cs10_id_aud_suspection_auditor,
        gto_id
    FROM
        reports.v_areasusp_khozestan

"""


def get_sql_alltshsmtm():

    return """
    SELECT
    cr01_internal_id,
    current_office,
    ca02_return_id,
    ca02_return_version,
    ca02_tax_year,
    ca09_tax_type_code,
    motamam_num,
    motamam_create_date,
    motamam_eblagh_date,
    motamam_eblagh,
    maliyat_motamam,
    daramad_motamam,
    daramad_motamam_kasr,
    avarez_vat,
    sale,
    cu05_id,
    auditor_name,
    cu05_createdate,
    eblagh_type,
    gto_id,
    eteraz_request_num,
    eteraz_meeting_date,
    eteraz_ray_approve_date,
    tavafogh,
    taid_eteraz,
    raf_taroz_eteraz,
    tadil_eteraz,
    eteraz_ray_eblagh_date,
    co03_outcome_eteraz,
    tamkin_num,
    tamkin_create_date,
    badvi_request_num,
    badvi_meeting_date,
    badvi_ray_approve_date,
    badvi_davat_create_date,
    badvi_davat_eblagh_date,
    taid_badvi,
    raf_taroz_badvi,
    tadil_badvi,
    badvi_ray_eblagh_date,
    co03_outcome_badvi,
    tajdid_request_num,
    tajdid_meeting_date,
    tajdid_ray_approve_date,
    tjdid_davat_create_date,
    tjdid_davat_eblagh_date,
    taid_tajdidnazar,
    raf_taroz_tajdidnazar,
    tadil_tajdidnazar,
    tajdid_ray_eblagh_date,
    co03_outcome_tajdidnazar,
    shora_request_num,
    shora_meeting_date,
    shora_ray_approve_date,
    shora_davat_create_date,
    shora_davat_eblagh_date,
    taid_shora,
    raf_taroz_shora,
    tadil_shora,
    shora_ray_eblagh_date,
    co03_outcome_shora,
    cstd_letter_type
FROM
    v_tshsmtm_khozestan

"""


def get_sql_allhesabdari():

    return """

SELECT
    ca02_return_id,
    is_169,
    remaind_all,
    remaind_tax,
    remaind_duty,
    remaind_other,
    remaind_pen_tax,
    remaind_pen_duty,
    remaind_169,
    remaind_169r,
    tax_assesment,
    pay_va_rpp,
    tax_tax,
    tax_duty,
    tax_other,
    pen_tax,
    pen_duty,
    pen_other,
    pen_169,
    pen_169r,
    intres,
    intresd,
    pay_tax,
    rpp_tax,
    pay_duty,
    rpp_duty,
    pay_other,
    rpp_other,
    pay_pen_tax,
    rpp_pen_tax,
    pay_pen_duty,
    rpp_pen_duty,
    pay_pen_169,
    rpp_pen_169,
    pay_pen_169r,
    rpp_pen_169r,
    pay_intres,
    rpp_intres,
    pay_intresd,
    rpp_intresd,
    gto_id
FROM
    reports.v_ehmoadi_khozestan

"""


def get_sql_allanbare():

    return """

SELECT
    cr01_tin_id,
    cr01_internal_id,
    cstd_audit_status,
    cu05_auditor,
    gto_id
FROM
    reports.v_audpool_khozestan

"""


def get_sql_sanimDarjariabBadvi():

    return """

    SELECT [شماره اقتصادی],[نام اداره],obj.* FROM
    (SELECT  [ID]
        ,[شناسه داخلی سامانه سنیم]
        ,[اداره فعلي]
        ,[سال عملکرد اظهارنامه]
        ,[شماره اظهارنامه]
        ,[آخرين ورژن POSTED اظهارنامه]
        ,[کد منبع مالياتي اظهارنامه]
        ,[کد نوع اظهارنامه]
        ,[شماره درخواست اعتراض / شکایت]
        ,[کد وضعيت درخواست اعتراض / شکایت]
        ,[کد نوع درخواست اعتراض / شکایت]
        ,[شماره منبع درخواست اعتراض / شکایت]
        ,[نوع قرار / نوع برگه ای که به آن اعتراض / شکایت شده است]
        ,[تاریخ منبع درخواست اعتراض / شکایت]
        ,[CO01_PRE_NOTICE_DATE]
        ,[کد وضعيت بازپس گیری]
        ,[CO01_WITHDRAWAL_TO]
        ,[تاريخ دريافت بازپس گيري]
        ,[تاريخ دريافت درخواست اعتراض / شکايت]
        ,[بیشترین شناسه ی ثبت شده ی جلسه استماع به ازای یک شماره درخواست اعتراض / شکایت]
        ,[تاريخ برگزاري جلسه استماع (آخرین جلسه استماع)]
        ,[کد وضعیت برگزاري جلسه استماع (آخرین جلسه استماع)]
        ,[کد نتیجه جلسه استماع (آخرین جلسه استماع)]
        ,[تاريخ رای]
        ,[تاريخ تاييد رای]
        ,[تاريخ بسته شدن اعتراض / شکايت]
        ,[نام کارمندی که کيس اعتراض به او تخصیص داده شده]
        ,[TP_AGREE]
        ,[نام نماينده سازمان]
        ,[سمت نماينده سازمان]
        ,[کد نماينده سازمان (آخرین جلسه استماع)]
        ,[شماره کيس اعتراض / شکایت قبلی]
        ,[نوع کيس اعتراض / شکایت قبلی]
        ,[TA_AGREE]
        ,[توافق ]
        ,[قرار ]
        ,[مهلت اجرای قرار]
        ,[تعديل ]
        ,[رفع تعرض]
        ,[تاييد]
        ,[نام فرد تاييد کننده رای]
        ,[نام کاربری مجری قرار ]
        ,[نام و نام خانوادگی مجری قرار ]
        ,[کد نامه منبع درخواست اعتراض]
        ,[آیا اعتراض / شکایت مربوط به ماده 169 هست یا عادی]
        ,[MAX_CO01_REQUEST_NO]
        ,[کد کاربری کارمندی که کيس اعتراض به او تخصیص داده شده]
        ,[کد کاربری نماينده سازمان]
        ,[کد کاربری  تاييد کننده رای]
        ,[کد کاربری مجری قرار]
        ,[کمترین شناسه ی ثبت شده ی جلسه استماع به ازای یک شماره درخواست اعتراض / شکایت]
        ,[تاریخ ثبت تکمیل اجرای قرار]
        ,[آخرین جلسه استماع دارای نتیجه، به ازای یک شماره درخواست اعتراض / شکایت]
        ,[نتیجه ی جلسه ی استماع (آخرین جلسه استماع دارای نتیجه، به ازای یک شماره درخواست اعتراض/شکایت)]
        ,[تاریخ کمترین شناسه ی ثبت شده ی جلسه استماع به ازای یک شماره درخواست اعتراض / شکایت]
        ,[تاریخ موافقت با قرار صادره جلسه استماع]
        ,[CA02_RETURN_VERSION_DEST]
        ,[CC03_ID_RULING_LETTER]
        ,[CO03_DECISION]
        ,[CO01_PORTAL_CHANNEL]
        ,[کد اداره کل]
        ,[تاریخ بروزرسانی]
    FROM [TestDb].[dbo].[V_OBJ60] 
    WHERE [کد نوع درخواست اعتراض / شکایت]='02' and [کد وضعيت درخواست اعتراض / شکایت]<>'OBJ_CPLT' 
    and [کد وضعيت درخواست اعتراض / شکایت]<>'OBJ_CLSD'
    and [تاريخ برگزاري جلسه استماع (آخرین جلسه استماع)]='NaT')
    as obj

    LEFT JOIN

    (SELECT DISTINCT [کد یکتای داخلی مؤدی],[شماره اقتصادی], [نام اداره]
    FROM [dbo].[V_PORTAL]) as portal ON obj.[شناسه داخلی سامانه سنیم]=portal.[کد یکتای داخلی مؤدی]

"""


def get_sql_mashaghelsonati_tashkhisEblaghNoGhatee():

    return """

select 
case
when  mm.cod_hozeh  =168141 then 'هويزه'
 when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1601 then 'اهواز کد يک'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1602 then 'اهواز کد دو'
 when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,5) =16439 then 'هنديجان'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,5) =16649 then 'لالي'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,5) =16718 then 'رامشير'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1603 then 'اهواز کد سه'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1604 then 'اهواز کد چهار '
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1605 then 'اهواز کد پنج'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1606 then 'اهواز کد شش'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1607 then 'اهواز کد هفت'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1608 then 'اهواز کد هشت'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1609 then 'اهواز کد نه'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1610 then 'اهواز کد 10'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1611 then 'اهواز کد 11'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1615 then 'اهواز کد 15'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1631 then 'آبادان کد 31'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1632 then 'آبادان کد 32'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1633 then 'آبادان کد 33'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1626 then 'دزفول'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1627 then 'دزفول'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1628 then 'دزفول'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1643 then 'ماهشهر کد43'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1644 then 'ماهشهر کد44'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1636 then 'بهبهان'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1639 then 'شوشتر'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1641 then 'گتوند'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1649 then 'بندر امام'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1648 then 'بندر امام'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1651 then 'انديمشک'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1654 then 'خرمشهر'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1659 then 'شادگان'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1661 then 'اميديه'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1663 then 'آغاجاري'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1664 then 'مسجد سليمان'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1668 then 'شوش'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1671 then 'رامهرمز'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1676 then 'ايذه'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1678 then 'هفتگل'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1679 then 'باغملک'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) =1681 then 'دشت آزاداگان'
else 'نامشخص' end as 'شهرستان'
,
case 
 when mm.cod_hozeh  IN (168141) then '168141'
when SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,5) IN ('16439','16649','16718') then SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,5) else SUBSTRING( cast (mm.cod_hozeh as varchar(20) ),1,4) end as 'کد اداره'
,
mm.Cod_Hozeh as 'کد حوزه',
mm.sal as 'عملکرد',
mm.K_Parvand as 'کلاسه پرونده',
TA.motamam AS 'متمم',
modi_inf.namee AS 'نام',
modi_inf.family AS 'نام خانوادگي',
modi_inf.cod_meli AS 'کد ملی',
modi_inf.modi_seq AS 'شماره مودی',
ta.asl_maliat as 'ماليات تشخيص' ,
ta.dar_maliat_mash as 'درآمد تشخيص',
ta.sabt_date as 'تاريخ ثبت برگ تشخيص'
,ta.eblag_date as 'تاريخ ابلاغ برگ تشخيص'
,ta.Eblagh_Sabt_Date as 'تاريخ ثبت ابلاغ برگ تشخيص'
,ta.[sabt_no] as 'شماره برگ تشخیص'
 from ghabln_inf hh inner join KMLink_Inf mm on hh.Sal=mm.sal and hh.cod_hozeh=mm.Cod_Hozeh and hh.k_parvand=mm.K_Parvand
inner join modi_inf on modi_inf.modi_seq=mm.Modi_Seq
inner join tashkhis_inf ta on ta.sal=mm.sal and ta.cod_hozeh=mm.Cod_Hozeh and ta.k_parvand =mm.K_Parvand and ta.modi_seq=mm.Modi_Seq
where
 (CONNECTIONPROPERTY('local_net_address' )!= '10.53.32.130' or (CONNECTIONPROPERTY('local_net_address' )= '10.53.32.130' and hh.cod_hozeh !=168141))
and
 (CONNECTIONPROPERTY('local_net_address' )!= '10.53.48.130' or (CONNECTIONPROPERTY('local_net_address' )= '10.53.48.130' and hh.cod_hozeh =168141))
and
LEN(ta.sabt_date)>2 and len (Eblagh_Sabt_Date) >2  
 
and ta.serial=(select max(serial)from tashkhis_inf where ta.cod_hozeh=cod_hozeh and ta.k_parvand=k_parvand and ta.sal=sal and ta.motamam=motamam and ta.modi_seq=modi_seq)
and hh.sal >=1389
and Not exists (select ghatee_inf.cod_hozeh from ghatee_inf where ta.cod_hozeh=cod_hozeh and ta.k_parvand=k_parvand and ta.sal=sal and ta.motamam=motamam and ta.modi_seq=modi_seq and LEN(ghatee_inf.sabt_date)>2 )
 

"""


def get_sql_arzeshafzoodeSonati():
    return


"""
SELECT [tblArzeshAfzoodeSonati].*, tblSabtenamArzeshAfzoode.[کدرهگیری] as [کدرهگیری ثبت نام]
  FROM [TestDb].[dbo].[tblArzeshAfzoodeSonati] 
  join
  [TestDb].[dbo].[tblSabtenamArzeshAfzoode] 
  on tblArzeshAfzoodeSonati.[شماره پرونده] = tblSabtenamArzeshAfzoode.[شناسه]
"""


def get_sql_residegi99():
    return """
    SELECT 
    sanim.edare AS [EdareSanim]
    ,Ghast.[کد اداره امور مالیاتی] AS [EdareGhasht]
    ,iif(sanim.edare IS NULL,Ghast.[کد اداره امور مالیاتی],sanim.edare)AS [ادارا منتخب]
    ,Eghtesadi.*
    FROM
    (SELECT * FROM
    [dbo].[tblSabtenamCodeEghtesadi]
    WHERE [وضعیت فعالیت] = 'True'
    AND ([وضعیت ثبت نام]=44 OR [وضعیت ثبت نام]=45)
    AND [تاریخ شروع فعالیت] BETWEEN '13000101' AND '13961229'
    AND [کد رهگیری] NOT IN (SELECT [کد رهگيري ثبت نام] FROM [TestDb].[dbo].[tblEzhar96Haghighi])
    AND [شناسه ملی] NOT IN (SELECT [شناسه ملي/کدملي] FROM [TestDb].[dbo].[tblEzhar96Haghighi])
    AND [شناسه ملی] NOT IN (SELECT [شناسه ملی]  FROM [TestDb].[dbo].[tblEzhar96Hoghoghi])
    AND [کد رهگیری] NOT IN (
                            SELECT [کد رهگيري ثبت نام ] FROM [dbo].[V_PORTAL]
                            WHERE 
                            LEFT([سال عملکرد],4)='1396' 
                            AND ([منبع مالیاتی]='ITXB' OR [منبع مالیاتی]='ITXC')
                            AND ((LEN([تاریخ صدور برگ تشخیص (تاریخ تایید گزارش حسابرسی)])>=10) OR  (LEN([تاريخ ايجاد برگ قطعي])>=10))
                            )
                            ) AS Eghtesadi
    LEFT JOIN
    (SELECT 
    DISTINCT 
    [کد رهگيري ثبت نام ]
    ,[نام اداره]
    ,[کد اداره]
    ,tax.dbo.tblEdareShahr.edare 
    FROM [dbo].[V_PORTAL]
    LEFT JOIN 
    tax.dbo.tblEdareShahr
    ON tax.dbo.tblEdareShahr.IrisCode=[کد اداره] collate Arabic_100_CI_AS
    ) as sanim
    ON Eghtesadi.[کد رهگیری]=sanim.[کد رهگيري ثبت نام ]
    LEFT JOIN
    (SELECT [گشت پستی],[کد اداره امور مالیاتی] FROM [dbo].[tblGashtPosti] WHERE [گشت پستی] NOT IN
    (SELECT [گشت پستی] FROM [dbo].[tblGashtPosti] GROUP BY [گشت پستی] HAVING COUNT(*)>1)) Ghast
    ON 
    Left(Eghtesadi.[کد پستی],5)=Ghast.[گشت پستی]

    """


def get_sql_noresidegi_arzeshafoodehSanim():
    return """
        SELECT
    [شماره اقتصادی]
    ,replace(REPLACE(REPLACE([نام مودی], CHAR(13), ''), CHAR(10), ''),'	','') AS [نام مودی]
    ,[کد رهگيري ثبت نام ]
    ,[کد اداره]
    ,[نام اداره]
    ,[شماره اظهارنامه]
    ,[سال عملکرد]
    ,CASE [منبع مالیاتی]  WHEN 'VAT' THEN N'مالیات بر ارزش افزوده' end AS [منبع مالیاتی]
    ,[دوره عملکرد]
    ,[تاریخ صدور برگ تشخیص (تاریخ تایید گزارش حسابرسی)]
    ,[تاریخ ابلاغ برگ تشخیص]
    ,[تاريخ ايجاد برگ قطعي]
    FROM [TestDb].[dbo].[V_PORTAL]
    where 
    ([منبع مالیاتی]='VAT')
    and [تاریخ صدور برگ تشخیص (تاریخ تایید گزارش حسابرسی)]='None'
    and [تاريخ ايجاد برگ قطعي]='NONE' 
    and substring([سال عملکرد],1,4) between '1396' and '1399'
    """


def get_sql_arzeshAfzoodeSonatiV2():
    return """
    SELECT iif([واحد مالیات برارزش افزوده]='nan',[اداره گشت پستی],[واحد مالیات برارزش افزوده])AS [ادارا منتخب],* FROM
    (SELECT tblmain.*,Ghast.[کد اداره امور مالیاتی] as [اداره گشت پستی] FROM 
    (SELECT tblarzesh.*,Eghtesadi.[کد پستی],SUBSTRING(Eghtesadi.[کد پستی],1,5) AS [gasht] FROM 
    (SELECT [tblArzeshAfzoodeSonati].*, tblSabtenamArzeshAfzoode.[کدرهگیری] as [کدرهگیری ثبت نام]
    FROM  [TestDb].[dbo].[tblArzeshAfzoodeSonati] 
    LEFT join
    [TestDb].[dbo].[tblSabtenamArzeshAfzoode] 
    on tblArzeshAfzoodeSonati.[شماره پرونده] = tblSabtenamArzeshAfzoode.[شناسه] where [سال عملکرد] BETWEEN 1387 and 1395) AS tblarzesh
    LEFT JOIN 
    (SELECT DISTINCT [کد رهگیری],[کد پستی]     
    FROM [TestDb].[dbo].[tblSabtenamCodeEghtesadi]) AS Eghtesadi
    ON tblarzesh.[کدرهگیری ثبت نام]=Eghtesadi.[کد رهگیری]) AS tblMain
    LEFT JOIN
        (SELECT [گشت پستی],[کد اداره امور مالیاتی] FROM [dbo].[tblGashtPosti] WHERE [گشت پستی] NOT IN
    (SELECT [گشت پستی] FROM [dbo].[tblGashtPosti] GROUP BY [گشت پستی] HAVING COUNT(*)>1)) Ghast
    ON 
    tblMain.[gasht]=Ghast.[گشت پستی]) as a
"""


def get_sql_sanim_count(tblname, ostan_txt='_khozestan'):
    return """
SELECT
 count(*) , '%s'
FROM
    reports.%s%s 
""" % (tblname, tblname, ostan_txt)


def get_sql_mashaghelsonati_amadeghatee(azsal='1392', tasal='1395', date='14011109'):

    return """ SELECT * FROM (SELECT  
ghabln_inf.cod_hozeh  as 'کد حوزه' ,
ghabln_inf.k_parvand as 'کلاسه پرونده',
tajdid_nazar_inf.motamam AS 'متمم' ,
ghabln_inf.Sal as 'عملکرد',
namee as 'نام',
family as 'نام خانوادگي',
modi_inf.cod_meli as 'کد ملی',
modi_inf.modi_seq as 'شناسه مودی',
ttta.asl_maliat as 'ماليات تشخيص',
ttta.dar_maliat_mash as'درآمد مشمول مالیات',
ttta.sabt_date as 'تاریخ ثبت برگ تشخیص',
ttta.sabt_no as'شماره ثبت برگ تشخیص',
'رای صادره تجدیدنظر' as 'علت قطعی سازی' 
FROM 
ghabln_inf
inner join KMLink_Inf on
ghabln_inf.cod_hozeh=KMLink_Inf.Cod_Hozeh and
ghabln_inf.k_parvand=KMLink_Inf.K_Parvand and
ghabln_inf.Sal=KMLink_Inf.sal
inner join tajdid_nazar_inf on
tajdid_nazar_inf.cod_hozeh=KMLink_Inf.Cod_Hozeh and
tajdid_nazar_inf.k_parvand=KMLink_Inf.K_Parvand and
tajdid_nazar_inf.Sal=KMLink_Inf.sal
and tajdid_nazar_inf.modi_seq=KMLink_Inf.Modi_Seq
 inner join modi_inf on
 modi_inf.modi_seq=KMLink_Inf.Modi_Seq
  left join tashkhis_inf ttta on
 ttta.cod_hozeh=KMLink_Inf.Cod_Hozeh and
ttta.k_parvand=KMLink_Inf.K_Parvand and
ttta.Sal=KMLink_Inf.sal
and ttta.modi_seq=KMLink_Inf.Modi_Seq
and ttta.motamam=tajdid_nazar_inf.motamam
and ttta.serial =(select max(serial) from tashkhis_inf where ttta.cod_hozeh=cod_hozeh and ttta.sal=sal and ttta.k_parvand=k_parvand and ttta.motamam=motamam and ttta.modi_seq=modi_seq)
where 
tajdid_nazar_inf.serial=(SELECT MAX(serial) FROM tajdid_nazar_inf FF WHERE FF.cod_hozeh=tajdid_nazar_inf.cod_hozeh AND FF.SAL=tajdid_nazar_inf.sal AND FF.k_parvand=tajdid_nazar_inf.k_parvand AND  FF.modi_seq=tajdid_nazar_inf.modi_seq AND FF.motamam=tajdid_nazar_inf.motamam AND LEN(FF.sabt_date)>2 and tajdid_nazar_inf.ray in (1,2,3))
and tajdid_nazar_inf.ray in (1,2,3)
and  exists (select * from tajdid_nazar_inf ff where FF.cod_hozeh=tajdid_nazar_inf.cod_hozeh AND FF.SAL=tajdid_nazar_inf.sal 
AND FF.k_parvand=tajdid_nazar_inf.k_parvand AND  FF.modi_seq=tajdid_nazar_inf.modi_seq and len (ff.sabt_date)>2  AND 
FF.motamam=tajdid_nazar_inf.motamam AND LEN(FF.sabt_date)>2  AND FF.ray in (1,2,3) )
and NOT  exists (select * from ghatee_inf ff where FF.cod_hozeh=tajdid_nazar_inf.cod_hozeh AND FF.SAL=tajdid_nazar_inf.sal 
AND FF.k_parvand=tajdid_nazar_inf.k_parvand AND  FF.modi_seq=tajdid_nazar_inf.modi_seq AND  FF.motamam=tajdid_nazar_inf.motamam 
and len (ff.sabt_date)>2 AND  FF.modi_seq=tajdid_nazar_inf.modi_seq   )
and 
ghabln_inf.Sal between %s and %s
union
select 
hh.cod_hozeh  as 'کد حوزه' ,
hh.k_parvand as 'کلاسه پرونده',
ta.motamam AS 'متمم' ,
hh.Sal as 'عملکرد',
namee as 'نام',
family as 'نام خانوادگي',
modi_inf.cod_meli as 'کد ملی',
modi_inf.modi_seq as 'شناسه مودی',
ta.asl_maliat as 'ماليات تشخيص',
ta.dar_maliat_mash as'درآمد مشمول مالیات',
ta.sabt_date as 'تاریخ ثبت برگ تشخیص',
ta.sabt_no as'شماره ثبت برگ تشخیص',
'عدم اعتراض مودی پس از ابلاغ برگ تشخیص' as 'علت قطعی سازی' 
 from ghabln_inf hh inner join KMLink_Inf mm on hh.Sal=mm.sal and hh.cod_hozeh=mm.Cod_Hozeh and hh.k_parvand=mm.K_Parvand
inner join modi_inf on modi_inf.modi_seq=mm.Modi_Seq
inner join tashkhis_inf ta on ta.sal=mm.sal and ta.cod_hozeh=mm.Cod_Hozeh and ta.k_parvand =mm.K_Parvand and ta.modi_seq=mm.Modi_Seq
where
LEN(ta.sabt_date)>2 and (len (Eblagh_Sabt_Date) >2  )
and 
(aks_modi is null or aks_modi ='' or aks_modi !=2)
and cast (eblag_date as float)between  13700101 and %s
and
nahve_eblag in (1,2,3)
and
not exists (select cod_hozeh from adam_faliat_inf ta where ta.cod_hozeh=hh.cod_hozeh and ta.sal=hh.Sal and ta.k_parvand=hh.k_parvand and len(ta.sabt_no)>1  and isnull(ebtal, 0) = 0 )
and ta.serial=(select max(serial)from tashkhis_inf tr where tr.sal=ta.sal and tr.cod_hozeh=ta.cod_hozeh and tr.k_parvand=ta.k_parvand and tr.motamam=ta.motamam and tr.modi_seq=ta.modi_seq)
and not exists (select cod_hozeh from ghatee_inf ga where ga.cod_hozeh=ta.cod_hozeh and ga.sal=ta.Sal and ga.k_parvand=ta.k_parvand and ga.motamam=ta.motamam and ga.modi_seq=ta.modi_seq and LEN(ga.sabt_date)>2)
and hh.sal between %s and %s
union
SELECT 
ghabln_inf.cod_hozeh  as 'کد حوزه' ,
ghabln_inf.k_parvand as 'کلاسه پرونده',
Badvi_inf.motamam AS 'متمم' ,
ghabln_inf.Sal as 'عملکرد',
namee as 'نام',
family as 'نام خانوادگي',
modi_inf.cod_meli as 'کد ملی',
modi_inf.modi_seq as 'شناسه مودی',
ttta.asl_maliat as 'ماليات تشخيص',
ttta.dar_maliat_mash as 'درآمد مشمول مالیات',
ttta.sabt_date as 'تاریخ ثبت برگ تشخیص',
ttta.sabt_no as'شماره ثبت برگ تشخیص',
'هیات بدوی رای صادر شده و مودی اعتراض ننموده'as'علت'
FROM 
ghabln_inf
inner join KMLink_Inf on
ghabln_inf.cod_hozeh=KMLink_Inf.Cod_Hozeh and
ghabln_inf.k_parvand=KMLink_Inf.K_Parvand and
ghabln_inf.Sal=KMLink_Inf.sal
inner join Badvi_inf on
Badvi_inf.cod_hozeh=KMLink_Inf.Cod_Hozeh and
Badvi_inf.k_parvand=KMLink_Inf.K_Parvand and
Badvi_inf.Sal=KMLink_Inf.sal
and Badvi_inf.modi_seq=KMLink_Inf.Modi_Seq
 inner join modi_inf on
 modi_inf.modi_seq=KMLink_Inf.Modi_Seq
 left join tashkhis_inf ttta on
 ttta.cod_hozeh=KMLink_Inf.Cod_Hozeh and
ttta.k_parvand=KMLink_Inf.K_Parvand and
ttta.Sal=KMLink_Inf.sal
and ttta.modi_seq=KMLink_Inf.Modi_Seq
and ttta.motamam=Badvi_inf.motamam
and ttta.serial =(select max(serial) from tashkhis_inf where ttta.cod_hozeh=cod_hozeh and ttta.sal=sal and ttta.k_parvand=k_parvand and ttta.motamam=motamam and ttta.modi_seq=modi_seq)
where 
Badvi_inf.serial=(SELECT MAX(serial) FROM Badvi_inf FF WHERE FF.cod_hozeh=Badvi_inf.cod_hozeh AND FF.SAL=Badvi_inf.sal AND FF.k_parvand=Badvi_inf.k_parvand AND  FF.modi_seq=Badvi_inf.modi_seq AND FF.motamam=Badvi_inf.motamam AND LEN(FF.sabt_date)>2 and Badvi_inf.ray in (1,2,3))
and Badvi_inf.ray in (1,2,3)
AND ( [ray_aks] IS NULL OR  [ray_aks]='' OR [ray_aks]!=2)
and  exists (select * from Badvi_inf ff where FF.cod_hozeh=Badvi_inf.cod_hozeh AND FF.SAL=Badvi_inf.sal 
AND FF.k_parvand=Badvi_inf.k_parvand AND  FF.modi_seq=Badvi_inf.modi_seq and len (ff.sabt_date)>2  AND 
FF.motamam=Badvi_inf.motamam AND LEN(FF.sabt_date)>2 AND cast([EblaghRayDate] as float)BETWEEN 13700101 AND %s AND FF.ray in (1,2,3) )
and not exists (select * from ghatee_inf ff where FF.cod_hozeh=Badvi_inf.cod_hozeh AND FF.SAL=Badvi_inf.sal 
AND FF.k_parvand=Badvi_inf.k_parvand AND  FF.modi_seq=Badvi_inf.modi_seq and len (ff.sabt_date)>2  AND 
FF.motamam=Badvi_inf.motamam AND LEN(FF.sabt_date)>2  )
and 
ghabln_inf.Sal between %s and %s
union
SELECT tbl.Cod_Hozeh,
tbl.K_Parvand,
tbl.motamam,
tbl.sal,
tbl.namee,
tbl.family,
tbl.cod_meli,
tbl.modi_seq,
tbl.asl_maliat,
tbl.dar_maliat_mash,
tbl.[تاریخ ثبت برگ تشخیص],
tbl.[شماره ثبت برگ تشخیص],
tbl.علت
 FROM
(select
tava.nazar_mcol, 
mm.Cod_Hozeh ,
mm.K_Parvand  ,
mm.sal ,
TA.motamam  ,
modi_inf.namee ,
modi_inf.family ,
modi_inf.cod_meli ,
modi_inf.modi_seq,
ta.asl_maliat  ,
ta.dar_maliat_mash ,
ta.sabt_date as 'تاریخ ثبت برگ تشخیص',
ta.sabt_no as'شماره ثبت برگ تشخیص',
'توافق با ممیز کل انجام شده' as 'علت'
from ghabln_inf hh inner join KMLink_Inf mm on hh.Sal=mm.sal and hh.cod_hozeh=mm.Cod_Hozeh and hh.k_parvand=mm.K_Parvand
inner join modi_inf on modi_inf.modi_seq=mm.Modi_Seq
inner join tashkhis_inf ta on ta.sal=mm.sal and ta.cod_hozeh=mm.Cod_Hozeh and ta.k_parvand =mm.K_Parvand and ta.modi_seq=mm.Modi_Seq
and ta.serial=(select max(serial)from tashkhis_inf where ta.sal=sal and ta.cod_hozeh=Cod_Hozeh and ta.k_parvand =K_Parvand and ta.modi_seq=Modi_Seq and ta.motamam=motamam)
inner join 
tavafogh_mcol tava on tava.sal=mm.sal and tava.cod_hozeh=mm.Cod_Hozeh and tava.k_parvand =mm.K_Parvand and tava.modi_seq=mm.Modi_Seq and tava.motamam=ta.motamam
where
 len (ta.sabt_date)>2 and 
 LEN(tava.sabt_date)>2 
and tava.serial=(select max(serial)from tavafogh_mcol where tava.sal=tavafogh_mcol.sal and tava.cod_hozeh=tavafogh_mcol.cod_hozeh and tava.k_parvand=tavafogh_mcol.k_parvand and tava.motamam=tavafogh_mcol.motamam     )
and ta.serial=(select max(serial)from tashkhis_inf where ta.sal=tashkhis_inf.sal and ta.cod_hozeh=tashkhis_inf.cod_hozeh and ta.k_parvand=tashkhis_inf.k_parvand and ta.motamam=tashkhis_inf.motamam  and len(tashkhis_inf.sabt_date) >2 )
AND NoT EXISTS (SELECT * FROM ghatee_inf TH WHERE Ta.sal=TH.sal and ta.cod_hozeh=TH.cod_hozeh and ta.k_parvand=TH.k_parvand and ta.motamam=TH.motamam and ta.modi_seq=TH.modi_seq and len(TH.sabt_date) >2 )
and  mm.sal between %s and %s
and ta.aks_modi=2) As tbl
inner join (select distinct nazar_mcol from tavafogh_mcol where nazar_mcol<4) tava on tava.nazar_mcol=tbl.nazar_mcol) as r
where
(( CONNECTIONPROPERTY('local_net_address' )!= '10.53.32.130' or (CONNECTIONPROPERTY('local_net_address' )= '10.53.32.130' and r.[کد حوزه] !=168141))
and
( CONNECTIONPROPERTY('local_net_address' )!= '10.53.48.130' or (CONNECTIONPROPERTY('local_net_address' )= '10.53.48.130' and r.[کد حوزه] =168141)))
""" % (azsal, tasal, date, azsal, tasal, date, azsal, tasal, azsal, tasal)


def get_tblupdateDate(tbl_name=None):
    sql_query = """SELECT MAX(update_date) FROM tblLog
                    WHERE table_name = '%s'
                """ % tbl_name

    return sql_query


def get_sql_agg_most():
    return """
SELECT * FROM
(SELECT tbl1.شهرستان as [تشخیص-شهرستان], tbl1.[کد اداره] as [تشخیص-کد اداره],
tbl2.شهرستان as [قطعی-شهرستان], tbl2.[کد اداره] as [قطعی-کد اداره] FROM
(SELECT [کد اداره],[شهرستان], COUNT(*) AS [تعداد تشخیص مستغلات] FROM 
[dbo].[tblTashkhisMost]
GROUP BY [کد اداره],[شهرستان]) AS tbl1
FULL OUTER JOIN
(SELECT [کد اداره],[شهرستان], COUNT(*)AS [تعداد قطعی مستغلات] FROM 
[dbo].[tblGhateeMost]
GROUP BY [کد اداره],[شهرستان]) AS tbl2
ON tbl1.[کد اداره] = tbl2.[کد اداره]) as tbl3
FULL OUTER JOIN
(SELECT [کد اداره] as [آماده قطعی-کد اداره],[شهرستان] as [آماده قطعی-شهرستان], COUNT(*)AS [تعداد آماده قطعی مستغلات] FROM 
[dbo].[tblGhateeMost]
GROUP BY [کد اداره],[شهرستان]) AS tbl4
ON tbl3.[قطعی-کد اداره] = tbl4.[آماده قطعی-کد اداره]
"""


def get_sql_all_tashkhis_mash(form_year, to_year, codemelli):
    """Return all tashkhis from mashaghel sonati"""
    return """select 
case
when  ghabln_inf.cod_hozeh  =168141 then 'هويزه'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1601 then 'اهواز کد يک'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1602 then 'اهواز کد دو'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,5) =16439 then 'هنديجان'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,5) =16649 then 'لالي'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,5) =16718 then 'رامشير'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1603 then 'اهواز کد سه'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1604 then 'اهواز کد چهار '
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1605 then 'اهواز کد پنج'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1606 then 'اهواز کد شش'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1607 then 'اهواز کد هفت'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1608 then 'اهواز کد هشت'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1609 then 'اهواز کد نه'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1610 then 'اهواز کد 10'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1611 then 'اهواز کد 11'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1615 then 'اهواز کد 15'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1631 then 'آبادان کد 31'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1632 then 'آبادان کد 32'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1633 then 'آبادان کد 33'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1626 then 'دزفول'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1627 then 'دزفول'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1628 then 'دزفول'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1643 then 'ماهشهر کد43'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1644 then 'ماهشهر کد44'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1636 then 'بهبهان'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1639 then 'شوشتر'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1641 then 'گتوند'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1649 then 'بندر امام'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1651 then 'انديمشک'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1654 then 'خرمشهر'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1659 then 'شادگان'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1661 then 'اميديه'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1663 then 'آغاجاري'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1664 then 'مسجد سليمان'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1668 then 'شوش'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1671 then 'رامهرمز'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1676 then 'ايذه'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1678 then 'هفتگل'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1679 then 'باغملک'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1681 then 'دشت آزاداگان'
else 'نامشخص' end as 'شهرستان'
,
case 
 when ghabln_inf.cod_hozeh  IN (168141) then '168141'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,5) IN ('16439','16649','16718') then SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,5) else SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) end as 'کد اداره'
,
namee as 'نام',
family as 'نام خانوادگي',
ghabln_inf.Sal as 'عملکرد',
ghabln_inf.cod_hozeh as 'کد حوزه',
ghabln_inf.k_parvand as 'کلاسه پرونده',
tashkhis_inf.motamam as 'متمم',
case when KMLink_Inf.have_ezhar=1 then 'داراي اظهارنامه' else 'فاقد اظهارنامه' end as 'وضعيت ارائه اظهارنامه',
Shohr_kasb as 'شهرت کسبي',
tabl_inf.S_des as 'شرح فعاليت',
modi_inf.modi_seq as 'شماره مودي',
modi_inf.cod_meli as 'کد ملی',
tashkhis_inf.dar_maliat_mash as 'درآمد ماليات مشاغل',
tashkhis_inf.mafiat101 as 'ميزان معافيت 101',
tashkhis_inf.moaf_sayer as 'ساير معافيت',
tashkhis_inf.asl_maliat as 'ماليات تشخيص',
tashkhis_inf.ghanony_maliat as 'ماليات قانوني',
tashkhis_inf.pardakht_mab as 'مبلغ پرداختي',
tashkhis_inf.mandeh_pardakht as 'مانده پرداختي',
tashkhis_inf.sabt_no as 'شماره ثبت برگ تشخيص',
tashkhis_inf.sabt_date as 'تاريخ ثبت برگ تشخيص',
tashkhis_inf.eblag_date as 'تاريخ ابلاغ برگ تشخيص',

case
when nahve_eblag=1 then 'به مودي' 
when nahve_eblag=2 then 'به بستگان' 
when nahve_eblag=3 then 'به مستخدم' 
when nahve_eblag=4 then 'قانوني' 
when nahve_eblag=5 then 'روزنامه اي' 
when nahve_eblag=6 then 'پست' 
when nahve_eblag=7 then 'غيره' else 'فاقد اطلاعات نحوه ابلاغ'
end as 'نحوه ابلاغ',
tashkhis_inf.Eblagh_Sabt_No as 'شماره ثبت ابلاغ برگ تشخيص',
tashkhis_inf.Eblagh_Sabt_Date as 'تاريخ ثبت ابلاغ برگ تشخيص' ,
 case when tashkhis_inf.aks_modi =1 then 'تمکين' 
 when tashkhis_inf.aks_modi =2 then 'اعتراض' else 'فاقد عکس العمل' end as 'عکس العمل مودي به برگ تشخيص',
 tashkhis_inf.aks_sabt_no as 'شماره ثبت عکس العمل برگ تشخيص',
 tashkhis_inf.aks_sabt_date as 'تاريخ ثبت عکس العمل برگ تشخيص' ,
 tashkhis_inf.Eblagh_Namek as 'ابلاغ کننده',
 tashkhis_inf.Eblagh_NameG as 'گيرنده برگ تشخيص'

from ghabln_inf 
inner join 
KMLink_Inf on
ghabln_inf.Sal=KMLink_Inf.sal and ghabln_inf.cod_hozeh=KMLink_Inf.Cod_Hozeh and ghabln_inf.k_parvand=KMLink_Inf.K_Parvand
inner join modi_inf on
modi_inf.modi_seq=KMLink_Inf.Modi_Seq
inner join tashkhis_inf on
tashkhis_inf.sal=KMLink_Inf.sal and tashkhis_inf.cod_hozeh=KMLink_Inf.Cod_Hozeh and tashkhis_inf.k_parvand=KMLink_Inf.K_Parvand and tashkhis_inf.modi_seq=KMLink_Inf.Modi_Seq
left join [tabl_inf] on
ghabln_inf.Faliat_sharh=tabl_inf.S_code and G_code=1

where 

LEN(modi_inf.modi_seq)>8
and tashkhis_inf.serial=(select max(tt.serial)from tashkhis_inf tt where tashkhis_inf.sal=tt.sal and tashkhis_inf.cod_hozeh=tt.cod_hozeh and tashkhis_inf.k_parvand=tt.k_parvand and tashkhis_inf.motamam=tt.motamam)
and ghabln_inf.Sal between %s and %s

and LEN(sabt_date)>2 and cod_meli in ('%s')
""" % (form_year, to_year, codemelli)


def get_sql_all_ghatee_mash(form_year, to_year, codemelli):
    """Return all tashkhis from mashaghel sonati"""
    return """select 
case
when  ghabln_inf.cod_hozeh  =168141 then 'هويزه'
 when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1601 then 'اهواز کد يک'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1602 then 'اهواز کد دو'
 when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,5) =16439 then 'هنديجان'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,5) =16649 then 'لالي'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,5) =16718 then 'رامشير'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1603 then 'اهواز کد سه'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1604 then 'اهواز چهار '
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1605 then 'اهواز کد پنج'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1607 then 'اهواز کد هفت'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1608 then 'اهواز کد هشت'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1610 then 'اهواز'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1611 then 'اهواز'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1631 then 'آبادان کد 31'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1632 then 'آبادان کد 32'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1633 then 'آبادان کد 33'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1626 then 'دزفول'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1627 then 'دزفول'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1628 then 'دزفول'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1643 then 'ماهشهر کد43'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1644 then 'ماهشهر کد44'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1636 then 'بهبهان'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1639 then 'شوشتر'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1641 then 'گتوند'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1649 then 'بندر امام'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1648 then 'بندر امام'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1651 then 'انديمشک'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1654 then 'خرمشهر'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1659 then 'شادگان'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1661 then 'اميديه'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1663 then 'آغاجاري'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1664 then 'مسجد سليمان'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1668 then 'شوش'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1671 then 'رامهرمز'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1676 then 'ايذه'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1678 then 'هفتگل'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1679 then 'باغملک'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) =1681 then 'دشت آزاداگان'
else 'نامشخص' end as 'شهرستان'
,
case 
 when ghabln_inf.cod_hozeh  IN (168141) then '168141'
when SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,5) IN ('16439','16649','16718') then SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,5) else SUBSTRING( cast (ghabln_inf.cod_hozeh as varchar(20) ),1,4) end as 'کد اداره'
,
namee as 'نام',
family as 'نام خانوادگي',
ghabln_inf.Sal as 'عملکرد',
ghabln_inf.cod_hozeh as 'کد حوزه',
ghabln_inf.k_parvand as 'کلاسه پرونده',
ghatee_inf.motamam as 'متمم',
case when KMLink_Inf.have_ezhar=1 then 'داراي اظهارنامه' else 'فاقد اظهارنامه' end as 'وضعيت ارائه اظهارنامه',
Shohr_kasb as 'شهرت کسبي',
tabl_inf.S_des as 'شرح فعاليت',
modi_inf.modi_seq as 'شماره مودي',
ghatee_inf.dar_maliat_mash as 'درآمد ماليات مشاغل',
ghatee_inf.mafiat101 as 'ميزان معافيت 101',
ghatee_inf.mandeh_pardakht as 'مانده پرداخت',
ghatee_inf.asl_maliat as 'ماليات قطعي',
ghatee_inf.pardakht_mab as 'مبلغ پرداختي',
ghatee_inf.mandeh_pardakht as 'مانده پرداختي',
ghatee_inf.sabt_no as 'شماره ثبت برگ قطعي',
ghatee_inf.sabt_date as 'تاريخ ثبت برگ قطعي',
ghatee_inf.eblag_date as 'تاريخ ابلاغ برگ قطعي',
modi_inf.cod_meli as 'کدملي',
case
when nahve_eblag=0 then 'الکترونيکي' 
when nahve_eblag=1 then 'به مودي' 
when nahve_eblag=2 then 'به بستگان' 
when nahve_eblag=3 then 'به مستخدم' 
when nahve_eblag=4 then 'قانوني' 
when nahve_eblag=5 then 'روزنامه اي' 
when nahve_eblag=6 then 'پست' 
when nahve_eblag=7 then 'غيره' else 'فاقد اطلاعات نحوه ابلاغ'
end as 'نحوه ابلاغ',
ghatee_inf.Eblagh_Sabt_No as 'شماره ثبت ابلاغ برگ قطعي',
ghatee_inf.Eblagh_Sabt_Date as 'تاريخ ثبت ابلاغ برگ قطعي' ,
 case when ghatee_inf.aks_modi =1 then 'تمکين' 
 when ghatee_inf.aks_modi =2 then 'اعتراض' else 'فاقد عکس العمل' end as 'عکس العمل مودي به برگ قطعي',
 ghatee_inf.aks_sabt_no as 'شماره ثبت عکس العمل برگ قطعي',
 ghatee_inf.aks_sabt_date as 'تاريخ ثبت عکس العمل برگ قطعي' ,
 ghatee_inf.Eblagh_Namek as 'ابلاغ کننده',
 ghatee_inf.Eblagh_NameG as 'گيرنده برگ قطعي',
  ghatee_inf.jarimeh_mab as 'مبلغ جريمه برگ قطعي'

from ghabln_inf 
inner join 
KMLink_Inf on
ghabln_inf.Sal=KMLink_Inf.sal and ghabln_inf.cod_hozeh=KMLink_Inf.Cod_Hozeh and ghabln_inf.k_parvand=KMLink_Inf.K_Parvand
inner join modi_inf on
modi_inf.modi_seq=KMLink_Inf.Modi_Seq
inner join ghatee_inf on
ghatee_inf.sal=KMLink_Inf.sal and ghatee_inf.cod_hozeh=KMLink_Inf.Cod_Hozeh and ghatee_inf.k_parvand=KMLink_Inf.K_Parvand and ghatee_inf.modi_seq=KMLink_Inf.Modi_Seq
left join [tabl_inf] on
ghabln_inf.Faliat_sharh=tabl_inf.S_code and G_code=1
where 
LEN(modi_inf.modi_seq)>8
and ghatee_inf.serial=(select max(tt.serial)from ghatee_inf tt where ghatee_inf.sal=tt.sal and ghatee_inf.cod_hozeh=tt.cod_hozeh and ghatee_inf.k_parvand=tt.k_parvand and ghatee_inf.motamam=tt.motamam)
and ghabln_inf.Sal between %s and %s and cod_meli in ('%s')
and len (ghatee_inf.Eblagh_Sabt_Date)>4
and LEN(sabt_date)>2
""" % (form_year, to_year, codemelli)


def get_sql_alltables(db_name):
    return """ select TABLE_NAME
FROM [%s].INFORMATION_SCHEMA.TABLES
WHERE TABLE_TYPE='BASE TABLE'
""" % db_name


def get_sql_if_column_in_table(query, table='[MASHAGHEL].[dbo].[KMLink_Inf]', column='Modi_Seq'):
    return """IF COL_LENGTH({},{}) IS NOT NULL 
                BEGIN
                    BEGIN TRAN 
                    {} 
                    COMMIT TRAN
                END""".format(table, column, query)


def get_sql_agg_soratmoamelat():
    return """
use TestDb
DELETE FROM tblSoratMoamelat1396Hist
WHERE [تاریخ بروز رسانی]=REPLACE(CONVERT(DATE,GETDATE()),'-','')
use tax
insert into TestDb.dbo.tblSoratMoamelat1396Hist
SELECT 
REPLACE(CONVERT(DATE,GETDATE()),'-','') as [تاریخ بروز رسانی]
, city as [شهر]
,ISNULL([Ejare],0) AS[Ejare]
,ISNULL([PeymankarPeymanBoland],0) AS [PeymankarPeymanBoland]
,ISNULL([KarfarmaPeymanBoland],0) AS [KarfarmaPeymanBoland]
,ISNULL([KharidarKarfarma],0) AS [KharidarKarfarma]
,ISNULL([HagholamalKariSahebkalaForoshande],0) AS [HagholamalKariSahebkalaForoshande]
,ISNULL([HagholamalKariKarmozdKharidar],0) AS [HagholamalKariKarmozdKharidar]
,ISNULL([HagholamalKariKarmozdForoshande],0) AS [HagholamalKariKarmozdForoshande]
,ISNULL([Kharid],0) AS[Kharid]
,ISNULL([Forosh],0) AS[Forosh]
,ISNULL([SakhtPishforoshAmlak],0) AS [SakhtPishforoshAmlak]
FROM
((SELECT DISTINCT city FROM tblCityNewSys UNION SELECT '**') as tblCityNewSys
left join
(select 
[شهر]
,ISNULL([Ejare],0) AS[Ejare]
,ISNULL([PeymankarPeymanBoland],0) AS [PeymankarPeymanBoland]
,ISNULL([KarfarmaPeymanBoland],0) AS [KarfarmaPeymanBoland]
,ISNULL([KharidarKarfarma],0) AS [KharidarKarfarma]
,ISNULL([HagholamalKariSahebkalaForoshande],0) AS [HagholamalKariSahebkalaForoshande]
,ISNULL([HagholamalKariKarmozdKharidar],0) AS [HagholamalKariKarmozdKharidar]
,ISNULL([HagholamalKariKarmozdForoshande],0) AS [HagholamalKariKarmozdForoshande]
,ISNULL([Kharid],0) AS[Kharid]
,ISNULL([Forosh],0) AS[Forosh]
,ISNULL([SakhtPishforoshAmlak],0) AS [SakhtPishforoshAmlak]

 from
(
select isnull(tblCityNewSys.City,'**') as [شهر],'Ejare' as [نوع],1396 as [عملکرد],COUNT(*) as [مانده] from
(SELECT [شهر ملک] as shahr
FROM TestDb.[dbo].[tblSorammoamelatEjare]) as a
left join
tblCityNewSys
on tblCityNewSys.SubCity=a.shahr collate arabic_ci_as
group by tblCityNewSys.City

union

select isnull(tblCityNewSys.City,'**') as [شهر],'Forosh' as [نوع],1396 as [عملکرد],COUNT(*) as [مانده] from
(SELECT [شهر] as shahr
FROM TestDb.[dbo].[tblSorammoamelatForosh]) as a
left join
tblCityNewSys
on tblCityNewSys.SubCity=a.shahr collate arabic_ci_as
group by tblCityNewSys.City


union

select isnull(tblCityNewSys.City,'**') as [شهر],'HagholamalKariKarmozdForoshande' as [نوع],1396 as [عملکرد],COUNT(*) as [مانده] from
(SELECT [شهر صاحب کالا(فروشنده)] as shahr
FROM TestDb.[dbo].[tblSorammoamelatHagholamalKariKarmozdForoshande]) as a
left join
tblCityNewSys
on tblCityNewSys.SubCity=a.shahr collate arabic_ci_as
group by tblCityNewSys.City

union

select isnull(tblCityNewSys.City,'**') as [شهر],'HagholamalKariKarmozdKharidar' as [نوع],1396 as [عملکرد],COUNT(*) as [مانده] from
(SELECT [شهر خریدار/کارفرما(مدیریت پیمان)] as shahr
FROM TestDb.[dbo].[tblSorammoamelatHagholamalKariKarmozdKharidar]) as a
left join
tblCityNewSys
on tblCityNewSys.SubCity=a.shahr collate arabic_ci_as
group by tblCityNewSys.City

union

select isnull(tblCityNewSys.City,'**') as [شهر],'HagholamalKariSahebkalaForoshande' as [نوع],1396 as [عملکرد],COUNT(*) as [مانده] from
(SELECT [شهر صاحب کالا(فروشنده)] as shahr
FROM TestDb.[dbo].[tblSorammoamelatHagholamalKariSahebkalaForoshande]) as a
left join
tblCityNewSys
on tblCityNewSys.SubCity=a.shahr collate arabic_ci_as
group by tblCityNewSys.City


union

select isnull(tblCityNewSys.City,'**') as [شهر],'KarfarmaPeymanBoland' as [نوع],1396 as [عملکرد],COUNT(*) as [مانده] from
(SELECT [شهر] as shahr
FROM TestDb.[dbo].[tblSorammoamelatKarfarmaPeymanBoland]) as a
left join
tblCityNewSys
on tblCityNewSys.SubCity=a.shahr collate arabic_ci_as
group by tblCityNewSys.City

union

select isnull(tblCityNewSys.City,'**') as [شهر],'Kharid' as [نوع],1396 as [عملکرد],COUNT(*) as [مانده] from
(SELECT [شهر] as shahr
FROM TestDb.[dbo].[tblSorammoamelatKharid]) as a
left join
tblCityNewSys
on tblCityNewSys.SubCity=a.shahr collate arabic_ci_as
group by tblCityNewSys.City

union

select isnull(tblCityNewSys.City,'**') as [شهر],'KharidarKarfarma' as [نوع],1396 as [عملکرد],COUNT(*) as [مانده] from
(SELECT [شهر خریدار/کارفرما(مدیریت پیمان)] as shahr
FROM TestDb.[dbo].[tblSorammoamelatKharidarKarfarma]) as a
left join
tblCityNewSys
on tblCityNewSys.SubCity=a.shahr collate arabic_ci_as
group by tblCityNewSys.City

union

select isnull(tblCityNewSys.City,'**') as [شهر],'PeymankarPeymanBoland' as [نوع],1396 as [عملکرد],COUNT(*) as [مانده] from
(SELECT [شهر] as shahr
FROM TestDb.[dbo].[tblSorammoamelatPeymankarPeymanBoland]) as a

left join

tblCityNewSys
on tblCityNewSys.SubCity=a.shahr collate arabic_ci_as
group by tblCityNewSys.City

union

select isnull(tblCityNewSys.City,'**') as [شهر],'SakhtPishforoshAmlak' as [نوع],1396 as [عملکرد],COUNT(*) as [مانده] from
(SELECT [شهر ملک] as shahr
FROM TestDb.[dbo].[tblSorammoamelatSakhtPishforoshAmlak]) as a

left join

tblCityNewSys
on tblCityNewSys.SubCity=a.shahr collate arabic_ci_as
group by tblCityNewSys.City

)as src
   PIVOT
( sum([مانده])
      FOR [نوع] IN ([Ejare]
,[PeymankarPeymanBoland]
,[KarfarmaPeymanBoland]
,[KharidarKarfarma]
,[HagholamalKariSahebkalaForoshande]
,[HagholamalKariKarmozdKharidar]
,[HagholamalKariKarmozdForoshande]
,[Kharid]
,[Forosh]
,[SakhtPishforoshAmlak])) AS PivotTable) as tblMandeh
      on tblCityNewSys.City=tblMandeh.[شهر])
"""


def get_sql_v_portal(cols="""[تاریخ صدور برگ تشخیص (تاریخ تایید گزارش حسابرسی)], 
                     [سال عملکرد], [منبع مالیاتی],
            [شماره برگ قطعی], [کد اداره], [نام اداره],
            [کد ملی/شناسه ملی], [نام مودی],[کد رهگيري ثبت نام ],
            [شماره اظهارنامه],[پرداختی از اصل مالیات],
            [مالیات ابرازی], [مالیات قطعی],[مالیات تشخیص], [توافق], [تاریخ ابلاغ برگ تشخیص],
                [مالیات حاصل از اعتراض], [کد رهگیری اظهارنامه سنیم - آخرین ورژن اعمال شد]""", year='1400.0'):
    return f"""
            select {cols}
            FROM V_PORTAL
            where [سال عملکرد] = '{year}'
            """


def get_tax_types(path=r'E:\automating_reports_V2\mapping\TAX_TYPE.xlsx', sheet_name='tax_type'):
    tax_types = pd.read_excel(path, sheet_name=sheet_name)
    final_dict = {}
    for index, row in tax_types.iterrows():
        final_dict[row[0]] = row[1]

    return final_dict


def get_sql_hesabras_perf():
    return """
        SELECT * FROM
        (SELECT [شناسه اظهارنامه],[حسابرس کمکی],[کد ملی کاربر],سمت
        FROM
        (SELECT
        DISTINCT
            [شناسه اظهارنامه]
            ,[حسابرس کمکی]
            ,[کد کاربر]
        FROM [dbo].[V_AREASUSP]) as shak
        JOIN
        (
        SELECT DISTINCT 
        [کد کاربری]
        ,[کد ملی کاربر]
        ,IIF(Code IS NULL ,N'حسابرس ارشد',N'رئیس گروه')AS سمت
        FROM [dbo].[V_USERS]
        LEFT JOIN
        (SELECT DISTINCT [کد ملی] as Code FROM [dbo].[tblVRaeesGoroh]) as a
        ON [کد ملی کاربر]=Code
        )as Users
        ON [کد کاربر]=[کد کاربری]
        ) as tblUser
        JOIN
        (


        SELECT DISTINCT
        tblvp.* 

        ,tblbadvi.تاييد AS [تایید بدوی]
        ,tblbadvi.[تعديل ] AS [تعدیل بدوی]
        ,tblbadvi.[توافق ] AS [توافق بدوی]
        ,tblbadvi.[رفع تعرض] AS [رفع تعرض بدوی]

        ,tbltajdid.تاييد AS [تایید تجدید نظر]
        ,tbltajdid.[تعديل ] AS [تعدیل تجدید نظر]
        ,tbltajdid.[توافق ] AS [توافق تجدید نظر]
        ,tbltajdid.[رفع تعرض] AS [رفع تعرض تجدید نظر]

        FROM
        (SELECT 
        DISTINCT
            [شماره اقتصادی]
            ,[نام مودی]
            ,[کد اداره]
            ,[نام اداره]
            ,[شماره اظهارنامه]
            ,[سال عملکرد]
            ,[منبع مالیاتی]
            ,[شناسه حسابرسی]
            ,[تاریخ صدور برگ تشخیص (تاریخ تایید گزارش حسابرسی)]
            ,[مالیات تشخیص]
            ,[مالیات ابرازی]
            ,[نام حسابرس]
            ,[مالیات حاصل از اعتراض]
            ,[شماره درخواست هیات بدوی]
            ,[مالیات حاصل از بدوی]
            ,[شماره درخواست هیات تجدید نظر]
            ,[مالیات حاصل از تجدید]
            ,[تاريخ ايجاد برگ قطعي]
            ,[مالیات قطعی]
        FROM [TestDb].[dbo].[V_PORTAL]
        WHERE 
        LEN([تاریخ صدور برگ تشخیص (تاریخ تایید گزارش حسابرسی)])>5
        AND LEN([تاريخ ايجاد برگ قطعي]) > 5
        AND LEN([تاریخ ایجاد کیس حسابرسی])>0

        AND [تاریخ صدور برگ تشخیص (تاریخ تایید گزارش حسابرسی)] BETWEEN '1402/01/01' AND '1402/12/29') as tblvp
        LEFT JOIN
            [dbo].[V_OBJ60] as tblbadvi
            ON tblvp.[شماره درخواست هیات بدوی]=tblbadvi.[شماره درخواست اعتراض / شکایت]
            LEFT JOIN
            [dbo].[V_OBJ60] as tbltajdid
            ON tblvp.[شماره درخواست هیات تجدید نظر]=tbltajdid.[شماره درخواست اعتراض / شکایت]
        
        )as tblvportal
        ON [شناسه اظهارنامه]=[شماره اظهارنامه]
"""


def get_sql_query_farayand_residegi_1401():
    return """
    SELECT 
    main.[منبع مالیاتی],main.[نوع اظهارنامه],main.[نام اداره]
    ,[تعداد کل اظهارنامه های دریافت شده]
    ,[تعداد شناسه حسابرسی صادر شده]
    ,[تعداد برگ تشخیص صادر شده]
    ,CAST(ISNULL(cast((cast([تعداد برگ تشخیص صادر شده] as real)/cast([تعداد کل اظهارنامه های دریافت شده] as real))*100 as decimal(10)),0) as nvarchar)+'%' AS [درصد تعیین تکلیف شده]
    ,[میزان مالیات ابرازی]
    ,[میزان مالیات ابرازی پرونده های دارای برگ تشخیص]
    ,[میزان مالیات تشخیصی پرونده های دارای برگ تشخیص]
    ,CAST(ISNULL
    (iif([میزان مالیات تشخیصی پرونده های دارای برگ تشخیص]=0 and [میزان مالیات ابرازی پرونده های دارای برگ تشخیص]=0,0,
    (cast((cast(([میزان مالیات تشخیصی پرونده های دارای برگ تشخیص]-[میزان مالیات ابرازی پرونده های دارای برگ تشخیص]) as real)/cast(iif([میزان مالیات ابرازی پرونده های دارای برگ تشخیص]=0,[میزان مالیات تشخیصی پرونده های دارای برگ تشخیص],[میزان مالیات ابرازی پرونده های دارای برگ تشخیص]) as real))*100 as decimal(10))
    ))
    ,0) as nvarchar)+'%' AS [درصد افزایش مالیات تشخیص پرونده های دارای برگ تشخیص به مالیات ابرازی آنها]
    ,[تعداد برگ تشخیص ابلاغ شده]
    ,[تعداد توافق انجام شده در اجرای ماده238 ]
    ,[تعداد پرونده های ارسالی به هیات حل اختلاف مالیاتی]
    ,[تعداد قطعی بر اساس سکوت مودی]
    ,[تعداد قطعی بر مبنای تمکین مودی]
    ,[تعداد قطعی بر مبنای ماده 238]
    ,[تعداد قطعی بر مبنای رای بدوی]
    ,[تعداد قطعی بر مبنای رای تجدید نظر]
    ,[تعداد رای صادره بدوی]
    ,[تعداد برگ قطعی صادر شده]
    ,CAST(ISNULL(cast((cast([تعداد برگ قطعی صادر شده] as real)/cast([تعداد کل اظهارنامه های دریافت شده] as real))*100 as decimal(10)),0) as nvarchar)+'%' AS [درصد برگ قطعی صادره]
    ,[میزان مالیات ابرازی پرونده های دارای برگ قطعی]
    ,[میزان مالیات تشخیصی پرونده های دارای برگ قطعی]
    ,CAST(ISNULL
    (
    iif([میزان مالیات تشخیصی پرونده های دارای برگ قطعی]=0 and [میزان مالیات ابرازی پرونده های دارای برگ قطعی]=0,0,
    cast((cast(([میزان مالیات تشخیصی پرونده های دارای برگ قطعی]-[میزان مالیات ابرازی پرونده های دارای برگ قطعی]) as real)/cast(iif([میزان مالیات ابرازی پرونده های دارای برگ قطعی]=0,[میزان مالیات تشخیصی پرونده های دارای برگ قطعی],[میزان مالیات ابرازی پرونده های دارای برگ قطعی]) as real))*100 as decimal(10))
    )
    ,0) as nvarchar)+'%' AS [درصد افزایش مالیات تشخیص پرونده های دارای برگ قطعی به مالیات ابرازی آنها]
    ,[میزان مالیات قطعی پرونده های دارای برگ قطعی]
    ,[تعداد برگ قطعی ابلاغ شده]
    

    FROM
    (SELECT 
        [منبع مالیاتی]
        ,[نوع اظهارنامه] ,[نام اداره]
        ,COUNT(*) AS [تعداد کل اظهارنامه های دریافت شده]
        FROM [TestDb].[dbo].[V_PORTAL]
    where [سال عملکرد]='1401'
    AND [منبع مالیاتی] in (N'مالیات بر درآمد مشاغل',N'مالیات بر درآمد شرکت ها')
    AND [نوع اظهارنامه]IN(N'مودیان مهم با ریسک بالا',N'رتبه ریسک بالا')
    GROUP BY [منبع مالیاتی],[نوع اظهارنامه],[نام اداره]) AS MAIN
    LEFT JOIN

    (SELECT 
        [منبع مالیاتی]
        ,[نوع اظهارنامه] ,[نام اداره]
        ,COUNT(*) AS [تعداد شناسه حسابرسی صادر شده]
        FROM [TestDb].[dbo].[V_PORTAL]
    where [سال عملکرد]='1401'
    AND [منبع مالیاتی] in (N'مالیات بر درآمد مشاغل',N'مالیات بر درآمد شرکت ها')
    AND [نوع اظهارنامه]IN(N'مودیان مهم با ریسک بالا',N'رتبه ریسک بالا')
    AND LEN([تاریخ ایجاد کیس حسابرسی])>5
    GROUP BY [منبع مالیاتی] ,[نوع اظهارنامه] ,[نام اداره]) as a

    ON main.[منبع مالیاتی]=a.[منبع مالیاتی]
    AND main.[نوع اظهارنامه]=a.[نوع اظهارنامه]
    AND main.[نام اداره]=a.[نام اداره]
    LEFT JOIN
        (SELECT 
        [منبع مالیاتی]
        ,[نوع اظهارنامه] ,[نام اداره]
        ,COUNT(*)AS [تعداد برگ تشخیص صادر شده]
        FROM [TestDb].[dbo].[V_PORTAL]
    where [سال عملکرد]='1401'
    AND [منبع مالیاتی] in (N'مالیات بر درآمد مشاغل',N'مالیات بر درآمد شرکت ها')
    AND [نوع اظهارنامه]IN(N'مودیان مهم با ریسک بالا',N'رتبه ریسک بالا')
    AND LEN([تاریخ صدور برگ تشخیص (تاریخ تایید گزارش حسابرسی)])>5
    GROUP BY [منبع مالیاتی] ,[نوع اظهارنامه] ,[نام اداره]) as b

    ON main.[منبع مالیاتی]=b.[منبع مالیاتی]
    AND main.[نوع اظهارنامه]=b.[نوع اظهارنامه]
    AND main.[نام اداره]=b.[نام اداره]
    LEFT JOIN
        (SELECT 
        [منبع مالیاتی]
        ,[نوع اظهارنامه] ,[نام اداره]
        ,SUM(IIF(LEN([مالیات ابرازی])=0,0,cast([مالیات ابرازی] as numeric(24)))) [میزان مالیات ابرازی]
        FROM [TestDb].[dbo].[V_PORTAL]
    where [سال عملکرد]='1401'
    AND [منبع مالیاتی] in (N'مالیات بر درآمد مشاغل',N'مالیات بر درآمد شرکت ها')
    AND [نوع اظهارنامه]IN(N'مودیان مهم با ریسک بالا',N'رتبه ریسک بالا')
    GROUP BY [منبع مالیاتی] ,[نوع اظهارنامه] ,[نام اداره]) as c
    ON main.[منبع مالیاتی]=c.[منبع مالیاتی]
    AND main.[نوع اظهارنامه]=c.[نوع اظهارنامه]
    AND main.[نام اداره]=c.[نام اداره]
    LEFT JOIN

        (SELECT 
        [منبع مالیاتی]
        ,[نوع اظهارنامه] ,[نام اداره]
        ,SUM(IIF(LEN([مالیات ابرازی])=0,0,cast([مالیات ابرازی] as numeric(24)))) [میزان مالیات ابرازی پرونده های دارای برگ تشخیص]
        FROM [TestDb].[dbo].[V_PORTAL]
    where [سال عملکرد]='1401'
    AND [منبع مالیاتی] in (N'مالیات بر درآمد مشاغل',N'مالیات بر درآمد شرکت ها')
    AND [نوع اظهارنامه]IN(N'مودیان مهم با ریسک بالا',N'رتبه ریسک بالا')
        AND LEN([تاریخ صدور برگ تشخیص (تاریخ تایید گزارش حسابرسی)])>5
    GROUP BY [منبع مالیاتی] ,[نوع اظهارنامه] ,[نام اداره]) as d

    ON main.[منبع مالیاتی]=d.[منبع مالیاتی]
    AND main.[نوع اظهارنامه]=d.[نوع اظهارنامه]
    AND main.[نام اداره]=d.[نام اداره]
    LEFT JOIN
    

        (SELECT 
        [منبع مالیاتی]
        ,[نوع اظهارنامه] ,[نام اداره]
        ,SUM(IIF(LEN([مالیات تشخیص])=0,0,cast([مالیات تشخیص] as numeric(24)))) [میزان مالیات تشخیصی پرونده های دارای برگ تشخیص]
        FROM [TestDb].[dbo].[V_PORTAL]
    where [سال عملکرد]='1401'
    AND [منبع مالیاتی] in (N'مالیات بر درآمد مشاغل',N'مالیات بر درآمد شرکت ها')
    AND [نوع اظهارنامه]IN(N'مودیان مهم با ریسک بالا',N'رتبه ریسک بالا')
        AND LEN([تاریخ صدور برگ تشخیص (تاریخ تایید گزارش حسابرسی)])>5
    GROUP BY [منبع مالیاتی] ,[نوع اظهارنامه] ,[نام اداره])as e

    ON main.[منبع مالیاتی]=e.[منبع مالیاتی]
    AND main.[نوع اظهارنامه]=e.[نوع اظهارنامه]
    AND main.[نام اداره]=e.[نام اداره]
    LEFT JOIN
        (SELECT 
        [منبع مالیاتی]
        ,[نوع اظهارنامه] ,[نام اداره]
        ,COUNT(*)AS [تعداد برگ تشخیص ابلاغ شده]
        FROM [TestDb].[dbo].[V_PORTAL]
    where [سال عملکرد]='1401'
    AND [منبع مالیاتی] in (N'مالیات بر درآمد مشاغل',N'مالیات بر درآمد شرکت ها')
    AND [نوع اظهارنامه]IN(N'مودیان مهم با ریسک بالا',N'رتبه ریسک بالا')
    AND LEN([تاریخ ابلاغ برگ تشخیص])>5
    GROUP BY [منبع مالیاتی] ,[نوع اظهارنامه] ,[نام اداره])as f

    ON main.[منبع مالیاتی]=f.[منبع مالیاتی]
    AND main.[نوع اظهارنامه]=f.[نوع اظهارنامه]
    AND main.[نام اداره]=f.[نام اداره]
    LEFT JOIN

            (SELECT 
        [منبع مالیاتی]
        ,[نوع اظهارنامه] ,[نام اداره]
        ,COUNT(*)AS [تعداد توافق انجام شده در اجرای ماده238 ]
        FROM [TestDb].[dbo].[V_PORTAL]
    where [سال عملکرد]='1401'
    AND [منبع مالیاتی] in (N'مالیات بر درآمد مشاغل',N'مالیات بر درآمد شرکت ها')
    AND [نوع اظهارنامه]IN(N'مودیان مهم با ریسک بالا',N'رتبه ریسک بالا')
    AND [توافق]='Y'
    AND LEN([تاریخ رای])>5
    GROUP BY [منبع مالیاتی] ,[نوع اظهارنامه] ,[نام اداره]) as g

    ON main.[منبع مالیاتی]=g.[منبع مالیاتی]
    AND main.[نوع اظهارنامه]=g.[نوع اظهارنامه]
    AND main.[نام اداره]=g.[نام اداره]
    LEFT JOIN

    (SELECT 
        [منبع مالیاتی]
        ,[نوع اظهارنامه] ,[نام اداره]
        ,COUNT(*)AS [تعداد پرونده های ارسالی به هیات حل اختلاف مالیاتی]
        FROM [TestDb].[dbo].[V_PORTAL]
    where [سال عملکرد]='1401'
    AND [منبع مالیاتی] in (N'مالیات بر درآمد مشاغل',N'مالیات بر درآمد شرکت ها')
    AND [نوع اظهارنامه]IN(N'مودیان مهم با ریسک بالا',N'رتبه ریسک بالا')
    AND LEN([تاریخ اعتراض هیات بدوی])>5
    GROUP BY [منبع مالیاتی] ,[نوع اظهارنامه] ,[نام اداره]) as z1

    ON main.[منبع مالیاتی]=z1.[منبع مالیاتی]
    AND main.[نوع اظهارنامه]=z1.[نوع اظهارنامه]
    AND main.[نام اداره]=z1.[نام اداره]
    LEFT JOIN

        (SELECT 
        [منبع مالیاتی]
        ,[نوع اظهارنامه] ,[نام اداره]
        ,COUNT(*)AS [تعداد قطعی بر اساس سکوت مودی]
        FROM [TestDb].[dbo].[V_PORTAL]
    where [سال عملکرد]='1401'
    AND [منبع مالیاتی] in (N'مالیات بر درآمد مشاغل',N'مالیات بر درآمد شرکت ها')
    AND [نوع اظهارنامه]IN(N'مودیان مهم با ریسک بالا',N'رتبه ریسک بالا')
    AND [منبع قطعیت(صدور برگ قطعی در چه مرحله ای اتفاق افتاده است)]=N'حسابرسی سازمان امور مالیاتی'
    AND LEN([تاريخ ايجاد برگ قطعي])>5
    AND LEN([تاریخ ایجاد برگ تمکین])<5
    GROUP BY [منبع مالیاتی] ,[نوع اظهارنامه] ,[نام اداره]) as h

    ON main.[منبع مالیاتی]=h.[منبع مالیاتی]
    AND main.[نوع اظهارنامه]=h.[نوع اظهارنامه]
    AND main.[نام اداره]=h.[نام اداره]
    LEFT JOIN
    (SELECT 
        [منبع مالیاتی]
        ,[نوع اظهارنامه] ,[نام اداره]
        ,COUNT(*)AS [تعداد قطعی بر مبنای تمکین مودی]
        FROM [TestDb].[dbo].[V_PORTAL]
    where [سال عملکرد]='1401'
    AND [منبع مالیاتی] in (N'مالیات بر درآمد مشاغل',N'مالیات بر درآمد شرکت ها')
    AND [نوع اظهارنامه]IN(N'مودیان مهم با ریسک بالا',N'رتبه ریسک بالا')
    AND LEN([تاریخ ایجاد برگ تمکین])>5
    AND LEN([تاريخ ايجاد برگ قطعي])>5
    GROUP BY [منبع مالیاتی] ,[نوع اظهارنامه] ,[نام اداره]) as i

    ON main.[منبع مالیاتی]=i.[منبع مالیاتی]
    AND main.[نوع اظهارنامه]=i.[نوع اظهارنامه]
    AND main.[نام اداره]=i.[نام اداره]
    LEFT JOIN
        (SELECT 
        [منبع مالیاتی]
        ,[نوع اظهارنامه] ,[نام اداره]
        ,COUNT(*)AS [تعداد قطعی بر مبنای ماده 238]
        FROM [TestDb].[dbo].[V_PORTAL]
    where [سال عملکرد]='1401'
    AND [منبع مالیاتی] in (N'مالیات بر درآمد مشاغل',N'مالیات بر درآمد شرکت ها')
    AND [نوع اظهارنامه]IN(N'مودیان مهم با ریسک بالا',N'رتبه ریسک بالا')
    AND [منبع قطعیت(صدور برگ قطعی در چه مرحله ای اتفاق افتاده است)]=N'اعتراض به اداره امور مالیاتی'
    AND LEN([تاريخ ايجاد برگ قطعي])>5
    GROUP BY [منبع مالیاتی] ,[نوع اظهارنامه] ,[نام اداره]) as j

    ON main.[منبع مالیاتی]=j.[منبع مالیاتی]
    AND main.[نوع اظهارنامه]=j.[نوع اظهارنامه]
    AND main.[نام اداره]=j.[نام اداره]
    LEFT JOIN

    (SELECT 
        [منبع مالیاتی]
        ,[نوع اظهارنامه] ,[نام اداره]
        ,COUNT(*)AS [تعداد قطعی بر مبنای رای بدوی]
        FROM [TestDb].[dbo].[V_PORTAL]
    where [سال عملکرد]='1401'
    AND [منبع مالیاتی] in (N'مالیات بر درآمد مشاغل',N'مالیات بر درآمد شرکت ها')
    AND [نوع اظهارنامه]IN(N'مودیان مهم با ریسک بالا',N'رتبه ریسک بالا')
    AND [منبع قطعیت(صدور برگ قطعی در چه مرحله ای اتفاق افتاده است)]=N'هیات حل اختلاف بدوی'
    AND LEN([تاريخ ايجاد برگ قطعي])>5
    GROUP BY [منبع مالیاتی] ,[نوع اظهارنامه] ,[نام اداره]) as k

    ON main.[منبع مالیاتی]=k.[منبع مالیاتی]
    AND main.[نوع اظهارنامه]=k.[نوع اظهارنامه]
    AND main.[نام اداره]=k.[نام اداره]
    LEFT JOIN

    (
        SELECT 
        [منبع مالیاتی]
        ,[نوع اظهارنامه] ,[نام اداره]
        ,COUNT(*)AS [تعداد قطعی بر مبنای رای تجدید نظر]
        FROM [TestDb].[dbo].[V_PORTAL]
    where [سال عملکرد]='1401'
    AND [منبع مالیاتی] in (N'مالیات بر درآمد مشاغل',N'مالیات بر درآمد شرکت ها')
    AND [نوع اظهارنامه]IN(N'مودیان مهم با ریسک بالا',N'رتبه ریسک بالا')
    AND [منبع قطعیت(صدور برگ قطعی در چه مرحله ای اتفاق افتاده است)]=N'هیات حل اختلاف تجدید نظر'
    AND LEN([تاريخ ايجاد برگ قطعي])>5
    GROUP BY [منبع مالیاتی] ,[نوع اظهارنامه] ,[نام اداره]) as l

    ON main.[منبع مالیاتی]=l.[منبع مالیاتی]
    AND main.[نوع اظهارنامه]=l.[نوع اظهارنامه]
    AND main.[نام اداره]=l.[نام اداره]
    LEFT JOIN


    (SELECT 
        [منبع مالیاتی]
        ,[نوع اظهارنامه] ,[نام اداره]
        ,COUNT(*)AS [تعداد رای صادره بدوی]
        FROM [TestDb].[dbo].[V_PORTAL]
    where [سال عملکرد]='1401'
    AND [منبع مالیاتی] in (N'مالیات بر درآمد مشاغل',N'مالیات بر درآمد شرکت ها')
    AND [نوع اظهارنامه]IN(N'مودیان مهم با ریسک بالا',N'رتبه ریسک بالا')
    AND LEN([تاریخ رای بدوی])>5
    GROUP BY [منبع مالیاتی] ,[نوع اظهارنامه] ,[نام اداره]) as m

    ON main.[منبع مالیاتی]=m.[منبع مالیاتی]
    AND main.[نوع اظهارنامه]=m.[نوع اظهارنامه]
    AND main.[نام اداره]=m.[نام اداره]
    LEFT JOIN
    (SELECT 
        [منبع مالیاتی]
        ,[نوع اظهارنامه] ,[نام اداره]
        ,COUNT(*)AS [تعداد برگ قطعی صادر شده]
        FROM [TestDb].[dbo].[V_PORTAL]
    where [سال عملکرد]='1401'
    AND [منبع مالیاتی] in (N'مالیات بر درآمد مشاغل',N'مالیات بر درآمد شرکت ها')
    AND [نوع اظهارنامه]IN(N'مودیان مهم با ریسک بالا',N'رتبه ریسک بالا')
    AND LEN([تاريخ ايجاد برگ قطعي])>5
    GROUP BY [منبع مالیاتی] ,[نوع اظهارنامه] ,[نام اداره]) as n

    ON main.[منبع مالیاتی]=n.[منبع مالیاتی]
    AND main.[نوع اظهارنامه]=n.[نوع اظهارنامه]
    AND main.[نام اداره]=n.[نام اداره]
    LEFT JOIN

        (  SELECT 
        [منبع مالیاتی]
        ,[نوع اظهارنامه] ,[نام اداره]
        ,SUM(IIF(LEN([مالیات ابرازی])=0,0,cast([مالیات ابرازی] as numeric(24)))) [میزان مالیات ابرازی پرونده های دارای برگ قطعی]
        FROM [TestDb].[dbo].[V_PORTAL]
    where [سال عملکرد]='1401'
    AND [منبع مالیاتی] in (N'مالیات بر درآمد مشاغل',N'مالیات بر درآمد شرکت ها')
    AND [نوع اظهارنامه]IN(N'مودیان مهم با ریسک بالا',N'رتبه ریسک بالا')
        AND LEN([تاريخ ايجاد برگ قطعي])>5
    GROUP BY [منبع مالیاتی] ,[نوع اظهارنامه] ,[نام اداره]) as o

    ON main.[منبع مالیاتی]=o.[منبع مالیاتی]
    AND main.[نوع اظهارنامه]=o.[نوع اظهارنامه]
    AND main.[نام اداره]=o.[نام اداره]
    LEFT JOIN
        (SELECT 
        [منبع مالیاتی]
        ,[نوع اظهارنامه] ,[نام اداره]
        ,SUM(IIF(LEN([مالیات تشخیص])=0,0,cast([مالیات تشخیص] as numeric(24)))) [میزان مالیات تشخیصی پرونده های دارای برگ قطعی]
        FROM [TestDb].[dbo].[V_PORTAL]
    where [سال عملکرد]='1401'
    AND [منبع مالیاتی] in (N'مالیات بر درآمد مشاغل',N'مالیات بر درآمد شرکت ها')
    AND [نوع اظهارنامه]IN(N'مودیان مهم با ریسک بالا',N'رتبه ریسک بالا')
        AND LEN([تاريخ ايجاد برگ قطعي])>5
    GROUP BY [منبع مالیاتی] ,[نوع اظهارنامه] ,[نام اداره]) as p

    ON main.[منبع مالیاتی]=p.[منبع مالیاتی]
    AND main.[نوع اظهارنامه]=p.[نوع اظهارنامه]
    AND main.[نام اداره]=p.[نام اداره]
    LEFT JOIN
        ( SELECT 
        [منبع مالیاتی]
        ,[نوع اظهارنامه] ,[نام اداره]
        ,SUM(IIF(LEN([مالیات قطعی])=0,0,cast([مالیات قطعی] as numeric(24)))) [میزان مالیات قطعی پرونده های دارای برگ قطعی]
        FROM [TestDb].[dbo].[V_PORTAL]
    where [سال عملکرد]='1401'
    AND [منبع مالیاتی] in (N'مالیات بر درآمد مشاغل',N'مالیات بر درآمد شرکت ها')
    AND [نوع اظهارنامه]IN(N'مودیان مهم با ریسک بالا',N'رتبه ریسک بالا')
        AND LEN([تاريخ ايجاد برگ قطعي])>5
    GROUP BY [منبع مالیاتی] ,[نوع اظهارنامه] ,[نام اداره]) as q

    ON main.[منبع مالیاتی]=q.[منبع مالیاتی]
    AND main.[نوع اظهارنامه]=q.[نوع اظهارنامه]
    AND main.[نام اداره]=q.[نام اداره]
    LEFT JOIN
            (SELECT 
        [منبع مالیاتی]
        ,[نوع اظهارنامه] ,[نام اداره]
        ,COUNT(*)AS [تعداد برگ قطعی ابلاغ شده]
        FROM [TestDb].[dbo].[V_PORTAL]
    where [سال عملکرد]='1401'
    AND [منبع مالیاتی] in (N'مالیات بر درآمد مشاغل',N'مالیات بر درآمد شرکت ها')
    AND [نوع اظهارنامه]IN(N'مودیان مهم با ریسک بالا',N'رتبه ریسک بالا')
        AND LEN([تاریخ ابلاغ برگ قطعی])>5
    GROUP BY [منبع مالیاتی] ,[نوع اظهارنامه] ,[نام اداره]) as r

    ON main.[منبع مالیاتی]=r.[منبع مالیاتی]
    AND main.[نوع اظهارنامه]=r.[نوع اظهارنامه]
    AND main.[نام اداره]=r.[نام اداره]
"""


def get_sql_query_GhateeEblaghNashodeSanim():
    return """
SELECT 
[شماره اقتصادی]
,replace(REPLACE(REPLACE([نام مودی], CHAR(13), ''), CHAR(10), ''),'	','') AS [نام مودی]
,[کد رهگيري ثبت نام ]
,[کد اداره]
,[نام اداره]
,[شماره اظهارنامه]
,[سال عملکرد]
,[منبع مالیاتی]
,[دوره عملکرد]
,[شماره برگ تشخیص]
,[تاریخ صدور برگ تشخیص (تاریخ تایید گزارش حسابرسی)]
,[مالیات تشخیص]
,[مالیات قطعی]
,[تاریخ ابلاغ برگ تشخیص]
,[شماره برگ قطعی]
,[تاريخ ايجاد برگ قطعي]
,[تاریخ ابلاغ برگ قطعی]
FROM [testdbv2].[dbo].[V_PORTAL]
WHERE 
[منبع مالیاتی] IN (N'مالیات بر درآمد مشاغل',N'مالیات بر درآمد شرکت ها',N'مالیات بر ارزش افزوده' )
AND LEN([تاريخ ايجاد برگ قطعي])>5
AND LEN([تاریخ ابلاغ برگ قطعی])<5
"""


def get_sql_query_TashkhisEblaghNashdeSanim():
    return """
            SELECT 
            [شماره اقتصادی]
            ,replace(REPLACE(REPLACE([نام مودی], CHAR(13), ''), CHAR(10), ''),'	','') AS [نام مودی]
            ,[کد رهگيري ثبت نام ]
            ,[کد اداره]
            ,[نام اداره]
            ,[شماره اظهارنامه]
            ,[سال عملکرد]
            ,[منبع مالیاتی] 
            ,[دوره عملکرد]
            ,[شناسه حسابرسی]
            ,[شماره برگ تشخیص]
            ,[تاریخ صدور برگ تشخیص (تاریخ تایید گزارش حسابرسی)]
            ,[مالیات تشخیص]
            ,[تاریخ ابلاغ برگ تشخیص]
            ,[شماره برگ قطعی]
            ,[تاريخ ايجاد برگ قطعي]
            ,[ابلاغ الکترونیک برگ تشخیص]

            FROM [TestDbv2].[dbo].[V_PORTAL]
            WHERE [منبع مالیاتی] IN (N'مالیات بر درآمد مشاغل',N'مالیات بر درآمد شرکت ها',N'مالیات بر ارزش افزوده' )
                AND SUBSTRING([سال عملکرد],1,4)>='1397'
                AND LEN([تاریخ صدور برگ تشخیص (تاریخ تایید گزارش حسابرسی)])>5
                AND LEN([تاریخ ابلاغ برگ تشخیص])<5
"""


def get_sql_query_AmadeErsalBeHeiatSanim():
    return """
SELECT 
	   [شماره اقتصادی]
      ,[نام مودی]
      ,[کد رهگيري ثبت نام ]
      ,[کد اداره]
      ,[نام اداره]
      ,[شماره اظهارنامه]
      ,[سال عملکرد]
      ,[منبع مالیاتی]
      ,[دوره عملکرد]
      ,[تاریخ صدور برگ تشخیص (تاریخ تایید گزارش حسابرسی)]
	  	  ,[شماره برگ تشخیص]
	   ,[مالیات تشخیص]
      ,[تاریخ ابلاغ برگ تشخیص]
	  ,[ابلاغ الکترونیک برگ تشخیص]
	  ,[نوع ابلاغ برگ تشخیص]
	  ,[تاریخ اعتراض] 
	  ,[تاریخ رای] AS [تاریخ رای 238]
	  ,[تاریخ اعتراض هیات بدوی]
	  ,[تاريخ ايجاد برگ قطعي]
	  ,[مالیات قطعی]
	 ,[186Report].[dbo].DateDiff_persian([تاریخ اعتراض],[186Report].[dbo].[UDF_Gregorian_To_Persian](REPLACE(CONVERT(DATE,GETDATE()),'-',''))) AS [مدت زمان گذشته از اعتراض]
  FROM [TestDb].[dbo].[V_PORTAL]
  WHERE 
  ((LEN([تاریخ اعتراض])>5 
  AND LEN([تاریخ رای])<5 
  AND [186Report].[dbo].DateDiff_persian([تاریخ اعتراض],[186Report].[dbo].[UDF_Gregorian_To_Persian](REPLACE(CONVERT(DATE,GETDATE()),'-','')))>45
  )
  OR
  (
  ([نوع ابلاغ برگ تشخیص]=N'قانونی' AND LEN([تاریخ اعتراض])<5)
  OR
  ([نوع ابلاغ برگ تشخیص]=N'قانونی' AND [186Report].[dbo].DateDiff_persian([تاریخ اعتراض],[186Report].[dbo].[UDF_Gregorian_To_Persian](REPLACE(CONVERT(DATE,GETDATE()),'-','')))>45)  
  ))
  AND LEN([تاریخ ابلاغ برگ تشخیص])>5
  AND LEN([تاريخ بسته شدن اعتراض يا شکايت])<5
  AND LEN([تاریخ اعتراض هیات بدوی])<5
  AND LEN([تاریخ اعتراض هیات تجدید نظر])<5
  AND LEN([تاريخ ايجاد برگ قطعي])<5
"""


def get_sql_query_EterazMotevaghefShode238Sanim():
    return """
SELECT  [شماره اقتصادی]
      ,[نام مودی]
      ,[کد رهگيري ثبت نام ]
      ,[کد اداره]
      ,[نام اداره]
      ,[شماره اظهارنامه]
      ,[سال عملکرد]
      ,[منبع مالیاتی]
      ,[دوره عملکرد]
      ,[تاریخ صدور برگ تشخیص (تاریخ تایید گزارش حسابرسی)]
	  ,[شماره برگ تشخیص]
	  ,[مالیات تشخیص]
      ,[تاریخ ابلاغ برگ تشخیص]
	  ,[ابلاغ الکترونیک برگ تشخیص]
	  ,[نوع ابلاغ برگ تشخیص]
	  ,[تاریخ اعتراض] 
	  ,[توافق]AS [توافق با اداره]
	  ,[تاریخ رای] AS [تاریخ رای 238]
	  ,[تاریخ اعتراض هیات بدوی]
	  ,[تاريخ ايجاد برگ قطعي]
	  ,[مالیات قطعی]
	  ,[186Report].[dbo].DateDiff_persian([تاریخ اعتراض],[186Report].[dbo].[UDF_Gregorian_To_Persian](REPLACE(CONVERT(DATE,GETDATE()),'-',''))) AS [مدت زمان گذشته از اعتراض]
	  ,[186Report].[dbo].DateDiff_persian([تاریخ ابلاغ برگ تشخیص],[186Report].[dbo].[UDF_Gregorian_To_Persian](REPLACE(CONVERT(DATE,GETDATE()),'-',''))) [مدت زمان گذشته از ابلاغ تشخیص]
  FROM [testdbv2].[dbo].[V_PORTAL]
  WHERE 

	 LEN([تاریخ صدور برگ تشخیص (تاریخ تایید گزارش حسابرسی)])>5
	 AND LEN([تاریخ ابلاغ برگ تشخیص])>5
	 AND LEN([تاریخ اعتراض])>5
	 AND LEN([تاریخ اعتراض هیات بدوی])<5
	 AND LEN([تاريخ ايجاد برگ قطعي])<5
"""


def get_sql_query_EjraEblaghShodeDarayeCaseBazVaMandeBedehiSanim():
    return """
SELECT * FROM
(SELECT 

[کد یکتای داخلی مؤدی]
      ,[شماره اقتصادی]
	  ,[کد رهگيري ثبت نام ]
      ,[نام مودی]
      ,[نام پدر]
	  ,[کد اداره]
	  ,[نام اداره]
      ,[تاریخ صدور برگ تشخیص (تاریخ تایید گزارش حسابرسی)]
      ,[تاریخ ابلاغ برگ تشخیص]
	  ,[سال عملکرد]
	  ,[منبع مالیاتی]
      ,replace([شماره اظهارنامه],'.0','')[شماره اظهارنامه]
      ,[تاريخ ايجاد برگ قطعي]
     ,[دوره عملکرد]
      ,[شماره برگ قطعی]
     
      ,[تاریخ ابلاغ برگ قطعی]
     
      ,[تاریخ ایجاد برگ اجرا]
      ,[تاریخ ابلاغ برگ اجرا]
     
      ,[پرداختی از اصل مالیات]
      ,[مانده بدهی از اصل مالیات]
      ,[اعتبار ابرازی]
      ,[SOURCE_EJRA]
      ,[آخرین بروزرسانی]
  FROM [TestDb].[dbo].[V_PORTAL]
  WHERE 
  LEN([تاریخ ابلاغ برگ قطعی])>5
  AND LEN([تاریخ ایجاد برگ اجرا])>5
  AND LEN([تاریخ ابلاغ برگ اجرا])>5
  AND [تاریخ ایجاد برگ اجرا] > [تاریخ ابلاغ برگ قطعی]
and [SOURCE_EJRA] NOT IN('TAX','TAM','TAD')
) as t


 JOIN

(
SELECT 
distinct
[شناسه داخلي سامانه سنیم]
      ,[شماره اظهارنامه]
  
      ,[وضعیت کیس اجرایی]
      ,[تاریخ وضعیت]
      ,[نوع کیس]
      ,[تاریخ تولید کیس]
      ,[سال عملکرد]
      ,[دوره مالیاتی]
      ,[منبع مالیاتی]
      ,[تاریخ اختتام وصول اجرا]
      ,[کد اداره کل]
      ,[آخرین بروزرسانی]
  FROM [TestDb].[dbo].[V_CASEEJRA]
  where [وضعیت کیس اجرایی]<>'CAS_CLSD'
)as tblcase
ON t.[کد یکتای داخلی مؤدی]=tblcase.[شناسه داخلي سامانه سنیم]
and 
cast(t.[شماره اظهارنامه] as numeric)=cast(tblcase.[شماره اظهارنامه] as numeric)
and substring(t.[سال عملکرد],1,4)=substring(tblcase.[سال عملکرد],1,4)
and t.[منبع مالیاتی]=tblcase.[منبع مالیاتی]
and substring(t.[دوره عملکرد],1,1)=substring(tblcase.[دوره مالیاتی],1,1)
WHERE  IIF(LEN([مانده بدهی از اصل مالیات])=0,0,cast([مانده بدهی از اصل مالیات] as numeric(24))) >0
"""


def get_sql_query_EjraEblaghNaShodeDarayeMandeBedehiEbraziSanim():
    return """
SELECT 
[شماره اقتصادی]
,replace(REPLACE(REPLACE([نام مودی], CHAR(13), ''), CHAR(10), ''),'	','') AS [نام مودی]
,[کد رهگيري ثبت نام ]
,[کد اداره]
,[نام اداره]
,[شماره اظهارنامه]
,[سال عملکرد]
,[منبع مالیاتی] 
,[دوره عملکرد]
,[شناسه حسابرسی]
,[مالیات ابرازی]
,[شماره برگ تشخیص]
,[تاریخ صدور برگ تشخیص (تاریخ تایید گزارش حسابرسی)]
,[مالیات تشخیص]
,[تاریخ ابلاغ برگ تشخیص]
,[شماره برگ قطعی]
,[تاريخ ايجاد برگ قطعي]
,[ابلاغ الکترونیک برگ تشخیص]
,[شماره برگ اجرا]
,[تاریخ ایجاد برگ اجرا]
,[تاریخ ابلاغ برگ اجرا]
,[پرداختی از اصل مالیات]
,[مانده بدهی از اصل مالیات]
,[میزان مبلغ بخشودگی]
,IIF(LEN([مالیات ابرازی])=0,0,cast([مالیات ابرازی] as numeric(24)))-IIF(LEN([پرداختی از اصل مالیات])=0,0,cast([پرداختی از اصل مالیات] as numeric(24))) as [مانده ابرازی]
FROM [TestDbv2].[dbo].[V_PORTAL]
WHERE [منبع مالیاتی] IN (N'مالیات بر درآمد مشاغل',N'مالیات بر درآمد شرکت ها',N'مالیات بر ارزش افزوده' )
      AND LEN([تاريخ ايجاد برگ قطعي])<5
      AND LEN([تاریخ ایجاد برگ اجرا])>5
      AND LEN([تاریخ ابلاغ برگ اجرا])<5
	  AND IIF(LEN([مالیات ابرازی])=0,0,cast([مالیات ابرازی] as numeric(24)))-IIF(LEN([پرداختی از اصل مالیات])=0,0,cast([پرداختی از اصل مالیات] as numeric(24)))>0 
"""


def get_sql_query_EjraEblaghNaShodeDarayeMandeBedehiGhateeSanim():
    return """
SELECT 
[شماره اقتصادی]
,replace(REPLACE(REPLACE([نام مودی], CHAR(13), ''), CHAR(10), ''),'	','') AS [نام مودی]
,[کد رهگيري ثبت نام ]
,[کد اداره]
,[نام اداره]
,[شماره اظهارنامه]
,[سال عملکرد]
,[منبع مالیاتی] 
,[دوره عملکرد]
,[شناسه حسابرسی]
,[شماره برگ تشخیص]
,[تاریخ صدور برگ تشخیص (تاریخ تایید گزارش حسابرسی)]
,[مالیات تشخیص]
,[تاریخ ابلاغ برگ تشخیص]
,[شماره برگ قطعی]
,[تاريخ ايجاد برگ قطعي]
,[ابلاغ الکترونیک برگ تشخیص]
,[شماره برگ اجرا]
,[تاریخ ایجاد برگ اجرا]
,[تاریخ ابلاغ برگ اجرا]
,[پرداختی از اصل مالیات]
,[مانده بدهی از اصل مالیات]
,[میزان مبلغ بخشودگی]
FROM [TestDbv2].[dbo].[V_PORTAL]
WHERE [منبع مالیاتی] IN (N'مالیات بر درآمد مشاغل',N'مالیات بر درآمد شرکت ها',N'مالیات بر ارزش افزوده' )
      AND LEN([تاريخ ايجاد برگ قطعي])>5
      AND LEN([تاریخ ایجاد برگ اجرا])>5
      AND LEN([تاریخ ابلاغ برگ اجرا])<5
	    AND IIF(LEN([مانده بدهی از اصل مالیات])=0,0,cast([مانده بدهی از اصل مالیات] as numeric(24))) >0
"""


def get_sql_query_EjraEblaghNaShodeDarayeMandeBedehiSanim():
    return """
SELECT 
[شماره اقتصادی]
,replace(REPLACE(REPLACE([نام مودی], CHAR(13), ''), CHAR(10), ''),'	','') AS [نام مودی]
,[کد رهگيري ثبت نام ]
,[کد اداره]
,[نام اداره]
,[شماره اظهارنامه]
,[سال عملکرد]
,[منبع مالیاتی] 
,[دوره عملکرد]
,[شناسه حسابرسی]
,[شماره برگ تشخیص]
,[تاریخ صدور برگ تشخیص (تاریخ تایید گزارش حسابرسی)]
,[مالیات تشخیص]
,[تاریخ ابلاغ برگ تشخیص]
,[شماره برگ قطعی]
,[تاريخ ايجاد برگ قطعي]
,[ابلاغ الکترونیک برگ تشخیص]
,[شماره برگ اجرا]
,[تاریخ ایجاد برگ اجرا]
,[تاریخ ابلاغ برگ اجرا]
,[پرداختی از اصل مالیات]
,[مانده بدهی از اصل مالیات]
,[میزان مبلغ بخشودگی]
FROM [TestDbv2].[dbo].[V_PORTAL]
WHERE [منبع مالیاتی] IN (N'مالیات بر درآمد مشاغل',N'مالیات بر درآمد شرکت ها',N'مالیات بر ارزش افزوده' )
      AND LEN([تاريخ ايجاد برگ قطعي])>5
      AND LEN([تاریخ ایجاد برگ اجرا])>5
      AND LEN([تاریخ ابلاغ برگ اجرا])<5
	    AND IIF(LEN([مانده بدهی از اصل مالیات])=0,0,cast([مانده بدهی از اصل مالیات] as numeric(24))) >0
	
"""


def get_sql_query_AmadeGhateesaziSanim():
    return """
SELECT [شماره اقتصادی]
      ,[نام مودی]
      ,[کد رهگيري ثبت نام ]
      ,[کد اداره]
      ,[نام اداره]
      ,[شماره اظهارنامه]
      ,[سال عملکرد]
      ,[منبع مالیاتی]
      ,[دوره عملکرد]
      ,[تاریخ صدور برگ تشخیص (تاریخ تایید گزارش حسابرسی)]
	  ,[شماره برگ تشخیص]
	  ,[مالیات تشخیص]
      ,[تاریخ ابلاغ برگ تشخیص]
	  ,[ابلاغ الکترونیک برگ تشخیص]
	  ,[نوع ابلاغ برگ تشخیص]
	  ,[تاریخ اعتراض] 
	  ,[توافق]AS [توافق با اداره]
	  ,[تاریخ رای] AS [تاریخ رای 238]
	  ,[تاریخ اعتراض هیات بدوی]
	  ,[تاریخ رای بدوی]
	  ,[186Report].[dbo].[UDF_Gregorian_To_Persian](REPLACE(CONVERT(DATE,[تاريخ ابلاغ رای بدوی]),'-','')) AS [تاريخ ابلاغ رای بدوی]
	  ,[تاریخ اعتراض هیات تجدید نظر]
	  ,[تاریخ رای تجدید نظر]
	  ,[تاریخ ابلاغ رای تجدید نظر]
	  ,[تاريخ ايجاد برگ قطعي]
	  ,[مالیات قطعی]
	  ,[186Report].[dbo].DateDiff_persian([تاریخ اعتراض],[186Report].[dbo].[UDF_Gregorian_To_Persian](REPLACE(CONVERT(DATE,GETDATE()),'-',''))) AS [مدت زمان گذشته از اعتراض]
	  ,[186Report].[dbo].DateDiff_persian([تاریخ ابلاغ برگ تشخیص],[186Report].[dbo].[UDF_Gregorian_To_Persian](REPLACE(CONVERT(DATE,GETDATE()),'-',''))) [مدت زمان گذشته از ابلاغ تشخیص]
	  ,'' AS  [مدت زمان گذشته از ابلاغ رای هیات]
      ,N'آماده قطعی بر اساس سکوت مودی به برگ تشخیص' AS [وضعیت]
  FROM [testdbv2].[dbo].[V_PORTAL]
  WHERE 

	 LEN([تاریخ صدور برگ تشخیص (تاریخ تایید گزارش حسابرسی)])>5
	 AND LEN([تاریخ ابلاغ برگ تشخیص])>5
	 AND [نوع ابلاغ برگ تشخیص]<>N'قانونی'
	 AND LEN([تاریخ اعتراض])<5
	 AND LEN([تاریخ اعتراض هیات بدوی])<5
	 AND LEN([تاریخ اعتراض هیات تجدید نظر])<5
	 AND LEN([تاريخ ايجاد برگ قطعي])<5
	 AND [186Report].[dbo].DateDiff_persian([تاریخ ابلاغ برگ تشخیص],[186Report].[dbo].[UDF_Gregorian_To_Persian](REPLACE(CONVERT(DATE,GETDATE()),'-','')))>30
	 AND [منبع مالیاتی] IN (N'مالیات بر درآمد مشاغل',N'مالیات بر درآمد شرکت ها',N'مالیات بر ارزش افزوده' )
	 UNION


	 SELECT  [شماره اقتصادی]
      ,[نام مودی]
      ,[کد رهگيري ثبت نام ]
      ,[کد اداره]
      ,[نام اداره]
      ,[شماره اظهارنامه]
      ,[سال عملکرد]
      ,[منبع مالیاتی]
      ,[دوره عملکرد]
      ,[تاریخ صدور برگ تشخیص (تاریخ تایید گزارش حسابرسی)]
	  ,[شماره برگ تشخیص]
	  ,[مالیات تشخیص]
      ,[تاریخ ابلاغ برگ تشخیص]
	  ,[ابلاغ الکترونیک برگ تشخیص]
	  ,[نوع ابلاغ برگ تشخیص]
	  ,[تاریخ اعتراض] 
	  ,[توافق]AS [توافق با اداره]
	  ,[تاریخ رای] AS [تاریخ رای 238]
	  ,[تاریخ اعتراض هیات بدوی]
	  ,[تاریخ رای بدوی]
	  ,[186Report].[dbo].[UDF_Gregorian_To_Persian](REPLACE(CONVERT(DATE,[تاريخ ابلاغ رای بدوی]),'-','')) AS [تاريخ ابلاغ رای بدوی]
	  ,[تاریخ اعتراض هیات تجدید نظر]
	  ,[تاریخ رای تجدید نظر]
	  ,[تاریخ ابلاغ رای تجدید نظر]
	  ,[تاريخ ايجاد برگ قطعي]
	  ,[مالیات قطعی]
	  ,[186Report].[dbo].DateDiff_persian([تاریخ اعتراض],[186Report].[dbo].[UDF_Gregorian_To_Persian](REPLACE(CONVERT(DATE,GETDATE()),'-',''))) AS [مدت زمان گذشته از اعتراض]
	  ,[186Report].[dbo].DateDiff_persian([تاریخ ابلاغ برگ تشخیص],[186Report].[dbo].[UDF_Gregorian_To_Persian](REPLACE(CONVERT(DATE,GETDATE()),'-',''))) [مدت زمان گذشته از ابلاغ تشخیص]
	  ,[186Report].[dbo].DateDiff_persian([186Report].[dbo].[UDF_Gregorian_To_Persian](REPLACE(CONVERT(DATE,[تاريخ ابلاغ رای بدوی]),'-','')),[186Report].[dbo].[UDF_Gregorian_To_Persian](REPLACE(CONVERT(DATE,GETDATE()),'-',''))) [مدت زمان گذشته از ابلاغ رای هیات]
      ,N'آماده قطعی بر اساس سکوت مودی نسبت به رای هیات بدوی' AS [وضعیت]
  FROM [testdbv2].[dbo].[V_PORTAL]
  WHERE 

	 LEN([تاریخ صدور برگ تشخیص (تاریخ تایید گزارش حسابرسی)])>5
	 AND LEN([تاریخ ابلاغ برگ تشخیص])>5
	 AND LEN([تاریخ اعتراض هیات بدوی])>5
	 AND LEN([تاريخ ابلاغ رای بدوی])>5
	 AND LEN([تاریخ اعتراض هیات تجدید نظر])<5
	 AND LEN([تاريخ ايجاد برگ قطعي])<5
	 AND [186Report].[dbo].DateDiff_persian([186Report].[dbo].[UDF_Gregorian_To_Persian](REPLACE(CONVERT(DATE,[تاريخ ابلاغ رای بدوی]),'-','')),[186Report].[dbo].[UDF_Gregorian_To_Persian](REPLACE(CONVERT(DATE,GETDATE()),'-','')))>20
	 AND [منبع مالیاتی] IN (N'مالیات بر درآمد مشاغل',N'مالیات بر درآمد شرکت ها',N'مالیات بر ارزش افزوده' )
"""


def get_sql_query_BadviEblaghNashodeSanim():
    return """
SELECT * FROM [TestDb].[dbo].[tblbadvitakmilshode1392] WHERE [تاریخ ابلاغ رای بدوی]=''
UNION
SELECT * FROM [TestDb].[dbo].[tblbadvitakmilshode1393] WHERE [تاریخ ابلاغ رای بدوی]=''
UNION
SELECT * FROM [TestDb].[dbo].[tblbadvitakmilshode1394] WHERE [تاریخ ابلاغ رای بدوی]=''
UNION
SELECT * FROM [TestDb].[dbo].[tblbadvitakmilshode1395] WHERE [تاریخ ابلاغ رای بدوی]=''
UNION
SELECT * FROM [TestDb].[dbo].[tblbadvitakmilshode1396] WHERE [تاریخ ابلاغ رای بدوی]=''
UNION
SELECT * FROM [TestDb].[dbo].[tblbadvitakmilshode1397] WHERE [تاریخ ابلاغ رای بدوی]=''
UNION
SELECT * FROM [TestDb].[dbo].[tblbadvitakmilshode1398] WHERE [تاریخ ابلاغ رای بدوی]=''
UNION
SELECT * FROM [TestDb].[dbo].[tblbadvitakmilshode1399] WHERE [تاریخ ابلاغ رای بدوی]=''
UNION
SELECT * FROM [TestDb].[dbo].[tblbadvitakmilshode1400] WHERE [تاریخ ابلاغ رای بدوی]=''
UNION
SELECT * FROM [TestDb].[dbo].[tblbadvitakmilshode1401] WHERE [تاریخ ابلاغ رای بدوی]=''
"""


def get_sql_query_TajdidnazarEblaghNashodeSanim():
    return """
SELECT * FROM [TestDb].[dbo].[tbltajdidnazartakmilshode1392] WHERE [تاریخ ابلاغ رای تجدید نظر]=''
UNION
SELECT * FROM [TestDb].[dbo].[tbltajdidnazartakmilshode1393] WHERE [تاریخ ابلاغ رای تجدید نظر]=''
UNION
SELECT * FROM [TestDb].[dbo].[tbltajdidnazartakmilshode1394] WHERE [تاریخ ابلاغ رای تجدید نظر]=''
UNION
SELECT * FROM [TestDb].[dbo].[tbltajdidnazartakmilshode1395] WHERE [تاریخ ابلاغ رای تجدید نظر]=''
UNION
SELECT * FROM [TestDb].[dbo].[tbltajdidnazartakmilshode1396] WHERE [تاریخ ابلاغ رای تجدید نظر]=''
UNION
SELECT * FROM [TestDb].[dbo].[tbltajdidnazartakmilshode1397] WHERE [تاریخ ابلاغ رای تجدید نظر]=''
UNION
SELECT * FROM [TestDb].[dbo].[tbltajdidnazartakmilshode1398] WHERE [تاریخ ابلاغ رای تجدید نظر]=''
UNION
SELECT * FROM [TestDb].[dbo].[tbltajdidnazartakmilshode1399] WHERE [تاریخ ابلاغ رای تجدید نظر]=''
UNION
SELECT * FROM [TestDb].[dbo].[tbltajdidnazartakmilshode1400] WHERE [تاریخ ابلاغ رای تجدید نظر]=''
UNION
SELECT * FROM [TestDb].[dbo].[tbltajdidnazartakmilshode1401] WHERE [تاریخ ابلاغ رای تجدید نظر]=''
"""
