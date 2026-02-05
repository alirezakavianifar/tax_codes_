from helpers import connect_to_sql, get_sql_con
from sql_queries import get_sql_alltables, get_sql_if_column_in_table
import pandas as pd


df = pd.read_excel(r'E:\automating_reports_V2\saved_dir\r.xls')


sql_query = """
select case status when 1 then 'انتساب'   else 'عدم انتساب' end as 'وضعيت',
row 'رديف', Start_Date As 'تاريخ شروع',End_Date As 'تاريخ خاتمه',
Masool_Name as 'مسئول گروه' ,
New_User_Name as 'اعضاء گروه',
Comment as 'علت',
Code_Parvandeh as 'شماره پرونده',
k_parvand
from Hokm_last_inf
where isnull(hokm_kind,1)=1  and Residegi_Kind=0 and cod_hozeh >= 160000 and cod_hozeh <= 169999 and sal >= '1401' and sal <= '1401' 
and k_parvand='111122/000'
order by cod_hozeh,convert(float,replace(k_parvand,'/','.')),sal,row
"""

for index, row in df.iterrows():

    sql_query = """
select case status when 1 then 'انتساب'   else 'عدم انتساب' end as 'وضعيت',
row 'رديف', Start_Date As 'تاريخ شروع',End_Date As 'تاريخ خاتمه',
Masool_Name as 'مسئول گروه' ,
New_User_Name as 'اعضاء گروه',
Comment as 'علت',
Code_Parvandeh as 'شماره پرونده',
k_parvand
from Hokm_last_inf
where isnull(hokm_kind,1)=1  and Residegi_Kind=0 and cod_hozeh >= 160000 and cod_hozeh <= 169999 and sal >= '1401' and sal <= '1401' 
order by cod_hozeh,convert(float,replace(k_parvand,'/','.')),sal,row
"""

    try:
        record = connect_to_sql(sql_query, sql_con=get_sql_con(
            database='MASHAGHEL', server='10.52.0.50\AHWAZ', username='mash', password='123456'),
            read_from_sql=True, return_df=True, num_runs=0)
        print(index)
        print('********************************************************************************')
    except Exception as e:
        print(e)
        continue
