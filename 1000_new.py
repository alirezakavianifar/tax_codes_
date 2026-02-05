from helpers import merge_multiple_excel_files
import pandas as pd
import numpy as np

years = [1396,1397,1398,1399]
sources = ['مالیات بر درآمد مشاغل','مالیات بر درآمد شرکت ها']
merge_multiple_excel_files(r'D:\excel\New folder (2)', r'D:\excel\New folder (2)')

df = pd.read_excel(r'D:\excel\New folder (2)\Merged.xlsx')
cols = df.columns

df_ghatee = df[~df['شماره برگ قطعی'].isnull()]


df_no_ghatee = df[df['شماره برگ قطعی'].isnull()]

df_heiat = df_no_ghatee[~df_no_ghatee['تاریخ صدور برگ دعوت هیات حل اختلاف بدوی'].isnull()]

df_tashkhis_eblagh = df_no_ghatee.merge(df_heiat,how='outer', left_on='شماره اظهارنامه', right_on='شماره اظهارنامه', suffixes=('', '_y'), indicator=True)
df_tashkhis_eblagh.drop(df_tashkhis_eblagh.filter(regex='_y$').columns, axis=1, inplace=True)
df_tashkhis_eblagh = df_tashkhis_eblagh.loc[df_tashkhis_eblagh['_merge']=='left_only']
df_tashkhis_eblagh_nashode = df_tashkhis_eblagh[df_tashkhis_eblagh['تاریخ ابلاغ برگ تشخیص'].isnull()]
df_tashkhis_eblagh = df_tashkhis_eblagh[~df_tashkhis_eblagh['تاریخ ابلاغ برگ تشخیص'].isnull()]


df_tashkhis_eblagh_nashode.rename(columns={'_merge':'آخرین وضعیت'}, inplace=True)
df_tashkhis_eblagh_nashode['آخرین وضعیت'] = 'تشخیص ابلاغ نشده'
df_ghatee['آخرین وضعیت'] = 'قطعی صادر شده'
df_heiat['آخرین وضعیت'] = 'هیات حل اختلاف'
df_tashkhis_eblagh.rename(columns={'_merge':'آخرین وضعیت'}, inplace=True)
df_tashkhis_eblagh['آخرین وضعیت'] = 'تشخیص ابلاغ شده'

df_merged = pd.concat([df_heiat, df_tashkhis_eblagh, df_tashkhis_eblagh_nashode, df_ghatee])
df_merged.to_excel(r'D:\excel\New folder (2)\1000p.xlsx')

lst = []
new_df = pd.concat(lst)
for year in years:
    for source in sources:
        
        df_new = df_merged.loc[(df_merged['منبع مالیاتی'] == source) & (df_merged['سال عملکرد'] == int(year))]
        df_new = df_new.sort_values('میزان مالیات تشخیصی', ascending=False).head(100)
        lst.append(df_new)
        df_new.to_excel(r'D:\Excel\%s-%s.xlsx' % (source, year))
        
        p_df_1 = pd.pivot_table(new_df, values='کد اداره فعلی مودی در سنیم',
                            index= 'نام اداره فعلی', columns=['منبع مالیاتی', 'سال عملکرد'], aggfunc=len, 
                            fill_value=0, margins=True, margins_name='جمع کلی')
        
        
        
for source in sources:
    df_new = df_new.sort_values('میزان مالیات تشخیصی', ascending=False).head(100)
    
    p_df_1 = pd.pivot_table(new_df, values='کد اداره فعلی مودی در سنیم',
                            index= 'نام اداره فعلی', columns=['منبع مالیاتی', 'سال عملکرد','آخرین وضعیت'], aggfunc=len, 
                            fill_value=0, margins=True, margins_name='جمع کلی')
    
    df_new = pd.pivot_table(df, values='منبع مالیاتی', index=['نام اداره فعلی'], 
                            columns=['سال عملکرد'], 
                            aggfunc=len, 
                            fill_value=0)
    
    p_df_1.to_excel(r'D:\Excel\آمار-%s.xlsx' % (source))
    
    p_df_1.to_excel(r'D:\Excel\آمار جدید.xlsx')
                   
    
    



