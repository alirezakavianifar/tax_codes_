import pandas as pd
from helpers import find_edares
import numpy as np


def is_eblagh(x, type):
    if len(x) > 3:
        return '%s ابلاغ شده' % type
    return '%s ابلاغ نشده' % type


# read Data
df_ghatee = pd.read_excel(
    r'E:\automating_reports_V2\mostaghelat\Rpt_Deterministic(1).xls')
df_tashkhis = pd.read_excel(
    r'E:\automating_reports_V2\mostaghelat\Rpt_Diagnosis(1).xls')
# Determine Edares
df_ghatee_with_edare = find_edares(
    df_ghatee, report_typ='شماره برگ قطعی')
df_tashkhis_with_edare = find_edares(
    df_tashkhis, report_typ='شماره برگ تشخیص')
df_ghatee_with_edare['قطعی'] = df_ghatee_with_edare['تاریخ ابلاغ'].apply(
    lambda x: is_eblagh(str(x), 'قطعی'))
df_tashkhis_with_edare['تشخیص'] = df_tashkhis_with_edare['تاریخ ابلاغ'].apply(
    lambda x: is_eblagh(str(x), 'تشخیص'))

cols = ['شهرستان', 'کد اداره',
        'عملکرد', 'قطعی', 'جمع مالیات']

df_ghatee_with_edare = df_ghatee_with_edare[cols]

agg_g = df_ghatee_with_edare.groupby(['شهرستان', 'کد اداره', 'عملکرد', 'قطعی'])[
    'جمع مالیات'].agg(['sum', 'count'])


piv_agg_g = pd.pivot_table(agg_g, values=['count', 'sum'], index=['شهرستان', 'کد اداره', 'قطعی'],
                           columns=['عملکرد'
                                    ],
                           aggfunc=np.sum, fill_value=0).reset_index()\
    .to_excel(r'E:\automating_reports_V2\mostaghelat\ghateeMablagh3.xlsx')


df_g = df_ghatee_with_edare.groupby(['شهرستان', 'کد اداره', 'عملکرد',
                                     'قطعی']).size().reset_index()

dff_agg_g = pd.pivot_table(df_ghatee_with_edare[['شهرستان', 'کد اداره', 'عملکرد', 'قطعی', 'جمع مالیات']],
                           values=['جمع مالیات'], index=['شهرستان', 'کد اداره'],
                           columns=['عملکرد',
                                    'قطعی'],
                           aggfunc=np.sum, fill_value=0).reset_index().\
    to_excel(r'E:\automating_reports_V2\mostaghelat\ghateeMablagh.xlsx')

df__g_sum = df_ghatee_with_edare[['شهرستان', 'کد اداره', 'عملکرد', 'قطعی', 'جمع مالیات']].\
    groupby(['شهرستان', 'کد اداره', 'عملکرد',
             'قطعی']).sum()

df_g.rename(columns={0: 'تعداد'},
            inplace=True)

df_t = df_tashkhis_with_edare.groupby(['شهرستان', 'کد اداره', 'عملکرد',
                                       'تشخیص']).size().reset_index()
df_t.rename(columns={0: 'تعداد'},
            inplace=True)

dff_agg_g = pd.pivot_table(df_g, values=['تعداد'], index=['شهرستان', 'کد اداره'],
                           columns=['عملکرد',
                                    'قطعی'],
                           aggfunc=np.sum, fill_value=0).reset_index().to_excel(r'E:\automating_reports_V2\mostaghelat\ghatee.xlsx')


dff_agg_g = pd.pivot_table(df_t, values=['تعداد'], index=['شهرستان', 'کد اداره'],
                           columns=['عملکرد',
                                    'تشخیص'],
                           aggfunc=np.sum, fill_value=0).reset_index().to_excel(r'E:\automating_reports_V2\mostaghelat\tashkhis.xlsx')
