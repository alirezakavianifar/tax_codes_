import numpy as np
import jdatetime

from automation.helpers import get_update_date


def is_modi_important(x, type='IMHR'):
    if x == type:
        return "بلی"
    return "خیر"


def map_work(x, df_tax_types):

    if x in df_tax_types.keys():
        return df_tax_types[x]
    return 'None'


def is_ghatee(x):
    if len(x) > 5:
        return True
    else:
        return False


def format_percentage(x):
    return f'{x:.2%}'


def is_residegi(x, message):
    if len(x) > 5:
        return f'{message} شده'
    return f'{message} نشده'


def count_tash_done(x, days=7):
    if len(x) < 8:
        return False
    else:
        if '/' in x:
            tash_date = np.array(x.split('/'), dtype=int)
        else:
            tash_date = [x[:4], x[4:6], x[6:8]]
        nw = get_update_date()
        nw_year = int(nw[:4])
        nw_month = int(nw[4:6])
        nw_day = int(nw[6:8])
        nw_date = jdatetime.date(nw_year, nw_month, nw_day, locale='fa_IR')
        tash_date = jdatetime.date(
            int(tash_date[0]), int(tash_date[1]), int(tash_date[2]), locale='fa_IR')
        diff = (nw_date - tash_date).days
        if diff < days:
            return True
        return False


def detect_last_condition(x):

    for item in x.index.tolist():
        if len(str(x['تاریخ ابلاغ برگ قطعی'])) > 5:
            return 'برگ قطعی ابلاغ شد'
        if len(str(x['شماره برگ قطعی'])) > 5:
            return 'برگ قطعی صادر شد'
        if len(str(x['تاریخ اعتراض هیات تجدید نظر'])) > 5:
            return 'هیات تجدید نظر'
        if len(str(x['تاريخ ايجاد برگ دعوت بدوی'])) > 5:
            return 'هیات بدوی'
        if x['توافق'] == 'Y':
            return 'دارای توافق'
        if len(str(x['شماره برگ تشخیص'])) > 5:
            return 'برگ تشخیص صادر شد'
        if len(str(x['تاریخ ابلاغ برگ تشخیص'])) > 5:
            return 'برگ تشخیص ابلاغ شد'
        if len(str(x['تاریخ ایجاد کیس حسابرسی'])) > 5:
            return 'در حال رسیدگی'
        if len(str(x['شماره برگ اجرا'])) > 5:
            return 'اجرائیات'

        return None


def get_ezhar_type(x):
    if x[-5:] == 'TERHR':
        return 'بر اساس تراکنش بانکی'
    elif x[-4:] == 'IMHR':
        return "مودیان مهم با ریسک بالا"
    elif x[-4:] == 'BDLR':
        return 'اظهارنامه های ارزش افزوده مشمول ضوابط اجرایی موضوع بند (ب)تبصره (6) قانون بودجه سال 1400'
    elif x[-4:] == 'T100':
        return 'تبصره 100'
    elif x[-4:] == 'ERHR':
        return 'رتبه ریسک بالا'
    elif x[-4:] == 'MRHR':
        return 'ریسک متوسط'
    elif x[-4:] == 'DMHR':
        return 'اظهارنامه برآوردی صفر'
    elif x[-3:] == 'ZHR':
        return 'اظهارنامه صفر دارای اطلاعات سیستمی'
    elif x[-2:] == 'LR':
        return 'رتبه ریسک پایین'
    elif x[-2:] == 'ZR':
        return 'اظهارنامه صفر فاقد اطلاعات سیستمی/جهت بررسی توسط اداره'
    elif x[-2:] == 'DM':
        return 'اظهارنامه برآوردی غیر صفر'
    elif x[-2:] == 'HR':
        return 'انتخاب شده بدون اعمال معیار ریسک'
    else:
        return 'نامشخص'
