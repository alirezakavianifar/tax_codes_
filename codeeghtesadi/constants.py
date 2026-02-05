import os
from selenium import webdriver
import socket
from dataclasses import dataclass










@dataclass
class Modi():
    melli: str = None
    sex: str = None
    name: str = None
    father_name: str = None
    dob: str = None
    id_num: str = None


@dataclass
class ModiHoghogh():
    melli_code: str = None
    name: str = None
    sur_name: str = None
    start_work_date: str = None
    sum_ongoing_gross_salary_rial: str = None
    gross_overtime_rial: str = None
    other_eventual_cash_payments_rial: str = None
    bonus_rial: str = None
    uncontious_outstanding_payments_received_rial: str = None
    non_cash_uncontinous_benefit_current_month_rial: str = None
    year_end_bonus_new_year_rial: str = None
    unused_leave_payments_amount_rial: str = None
    net_tax: str = None


@dataclass
class ModiHoghoghLst():
    melli_code: str = None
    name: str = None
    sur_name: str = None
    position: str = None


@dataclass
class Modi_pazerande():
    melli: str = None
    hoviyati: str = None
    code: str = None
    shomarepayane: str = None
    vaziat: str = None
    vaziatelsagh: str = None
    money1400: str = None
    money1401: str = None

















def get_server_namesV2():

    server_names = [
        ('ahwaz', r'10.52.0.50\ahwaz'),
        ('abadan', r'10.52.112.130\abadan'),
        ('khoramshahr', r'10.52.224.130\khoramshahr'),
        ('shadegan', r'10.53.64.130\SHADEGAN'),
        ('mahshahr', r'10.53.128.130\mahshahr'),
        ('bandaremam', r'10.53.208.130\BANDAREMAM'),
        ('hendijan', r'10.53.192.130\HENDIJAN'),
        ('shooshtar', r'10.53.96.130\SHOOSHTAR'),
        ('mis', r'10.53.144.130\MIS'),
        ('gotvand', r'10.53.112.130\GOTVAND'),
        ('shuosh', r'10.53.80.130\SHUOSH'),
        ('dezful', r'10.52.240.130\Dezful'),
        ('andimeshk', r'10.52.160.130\Andimeshk'),
        ('behbahan', r'10.52.208.130\Behbahan'),
        ('izeh', r'10.52.176.130\Izeh'),
        ('baghmalek', r'10.52.192.130\BAGHMALEK'),
        ('ramhormoz', r'10.53.16.130\ramhormoz'),
        ('haftkel', r'10.53.176.130\HAFTKEL'),
        ('omidieh', r'10.52.144.130\OMIDIEH'),
        ('aghajari', r'10.52.128.130\Aghajari'),
        ('ramshir', r'10.53.0.130\ramshir'),
        ('sosangerd', r'10.53.32.130\Susangerd'),
        # ('lali', r'10.53.160.130\lali'),
        # ('hoveyze', r'10.53.48.130\hoveyze'),
    ]

    return server_names


def get_report_links(report_type: str):
    if report_type == 'ezhar':
        return [
            '/html/body/form/div[2]/div/div[2]/\
                 main/div[2]/div/div/div/div/div/\
                 div/div[2]/div[2]/div[5]/div[1]/div/\
                 div[2]/table/tbody/tr[2]/td[5]/a',
            '/html/body/form/div[2]/div/div[2]/\
                 main/div[2]/div/div/div/div/div/\
                 div/div[2]/div[2]/div[5]/div[1]/div/\
                 div[2]/table/tbody/tr[2]/td[4]/a',
            '/html/body/form/div[2]/div/div[2]/\
                 main/div[2]/div/div/div/div/div/\
                 div/div[2]/div[2]/div[5]/div[1]/div/\
                 div[2]/table/tbody/tr[2]/td[8]/a'
        ]

    if report_type in ['tashkhis_sader_shode']:
        return [
            '/html/body/form/div[2]/div/div[2]/main/div[2]/div/div/div/div/font/div/div/div[2]/div[2]/div[5]/div[1]/div/div[2]/table/tbody/tr[2]/td[14]/a',
            '/html/body/form/div[2]/div/div[2]/main/div[2]/div/div/div/div/font/div/div/div[2]/div[2]/div[5]/div[1]/div/div[2]/table/tbody/tr[2]/td[15]/a',
            '/html/body/form/div[2]/div/div[2]/main/div[2]/div/div/div/div/font/div/div/div[2]/div[2]/div[5]/div[1]/div/div[2]/table/tbody/tr[2]/td[8]/a'
        ]

    if report_type in ['tashkhis_eblagh_shode']:
        return [
            '/html/body/form/div[2]/div/div[2]/main/div[2]/div/div/div/div/font/div/div/div[2]/div[2]/div[5]/div[1]/div/div[2]/table/tbody/tr[2]/td[4]/a',
            '/html/body/form/div[2]/div/div[2]/main/div[2]/div/div/div/div/font/div/div/div[2]/div[2]/div[5]/div[1]/div/div[2]/table/tbody/tr[2]/td[3]/a',
            '/html/body/form/div[2]/div/div[2]/main/div[2]/div/div/div/div/font/div/div/div[2]/div[2]/div[5]/div[1]/div/div[2]/table/tbody/tr[2]/td[7]/a'
        ]

    if report_type in ['ghatee_sader_shode']:
        return [
            '/html/body/form/div[2]/div/div[2]/main/div[2]/div/div/div/div/font/div/div/div[2]/div[2]/div[5]/div[1]/div/div[3]/table/tbody/tr[2]/td[14]/a',
            '/html/body/form/div[2]/div/div[2]/main/div[2]/div/div/div/div/font/div/div/div[2]/div[2]/div[5]/div[1]/div/div[3]/table/tbody/tr[2]/td[15]/a',
            '/html/body/form/div[2]/div/div[2]/main/div[2]/div/div/div/div/font/div/div/div[2]/div[2]/div[5]/div[1]/div/div[3]/table/tbody/tr[2]/td[8]/a'
        ]

    if report_type in ['ghatee_eblagh_shode']:
        return [
            '/html/body/form/div[2]/div/div[2]/main/div[2]/div/div/div/div/font/div/div/div[2]/div[2]/div[5]/div[1]/div/div[3]/table/tbody/tr[2]/td[4]/a',
            '/html/body/form/div[2]/div/div[2]/main/div[2]/div/div/div/div/font/div/div/div[2]/div[2]/div[5]/div[1]/div/div[3]/table/tbody/tr[2]/td[3]/a',
            '/html/body/form/div[2]/div/div[2]/main/div[2]/div/div/div/div/font/div/div/div[2]/div[2]/div[5]/div[1]/div/div[3]/table/tbody/tr[2]/td[7]/a'

        ]

    if report_type == 'tashkhis_eblagh_shode':
        return [
            '/html/body/form/div[2]/div/div[2]/main/div[2]/div/div/div/div/font/div/div\
                /div[2]/div[2]/div[5]/div[1]/div/div[2]/table/tbody/tr[2]/td[4]/a',
            '/html/body/form/div[2]/div/div[2]/main/div[2]/div/div/div/div/font/div\
                /div/div[2]/div[2]/div[5]/div[1]/div/div[2]/table/tbody/tr[2]/td[3]/a',
            '/html/body/form/div[2]/div/div[2]/main/div[2]/div/div/div/div/font/div/\
                div/div[2]/div[2]/div[5]/div[1]/div/div[2]/table/tbody/tr[2]/td[7]/a'
        ]


lst_years_arzeshafzoodeSonati = {
    'e:\\automating_reports_V2\\saved_dir\\arzeshafzoodeh_sonati\\frmnotAudited.xls': '1387',
    'e:\\automating_reports_V2\\saved_dir\\arzeshafzoodeh_sonati\\frmnotAudited(1).xls': '1388',
    'e:\\automating_reports_V2\\saved_dir\\arzeshafzoodeh_sonati\\frmnotAudited(2).xls': '1389',
    'e:\\automating_reports_V2\\saved_dir\\arzeshafzoodeh_sonati\\frmnotAudited(3).xls': '1390',
    'e:\\automating_reports_V2\\saved_dir\\arzeshafzoodeh_sonati\\frmnotAudited(4).xls': '1391',
    'e:\\automating_reports_V2\\saved_dir\\arzeshafzoodeh_sonati\\frmnotAudited(5).xls': '1392',
    'e:\\automating_reports_V2\\saved_dir\\arzeshafzoodeh_sonati\\frmnotAudited(6).xls': '1393',
    'e:\\automating_reports_V2\\saved_dir\\arzeshafzoodeh_sonati\\frmnotAudited(7).xls': '1394',
    'e:\\automating_reports_V2\\saved_dir\\arzeshafzoodeh_sonati\\frmnotAudited(8).xls': '1395',
    'e:\\automating_reports_V2\\saved_dir\\arzeshafzoodeh_sonati\\frmnotAudited(9).xls': '1396',
    'e:\\automating_reports_V2\\saved_dir\\arzeshafzoodeh_sonati\\frmnotAudited(10).xls': '1397',
    'e:\\automating_reports_V2\\saved_dir\\arzeshafzoodeh_sonati\\frmnotAudited(11).xls': '1398',
    'e:\\automating_reports_V2\\saved_dir\\arzeshafzoodeh_sonati\\frmnotAudited(12).xls': '1399',
    'e:\\automating_reports_V2\\saved_dir\\arzeshafzoodeh_sonati\\frmnotAudited(13).xls': '1400',
    'e:\\automating_reports_V2\\saved_dir\\arzeshafzoodeh_sonati\\frmnotAudited(14).xls': '1401',
}

years = [
    ('all', '0'),
    ('common_years', '1'),
    ('1392', '92'),
    ('1393', '93'),
    ('1394', '94'),
    ('1395', '95'),
    ('1396', '96'),
    ('1397', '97'),
    ('1398', '98'),
    ('1399', '99'),
    ('1400', '00'),
    ('1401', '01'),
]

dict_years = {
    'all': '0',
    'common_years': '1',
    '1387': '87',
    '1388': '88',
    '1389': '89',
    '1390': '90',
    '1391': '91',
    '1392': '92',
    '1393': '93',
    '1394': '94',
    '1395': '95',
    '1396': '96',
    '1397': '97',
    '1398': '98',
    '1399': '99',
    '1400': '00',
    '1401': '01',
}


def get_dict_years():
    return dict_years


all_years = years[0][1]



def get_all_years():
    return all_years





lst_reports = [
    ('common_reports', '0'),
    ('heiat_reports', '00'),
    ('ezhar', '1'),
    ('hesabrasi_darjarian_before5', '2'),
    ('hesabrasi_darjarian_after5', '3'),
    ('hesabrasi_takmil_shode', '4'),
    ('tashkhis_sader_shode', '5'),
    ('tashkhis_eblagh_shode', '6'),
    ('tashkhis_eblagh_nashode', '7'),
    ('tashkhis_motamam', '8'),
    ('eteraz_darjarian_dadrasi', '9'),
    ('eteraz_takmil_shode', '10'),
    ('badvi_darjarian_dadrasi', '11'),
    ('badvi_takmil_shode', '12'),
    ('tajdidnazer_darjarian_dadrasi', '13'),
    ('tajdidnazar_takmil_shode', '14'),
    ('ghatee_sader_shode', '15'),
    ('ghatee_eblagh_shode', '16'),
    ('ghatee_eblagh_nashode', '17'),
    ('ejraee_sader_shode', '18'),
    ('ejraee_eblagh_shode', '19'),
    ('case_ejraee', '20'),
    ('1000_parvande', '21'),
    ('badvi_darjarian_dadrasi_hamarz', '22'),
    ('amar_sodor_gharar_karshenasi', '23'),
    ('amar_sodor_ray', '24'),
    ('imp_parvand', '25'),
]

comm_reports = lst_reports[0][1]
heiat = lst_reports[1][1]


def get_comm_reports():
    return comm_reports


def get_heiat():
    return heiat


common_reports = [
    'ezhar',
    'tashkhis_sader_shode',
    'tashkhis_eblagh_shode',
    'ghatee_sader_shode',
    'ghatee_eblagh_shode',
]

heiat_reports = lst_reports[12:16]


def get_heiat_reports():
    return heiat_reports


common_years = years[5:]
comm_years = years[1][1]


def get_common_years():
    return common_years


def get_comm_years():
    return comm_years


def get_common_reports():
    return common_reports


def get_lst_reports():
    return lst_reports


def get_years():
    return years[2:]


def get_str_years():
    str_years = 'types:\n'
    for year in years:
        str_years += '%s=%s\n' % (year[0], year[1])

    return str_years


def get_str_help():
    str_help = 'Report types:'
    for item in lst_reports:
        str_help += '%s=%s,\n' % (item[0], item[1])

    return str_help


def get_ip():
    host_name = socket.gethostname()
    ip_addr = socket.gethostbyname(host_name)

    return ip_addr


def geck_location(set_save_dir=False, driver_type='firefox'):

    ip_addr = get_ip()

    if (set_save_dir and (ip_addr != '10.52.0.114')):
        return r'H:\پروژه اتوماسیون گزارشات\monthly_reports\saved_dir'

    if (set_save_dir and (ip_addr == '10.52.0.114')):
        return r'E:\automating_reports_V2\saved_dir'

    if ip_addr != '10.52.0.114' and driver_type == 'chrome':
        return r'E:\automating_reports_V2\chromedriver.exe'

    if ip_addr == '10.52.0.114' and driver_type == 'chrome':
        return r'E:\automating_reports_V2\chromedriver.exe'

    if ip_addr != '10.52.0.114':
        return r'D:\geckodriver.exe'

    else:
        cwd = os.path.join(os.getcwd(), 'geckodriver.exe')
        print(cwd)
        return cwd


def set_gecko_prefs(pathsave):
    fp = webdriver.FirefoxProfile()
    fp.set_preference('browser.download.folderList', 2)
    fp.set_preference('browser.download.manager.showWhenStarting', False)
    fp.set_preference('browser.download.dir', pathsave)
    fp.set_preference('browser.helperApps.neverAsk.openFile',
                      'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    fp.set_preference('browser.helperApps.neverAsk.saveToDisk',
                      'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')

    fp.set_preference('browser.helperApps.alwaysAsk.force', False)
    fp.set_preference('browser.download.manager.alertOnEXEOpen', False)
    fp.set_preference('browser.download.manager.focusWhenStarting', False)
    fp.set_preference('browser.download.manager.useWindow', False)
    fp.set_preference('browser.download.manager.showAlertOnComplete', False)
    fp.set_preference('browser.download.manager.closeWhenDone', False)

    return fp


def get_sql_con(server='.', database='testdb', username='sa', password='14579Ali'):

    constr = 'DRIVER={SQL Server};SERVER=' + server + ';DATABASE=' + \
        database + ';UID=' + username + ';PWD=' + password

    return constr


def get_remote_sql_con():
    server = '10.52.0.114'
    database = 'TestDb'
    username = 'sa'
    password = '14579Ali'
    constr = 'DRIVER={SQL Server};SERVER=' + server + ';DATABASE=' + \
        database + ';UID=' + username + ';PWD=' + password

    return constr


def get_soratmoamelat_mapping():
    return {
        'اطلاعات کارفرما در پيمان هاي بلند مدت': 'tblSorammoamelatKarfarmaPeymanBoland',
        'اجاره': 'tblSorammoamelatEjare',
        'فروش': 'tblSorammoamelatForosh',
        'اطلاعات پيمانکار در پيمان هاي بلند مدت': 'tblSorammoamelatPeymankarPeymanBoland',
        'خريد': 'tblSorammoamelatKharid',
        'فعاليت هاي ساخت و پيش فروش املاک': 'tblSorammoamelatSakhtPishforoshAmlak',
        'صادرات/فروش به شخص خارجي': 'tblSorammoamelatSaderatBeShakhsKhareji',
        'صادرات': 'tblSorammoamelatSaderatBeShakhsKhareji',
        'واردات/خريد از شخص خارجي': 'tblSorammoamelatVaredatAzShakhsKhareji',
        'واردات': 'tblSorammoamelatVaredatAzShakhsKhareji',
        'حق العملکاري - کارمزد - فروشنده': 'tblSorammoamelatHagholamalKariKarmozdForoshande',
        'حق العملکاري - کارمزد - خريدار/کارفرما': 'tblSorammoamelatHagholamalKariKarmozdKharidar',
        'حق العملکاري - کارمزد - خریدار': 'tblSorammoamelatHagholamalKariKarmozdKharidar',
        'حق العملکاري - صاحب کالا - فروشنده': 'tblSorammoamelatHagholamalKariSahebkalaForoshande',
        'حق العملکاري - خريدار/کارفرما': 'tblSorammoamelatKharidarKarfarma',
        'حق العملکاري - خریدار': 'tblSorammoamelatKharidarKarfarma',
        'حق العملکاري - خريدار': 'tblSorammoamelatKharidarKarfarma',
        'حق العملکاري - کارمزد - خريدار': 'tblSorammoamelatHagholamalKariSahebkalaForoshande',
        'کارفرما': 'tblSorammoamelatKharidarKarfarma',
        'فروش به شخص خارجي': 'tblSorammoamelatVaredatAzShakhsKhareji',
        'خريد از شخص خارجي': 'tblSorammoamelatVaredatAzShakhsKhareji',
        'کارفرما2': 'tblSorammoamelatHagholamalKariKarmozdKharidar',
        'بازرگان': 'tblGomrokBazargan',
        'حق العملکار/ترخیص کننده': 'tblGomrokHagholamalkarTarkhisKonande',
        'ارز': 'tblSayerArz',
        'تأمين کنندگان ارز': 'tblSayerTaminkonandeganArz',
        'تدارکات الکترونيکي': 'tblSayerTadarokatElectonic',
        'تسهيلات بانکي': 'tblSayerTashilatBanki',
        'تعهدات بانکي': 'tblSayerTahodatBanki',
        'خريدار بورس': 'tblSayerKharidarBors',
        'دریافت کنندگان ارز': 'tblSayerDaryaftkonnadeganArz',
        'سکه و طلا و جواهرات،کارشناسان رسمي': 'tblSayerSekkeVaTalaVaJaheratKarshenasanRasmi',
        'فروشنده بورس': 'tblSayerForoshandeBors',
        'مبایعه نامه های صادره بنگاه های معاملات املاک': 'tblSayerMobayenameSadereBongah',
        'وکالت نامه - دفتریار': 'tblSayerVekalatNameDaftaryar',
        'وکالت نامه - سردفتر': 'tblSayerVekalatNameSarDaftar',
        'وکالت نامه - طرفین سند حقوقی': 'tblSayerVekalatNameTarafinSanadHoghghi',
        'وکالتنامه-موکل': 'tblSayerVekalatNameMovakel',
        'وکالتنامه-وکيل': 'tblSayerVekalatNameVakil',
        'وکيل بورس': 'tblSayerVakilBors',
        'کارفرما قرارداد': 'tblSayerKarfarmaGharardad',
        'کارگزار خريدار بورس': 'tblSayerKargozarKharidarBors',
        'کارگزار ': 'tblSayerKargozarForoshandeBors',
        'پيمانکار قرارداد': 'tblSayerPeymankarGharardad',

    }


def get_soratmoamelat_mapping_address():
    return {
        'tblGomrokBazargan': 'آدرس بازرگان',
        'tblGomrokHagholamalkarTarkhisKonande': 'آدرس ترخیص کننده',
        'tblSorammoamelatEjare': 'آدرس طرف قرارداد',
        'tblSorammoamelatForosh': 'آدرس خریدار',
        'tblSorammoamelatHagholamalKariKarmozdForoshande': 'آدرس صاحب کالا(فروشنده)',
        'tblSorammoamelatHagholamalKariKarmozdKharidar': 'آدرس خریدار/کارفرما(مدیریت پیمان)',
        'tblSorammoamelatHagholamalKariSahebkalaForoshande': 'آدرس صاحب کالا(فروشنده)',
        'tblSorammoamelatKarfarmaPeymanBoland': 'آدرس کارفرما',
        'tblSorammoamelatKharid': 'آدرس فروشنده',
        'tblSorammoamelatKharidarKarfarma': 'آدرس خریدار/کارفرما(مدیریت پیمان)',
        'tblSorammoamelatPeymankarPeymanBoland': 'آدرس پیمانکار',
        'tblSorammoamelatSaderatBeShakhsKhareji': '',
        'tblSorammoamelatSakhtPishforoshAmlak': 'آدرس طرف قرارداد',
        'tblSorammoamelatVaredatAzShakhsKhareji': 'آدرس',
    }


def get_url186(name, fromdate, todate):
    urls = {'bankdarhalepeigiri': 'http://govahi186.tax.gov.ir/StimolSoftReport/StatusOfReqsInEdarat/ShowSelectReqCount.aspx?F={0}&T={1}&S=0&Edare=undefined&reqtl=1&rwndrnd=0.391666473501828'.format(fromdate, todate),
            'banksodorgovahi': 'http://govahi186.tax.gov.ir/StimolSoftReport/StatusOfReqsInEdarat/ShowSelectReqCount.aspx?F={0}&T={1}&S=1&Edare=undefined&reqtl=1&rwndrnd=0.10286883974335082'.format(
                fromdate, todate),
            'bankelambedehi': 'http://govahi186.tax.gov.ir/StimolSoftReport/StatusOfReqsInEdarat/ShowSelectReqCount.aspx?F={0}&T={1}&S=2&Edare=undefined&reqtl=1&rwndrnd=0.19619883107398017'.format(
                fromdate, todate),
            'bankadamsabeghe': 'http://govahi186.tax.gov.ir/StimolSoftReport/StatusOfReqsInEdarat/ShowSelectReqCount.aspx?F={0}&T={1}&S=3&Edare=undefined&reqtl=1&rwndrnd=0.9409760878581537'.format(
                fromdate, todate),
            'bankadameraesoratmali': 'http://govahi186.tax.gov.ir/StimolSoftReport/StatusOfReqsInEdarat/ShowSelectReqCount.aspx?F={0}&T={1}&S=4&Edare=undefined&reqtl=1&rwndrnd=0.9490208553378973'.format(
                fromdate, todate),
            'bankadameraesoratmalielambedehi': 'http://govahi186.tax.gov.ir/StimolSoftReport/StatusOfReqsInEdarat/ShowSelectReqCount.aspx?F={0}&T={1}&S=5&Edare=undefined&reqtl=1&rwndrnd=0.12348184643733573'.format(
                fromdate, todate),
            'bankdarkhasttekrari': 'http://govahi186.tax.gov.ir/StimolSoftReport/StatusOfReqsInEdarat/ShowSelectReqCount.aspx?F={0}&T={1}&S=6&Edare=undefined&reqtl=1&rwndrnd=0.9987754619692941'.format(
                fromdate, todate),
            'banknaghseetaleat': 'http://govahi186.tax.gov.ir/StimolSoftReport/StatusOfReqsInEdarat/ShowSelectReqCount.aspx?F={0}&T={1}&S=8&Edare=undefined&reqtl=1&rwndrnd=0.2656927736407235'.format(
                fromdate, todate),
            'asnafdarhalepeigiri': 'http://govahi186.tax.gov.ir/StimolSoftReport/StatusOfReqsInEdarat/ShowSelectReqCount.aspx?F={0}&T={1}&S=0&Edare=undefined&reqtl=3&rwndrnd=0.2053884823939629'.format(
                fromdate, todate),
            'asnafsodorgovahi': 'http://govahi186.tax.gov.ir/StimolSoftReport/StatusOfReqsInEdarat/ShowSelectReqCount.aspx?F={0}&T={1}&S=1&Edare=undefined&reqtl=1&rwndrnd=0.5633793857911045'.format(
                fromdate, todate),
            'asnafadamemkansodorgovahi': 'http://govahi186.tax.gov.ir/StimolSoftReport/StatusOfReqsInEdarat/ShowSelectReqCount.aspx?F={0}&T={1}&S=7&Edare=undefined&reqtl=3&rwndrnd=0.7202288185326258'.format(
                fromdate, todate),
            'bankbalaye7rooz': 'http://govahi186.tax.gov.ir/StimolSoftReport/MoreThan15DaysDetails/ShowMoreThan15DaysDetails.aspx?F={0}&T={1}&S=1&Edare=undefined&reqtl=1&rwndrnd=0.6731190588613609'.format(
                fromdate, todate),
            'bankpasokhdarmohlatmogharar': 'http://govahi186.tax.gov.ir/StimolSoftReport/MoreThan15DaysDetails/ShowMoreThan15DaysDetails.aspx?F={0}&T={1}&S=2&Edare=undefined&reqtl=1&rwndrnd=0.3354817228826432'.format(
                fromdate, todate),
            'bankpasokhbalaye7rooz': 'http://govahi186.tax.gov.ir/StimolSoftReport/MoreThan15DaysDetails/ShowMoreThan15DaysDetails.aspx?F={0}&T={1}&S=3&Edare=undefined&reqtl=1&rwndrnd=0.3154066478901216'.format(
                fromdate, todate),
            'asnafbalaye7rooz': 'http://govahi186.tax.gov.ir/StimolSoftReport/MoreThan15DaysDetails/ShowMoreThan15DaysDetails.aspx?F={0}&T={1}&S=7&Edare=undefined&reqtl=3&rwndrnd=0.6972614531535117'.format(
                fromdate, todate),
            'asnafpasokhdarmohlatmogharar': 'http://govahi186.tax.gov.ir/StimolSoftReport/MoreThan15DaysDetails/ShowMoreThan15DaysDetails.aspx?F={0}&T={1}&S=8&Edare=undefined&reqtl=3&rwndrnd=0.8946791182948994'.format(
                fromdate, todate),
            'asnafpasokhbalaye7rooz': 'http://govahi186.tax.gov.ir/StimolSoftReport/MoreThan15DaysDetails/ShowMoreThan15DaysDetails.aspx?F={0}&T={1}&S=9&Edare=undefined&reqtl=3&rwndrnd=0.33102715206520283'.format(
                fromdate, todate)
            }

    return urls[name]


def get186_titles():
    return ['bankdarhalepeigiri',
            'banksodorgovahi',
            'bankelambedehi',
            'bankadamsabeghe',
            'bankadameraesoratmali',
            'bankadameraesoratmalielambedehi',
            'bankdarkhasttekrari',
            'banknaghseetaleat',
            'asnafdarhalepeigiri',
            'asnafsodorgovahi',
            'asnafadamemkansodorgovahi',
            'bankbalaye7rooz',
            'bankpasokhdarmohlatmogharar',
            'bankpasokhbalaye7rooz',
            'asnafbalaye7rooz',
            'asnafpasokhdarmohlatmogharar',
            'asnafpasokhbalaye7rooz',
            ]


def get_newdatefor186():

    start_dates = [
        '13910101',
        '13960101',
        '13970101',
        '13970601',
        '13980101',
        '13980601',
        '13990101',
        '13990601',
        '14000101',
        '14000601',
        '14010101',
        '14010601',
    ]

    end_dates = [
        '13951229',
        '13961229',
        '13970531',
        '13971229',
        '13980531',
        '13981229',
        '13990531',
        '13991229',
        '14000531',
        '14001229',
        '14010531',
        '14011229',
    ]

    for date in zip(start_dates, end_dates):
        yield date


def return_sanim_download_links(p_instance, report_type, year):
    sanim_download_links = {
        'ezhar': [
            f"https://mgmt.tax.gov.ir/ords/f?p=100:13:{p_instance}:::RP,RIR,13:P13_GTO_ID,P13_GTO_NAME,P13_TAX_YEAR,P13_TAXTYPE,P13_WHERE_SELECTOR,P13_RETURN:2101,%D8%AE%D9%88%D8%B2%D8%B3%D8%AA%D8%A7%D9%86,{year},ITXC,1,2",
            f"https://mgmt.tax.gov.ir/ords/f?p=100:13:{p_instance}:::RP,RIR,13:P13_GTO_ID,P13_GTO_NAME,P13_TAX_YEAR,P13_TAXTYPE,P13_WHERE_SELECTOR,P13_RETURN:2101,%D8%AE%D9%88%D8%B2%D8%B3%D8%AA%D8%A7%D9%86,{year},ITXB,1,2",
            f"https://mgmt.tax.gov.ir/ords/f?p=100:13:{p_instance}:::RP,RIR,13:P13_GTO_ID,P13_GTO_NAME,P13_TAX_YEAR,P13_TAXTYPE,P13_WHERE_SELECTOR,P13_RETURN:2101,%D8%AE%D9%88%D8%B2%D8%B3%D8%AA%D8%A7%D9%86,{year},VAT,1,2"
        ],
        'tashkhis_sader_shode': [
            f"https://mgmt.tax.gov.ir/ords/f?p=100:410:{p_instance}::::P410_GTO_ID,P410_GTO_NAME,P410_TAX_TYPE,P410_TAX_TYPE_NAME,P410_TAX_YEAR,P410_WHERE_SELECTOR:2101,%D8%AE%D9%88%D8%B2%D8%B3%D8%AA%D8%A7%D9%86,ITXC,%D9%85%D8%A7%D9%84%DB%8C%D8%A7%D8%AA%20%D8%A8%D8%B1%20%D8%AF%D8%B1%D8%A2%D9%85%D8%AF%20%D8%B4%D8%B1%DA%A9%D8%AA%20%D9%87%D8%A7,{year},1",
            f"https://mgmt.tax.gov.ir/ords/f?p=100:410:{p_instance}::::P410_GTO_ID,P410_GTO_NAME,P410_TAX_TYPE,P410_TAX_TYPE_NAME,P410_TAX_YEAR,P410_WHERE_SELECTOR:2101,%D8%AE%D9%88%D8%B2%D8%B3%D8%AA%D8%A7%D9%86,ITXB,%D9%85%D8%A7%D9%84%DB%8C%D8%A7%D8%AA%20%D8%A8%D8%B1%20%D8%AF%D8%B1%D8%A2%D9%85%D8%AF%20%D8%B4%D8%B1%DA%A9%D8%AA%20%D9%87%D8%A7,{year},1",
            f"https://mgmt.tax.gov.ir/ords/f?p=100:410:{p_instance}::::P410_GTO_ID,P410_GTO_NAME,P410_TAX_TYPE,P410_TAX_TYPE_NAME,P410_TAX_YEAR,P410_WHERE_SELECTOR:2101,%D8%AE%D9%88%D8%B2%D8%B3%D8%AA%D8%A7%D9%86,VAT,%D9%85%D8%A7%D9%84%DB%8C%D8%A7%D8%AA%20%D8%A8%D8%B1%20%D8%AF%D8%B1%D8%A2%D9%85%D8%AF%20%D8%B4%D8%B1%DA%A9%D8%AA%20%D9%87%D8%A7,{year},1",
        ],
        'tashkhis_eblagh_shode': [
            f"https://mgmt.tax.gov.ir/ords/f?p=100:411:{p_instance}::::P411_GTO_ID,P411_GTO_NAME,P411_TAX_TYPE,P411_TAX_TYPE_NAME,P411_TAX_YEAR,P411_WHERE_SELECTOR:2101,%D8%AE%D9%88%D8%B2%D8%B3%D8%AA%D8%A7%D9%86,ITXC,%D9%85%D8%A7%D9%84%DB%8C%D8%A7%D8%AA%20%D8%A8%D8%B1%20%D8%AF%D8%B1%D8%A2%D9%85%D8%AF%20%D8%B4%D8%B1%DA%A9%D8%AA%20%D9%87%D8%A7,{year},1",
            f"https://mgmt.tax.gov.ir/ords/f?p=100:411:{p_instance}::::P411_GTO_ID,P411_GTO_NAME,P411_TAX_TYPE,P411_TAX_TYPE_NAME,P411_TAX_YEAR,P411_WHERE_SELECTOR:2101,%D8%AE%D9%88%D8%B2%D8%B3%D8%AA%D8%A7%D9%86,ITXB,%D9%85%D8%A7%D9%84%DB%8C%D8%A7%D8%AA%20%D8%A8%D8%B1%20%D8%AF%D8%B1%D8%A2%D9%85%D8%AF%20%D8%B4%D8%B1%DA%A9%D8%AA%20%D9%87%D8%A7,{year},1",
            f"https://mgmt.tax.gov.ir/ords/f?p=100:411:{p_instance}::::P411_GTO_ID,P411_GTO_NAME,P411_TAX_TYPE,P411_TAX_TYPE_NAME,P411_TAX_YEAR,P411_WHERE_SELECTOR:2101,%D8%AE%D9%88%D8%B2%D8%B3%D8%AA%D8%A7%D9%86,VAT,%D9%85%D8%A7%D9%84%DB%8C%D8%A7%D8%AA%20%D8%A8%D8%B1%20%D8%AF%D8%B1%D8%A2%D9%85%D8%AF%20%D8%B4%D8%B1%DA%A9%D8%AA%20%D9%87%D8%A7,{year},1",
        ],
        'ghatee_sader_shode': [
            f"https://mgmt.tax.gov.ir/ords/f?p=100:427:{p_instance}:::RIR,427:P427_GTO_ID,P427_GTO_NAME,P427_TAX_TYPE,P427_TAX_TYPE_NAME,P427_TAX_YEAR,P427_WHERE_SELECTOR:2101,%D8%AE%D9%88%D8%B2%D8%B3%D8%AA%D8%A7%D9%86,ITXC,%D9%85%D8%A7%D9%84%DB%8C%D8%A7%D8%AA%20%D8%A8%D8%B1%20%D8%AF%D8%B1%D8%A2%D9%85%D8%AF%20%D8%B4%D8%B1%DA%A9%D8%AA%20%D9%87%D8%A7,{year},1",
            f"https://mgmt.tax.gov.ir/ords/f?p=100:427:{p_instance}:::RIR,427:P427_GTO_ID,P427_GTO_NAME,P427_TAX_TYPE,P427_TAX_TYPE_NAME,P427_TAX_YEAR,P427_WHERE_SELECTOR:2101,%D8%AE%D9%88%D8%B2%D8%B3%D8%AA%D8%A7%D9%86,ITXB,%D9%85%D8%A7%D9%84%DB%8C%D8%A7%D8%AA%20%D8%A8%D8%B1%20%D8%AF%D8%B1%D8%A2%D9%85%D8%AF%20%D8%B4%D8%B1%DA%A9%D8%AA%20%D9%87%D8%A7,{year},1",
            f"https://mgmt.tax.gov.ir/ords/f?p=100:427:{p_instance}:::RIR,427:P427_GTO_ID,P427_GTO_NAME,P427_TAX_TYPE,P427_TAX_TYPE_NAME,P427_TAX_YEAR,P427_WHERE_SELECTOR:2101,%D8%AE%D9%88%D8%B2%D8%B3%D8%AA%D8%A7%D9%86,VAT,%D9%85%D8%A7%D9%84%DB%8C%D8%A7%D8%AA%20%D8%A8%D8%B1%20%D8%AF%D8%B1%D8%A2%D9%85%D8%AF%20%D8%B4%D8%B1%DA%A9%D8%AA%20%D9%87%D8%A7,{year},1",
        ],
        'ghatee_eblagh_shode': [
            f"https://mgmt.tax.gov.ir/ords/f?p=100:428:{p_instance}::::P428_GTO_ID,P428_GTO_NAME,P428_WHERE_SELECTOR,P428_TAX_YEAR,P428_TAX_TYPE,P428_TAX_TYPE_NAME:2101,%D8%AE%D9%88%D8%B2%D8%B3%D8%AA%D8%A7%D9%86,1,{year},ITXC,%D9%85%D8%A7%D9%84%DB%8C%D8%A7%D8%AA%20%D8%A8%D8%B1%20%D8%AF%D8%B1%D8%A2%D9%85%D8%AF%20%D8%B4%D8%B1%DA%A9%D8%AA%20%D9%87%D8%A7",
            f"https://mgmt.tax.gov.ir/ords/f?p=100:428:{p_instance}::::P428_GTO_ID,P428_GTO_NAME,P428_WHERE_SELECTOR,P428_TAX_YEAR,P428_TAX_TYPE,P428_TAX_TYPE_NAME:2101,%D8%AE%D9%88%D8%B2%D8%B3%D8%AA%D8%A7%D9%86,1,{year},ITXB,%D9%85%D8%A7%D9%84%DB%8C%D8%A7%D8%AA%20%D8%A8%D8%B1%20%D8%AF%D8%B1%D8%A2%D9%85%D8%AF%20%D8%B4%D8%B1%DA%A9%D8%AA%20%D9%87%D8%A7",
            f"https://mgmt.tax.gov.ir/ords/f?p=100:428:{p_instance}::::P428_GTO_ID,P428_GTO_NAME,P428_WHERE_SELECTOR,P428_TAX_YEAR,P428_TAX_TYPE,P428_TAX_TYPE_NAME:2101,%D8%AE%D9%88%D8%B2%D8%B3%D8%AA%D8%A7%D9%86,1,{year},VAT,%D9%85%D8%A7%D9%84%DB%8C%D8%A7%D8%AA%20%D8%A8%D8%B1%20%D8%AF%D8%B1%D8%A2%D9%85%D8%AF%20%D8%B4%D8%B1%DA%A9%D8%AA%20%D9%87%D8%A7",
        ],
        'badvi_darjarian_dadrasi': [
            f"https://mgmt.tax.gov.ir/ords/f?p=100:45:{p_instance}:::RP,RIR,45:P45_GTO_ID,P45_GTO_NAME,P45_TAX_YEAR,P45_WHERE_SELECTOR,P45_RETURN_PAGE,P45_DECISION_DEADLINE,P45_REQUEST_TYPE:2101,%D8%AE%D9%88%D8%B2%D8%B3%D8%AA%D8%A7%D9%86,{year},3,1100,25,02"
        ],
        'badvi_darjarian_dadrasi_hamarz': [
            f"https://mgmt.tax.gov.ir/ords/f?p=100:453:{p_instance}:::453::"
        ],
        'amar_sodor_gharar_karshenasi': [
            f"https://mgmt.tax.gov.ir/ords/f?p=100:380:{p_instance}:::::"
        ],
        'amar_sodor_ray': [
            f"https://mgmt.tax.gov.ir/ords/f?p=100:482:{p_instance}:::::"
        ],
        'imp_parvand': [
            f"https://mgmt.tax.gov.ir/ords/f?p=100:518:{p_instance}:::::"
        ],
        'badvi_takmil_shode': [
            f"https://mgmt.tax.gov.ir/ords/f?p=100:45:{p_instance}:::RP,RIR,45:P45_GTO_ID,P45_GTO_NAME,P45_TAX_YEAR,P45_WHERE_SELECTOR,P45_RETURN_PAGE,P45_SORS_EBLAGH,P45_REQUEST_TYPE:2101,%D8%AE%D9%88%D8%B2%D8%B3%D8%AA%D8%A7%D9%86,{year},9,1100,1,02"
        ],
        'tajdidnazer_darjarian_dadrasi': [
            f"https://mgmt.tax.gov.ir/ords/f?p=100:45:{p_instance}:::RP,RIR,45:P45_GTO_ID,P45_GTO_NAME,P45_TAX_YEAR,P45_WHERE_SELECTOR,P45_RETURN_PAGE,P45_DECISION_DEADLINE,P45_REQUEST_TYPE:2101,%D8%AE%D9%88%D8%B2%D8%B3%D8%AA%D8%A7%D9%86,{year},5,1100,25,03"
        ],
        'tajdidnazar_takmil_shode': [
            f"https://mgmt.tax.gov.ir/ords/f?p=100:45:{p_instance}:::RP,RIR,45:P45_GTO_ID,P45_GTO_NAME,P45_TAX_YEAR,P45_WHERE_SELECTOR,P45_RETURN_PAGE,P45_SORS_EBLAGH,P45_REQUEST_TYPE:2101,%D8%AE%D9%88%D8%B2%D8%B3%D8%AA%D8%A7%D9%86,{year},11,1100,2,03"
        ],
    }

    return sanim_download_links[report_type]
