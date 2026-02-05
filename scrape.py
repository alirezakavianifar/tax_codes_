import time
import glob
import os
from tqdm import tqdm
import uuid
import numpy as np
import threading
import datetime
from automation.custom_thread import CustomThread
import pandas as pd
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support.select import Select
from concurrent.futures import ThreadPoolExecutor, wait, ProcessPoolExecutor
from selenium.webdriver.support import expected_conditions as EC
from automation.helpers import login_sanim, login_hoghogh, login_list_hoghogh, login_nezam_mohandesi, add_one_day, \
    login_iris, login_arzeshafzoodeh, \
    login_tgju, get_update_date, connect_to_sql, \
    login_mostaghelat, login_codeghtesadi, login_soratmoamelat, login_vosolejra, \
    login_186, open_and_save_excel_files, \
    maybe_make_dir, input_info, merge_multiple_excel_sheets, \
    return_start_end, wait_for_download_to_finish, move_files, \
    remove_excel_files, init_driver, insert_into_tbl, \
    extract_nums, retryV1, check_driver_health, login_chargoon, \
    log_it, is_updated_to_download, drop_into_db, unzip_files, \
    is_updated_to_save, rename_files, merge_multiple_html_files, \
    merge_multiple_excel_files, log_the_func, wrap_it_with_params, wrap_it_with_paramsv1, cleanup, wrap_a_wrapper
from automation.download_helpers import download_1000_parvandeh
from automation.constants import get_dict_years, get_sql_con, lst_years_arzeshafzoodeSonati, \
    get_soratmoamelat_mapping, return_sanim_download_links, \
    get_newdatefor186, get_url186, get186_titles, ModiHoghogh, ModiHoghoghLst, get_report_links
import threading
import math
from automation.watchdog_186 import watch_over, is_downloaded
from automation.sql_queries import get_sql_arzeshAfzoodeSonatiV2
from automation.logger import log_it
from automation.scrape_helpers import scrape_iris_helper, soratmoamelat_helper, find_obj_and_click, download_excel, \
    adam, set_hoze, set_arzesh, find_hoghogh_info, set_user_permissions, set_imp_corps, select_from_dropdown, set_start_date, get_sodor_gharar_karshenasi, \
    scrape_it, insert_gashtPosti, save_process, get_amar_sodor_ray, get_imp_parvand, \
    insert_sabtenamArzeshAfzoodeh, insert_codeEghtesadi, get_heiat_data, ravand_residegi, \
    scrape_1000_helper, create_1000parvande_report, get_sabtenamCodeEghtesadiData, \
    save_in_db, get_eghtesadidata, get_dadrasidata, set_enseraf_heiat, get_amlak, get_modi_info, get_dadrasi_new, scrape_arzeshafzoodeh_helper, \
    get_mergeddf_arzesh, insert_arzeshafzoodelSonati, save_excel, check_health
from automation.sql_queries import get_sql_agg_soratmoamelat
from automation.soratmoamelat_helpers import get_soratmoamelat_link, \
    get_soratmoamelat_report_link, get_sorat_selected_year, get_sorat_selected_report

start_index = 1
n_retries = 0
first_list = [4, 5, 6, 7, 8, 9, 10, 21, 22, 23, 24]
second_list = [14, 15, 16, 17]
time_out_1 = 2080
time_out_2 = 2080
timeout_fifteen = 15
excel_file_names = ['Excel.xlsx', 'Excel(1).xlsx', 'Excel(2).xlsx']
badvi_file_names = ['جزئیات اعتراضات و شکایات.html']


@log_it
def log(row, success):
    print('log is called')


download_button_ezhar = '/html/body/form/div[2]/div/div[2]/main/div[2]/div/div/div/div/div/div/div[2]/div[1]/div[2]/button[3]'
download_button_ghatee_sader_shode = '/html/body/form/div[2]/div/div[2]/main/div[2]/div/div/div/div/font/div/div/div[2]/div[1]/div[2]/button[3]'
download_button_rest = '/html/body/form/div[2]/div/div[2]/main/div[2]/div/div/div/div/font/div/div/div[2]/div[1]/div[2]/button[2]'
menu_nav_1 = '//*[@id="t_MenuNav_1_1i"]'
menu_nav_2 = '/html/body/form/header/div[2]/div/ul/li[2]/div/div/div[2]/ul/li[1]/div/span[1]/a'
# menu_nav_2 = '/html/body/form/header/div[2]/div/ul/li[2]/div/div/ul/li[1]/div/span[1]/a'
year_button_1 = '//*[@id="P1100_TAX_YEAR_CONTAINER"]/div[2]/div/div'
year_button_2 = '/html/body/div[7]/div[2]/div[1]/button'
year_button_3 = '/html/body/div[6]/div[2]/div[2]/div/div[3]/ul/li'
year_button_4 = '/html/body/div[3]/div/ul/li[8]/div/span[1]/button'
download_excel_btn_2 = '/html/body/div[6]/div[3]/div/button[2]'
input_1 = '/html/body/span/span/span[1]/input'
td_1 = '/html/body/form/div[2]/div/div[2]/main/div[2]/div/div/div/div/font/div/div/div[2]/div[2]/div[5]/div[1]/div/div[3]/table/tbody/tr[2]'
td_2 = '/html/body/form/div[2]/div/div[2]/main/div[2]/div/div/div/div/font/div[2]/div/div[2]/div[2]/div[5]/div[1]/div/div[2]/table/tbody/tr[2]'
td_3 = '/html/body/form/div[2]/div/div[2]/main/div[2]/div/div/div/div/div[2]/div/div[2]/div[2]/div[5]/div[1]/div/div[2]/table/tbody/tr[2]'
year_button_5 = '/html/body/div[6]/div[3]/div/button[2]'
year_button_6 = '/html/body/form/div[2]/div/div[2]/main/div[2]/div/div/div/div/div/div/div[2]/div[1]/div[1]/div[3]/div/button'
td_4 = '/html/body/form/div[2]/div/div[2]/main/div[2]/div/div/div/div/div/div/div[2]/div[2]/div[5]/div[1]/div/div[1]/table/tr/th[8]/a'
td_5 = '/html/body/form/div[2]/div/div[2]/main/div[2]/div/div/div/div/div[1]/div/div/div/div[2]/div/span/span[1]/span/span[2]'
td_6 = '/html/body/form/div[2]/div/div[2]/main/div[2]/div/div/div/div/font/div[1]/div/div/div/div[2]/div/span/span[1]/span/span[1]'
td_ezhar = '/html/body/form/div[2]/div/div[2]/main/div[2]/div/div/div/div/div/div/div[2]/div[2]/div[5]/div[1]/div/div[2]/table/tbody/tr[2]/td[%s]/a'


def retry(func):

    def try_it(Cls):
        global n_retries
        try:
            result = func(Cls)
            return result

        except Exception as e:
            n_retries += 1
            print(e)
            if n_retries < 50:
                print('trying again')
                Cls.driver.close()
                path = Cls.path
                report_type = Cls.report_type
                year = Cls.year
                time.sleep(3)
                x = Scrape(path, report_type, year)
                x.scrape_sanim()
    return try_it


def get_td_number(report_type: str) -> str:
    if (report_type == 'ezhar'):
        td_number = 4
    elif (report_type == 'hesabrasi_darjarian_before5'):
        td_number = 5
    elif (report_type == 'hesabrasi_darjarian_after5'):
        td_number = 6
    elif (report_type == 'hesabrasi_takmil_shode'):
        td_number = 7
    elif (report_type == 'tashkhis_sader_shode'):
        td_number = 8
    elif (report_type == 'tashkhis_eblagh_shode'):
        td_number = 9
    elif (report_type == 'tashkhis_eblagh_nashode'):
        td_number = 10
    elif (report_type == 'ghatee_sader_shode'):
        td_number = 21
    elif (report_type == 'ghatee_eblagh_shode'):
        td_number = 22
    elif (report_type == 'ejraee_sader_shode'):
        td_number = 23
    elif (report_type == 'ejraee_eblagh_shode'):
        td_number = 24
    elif (report_type == 'badvi_darjarian_dadrasi'):
        td_number = 15
    elif (report_type == 'badvi_takmil_shode'):
        td_number = 16
    elif (report_type == 'tajdidnazer_darjarian_dadrasi'):
        td_number = 17
    elif (report_type == 'tajdidnazar_takmil_shode'):
        td_number = 18
    return td_number


@wrap_it_with_params(50, 1000000000, False, False, False, False)
def click_on_down_btn_sanim(driver, info, link):
    WebDriverWait(driver, 2).until(
        EC.presence_of_element_located((By.XPATH,
                                        link))).click()
    return driver, info


@wrap_it_with_params(15, 1000000000, False, False, False, False)
def click_on_down_btn_excelsanimforheiat(driver, info, btn_id='OBJECTION_DETAILS_IR_actions_button', report_type=None):

    WebDriverWait(driver, 5).until(
        EC.presence_of_element_located(
            (By.CLASS_NAME, 'a-IRR-actions'))).click()
    time.sleep(1)
    # WebDriverWait(driver, time_out_2).until(
    #     EC.presence_of_element_located(
    #         (By.XPATH, year_button_4))).click()

    btns = driver.find_elements(By.TAG_NAME, 'button')

    # [btn.click() if btn.text == 'دانلود کردن' else continue for btn in btns]
    for btn in btns:
        if btn.text == 'دانلود کردن':
            btn.click()
            break

    time.sleep(0.5)
    if btn_id == "OBJECTION_DETAILS_IR_actions_button":

        if report_type == 'imp_parvand':
            btn_downs = driver.find_element(
                By.XPATH, '/html/body/div[6]/div[2]/ul')
            btn_downs = btn_downs.find_elements(
                By.TAG_NAME, 'li')

            [btn.click() if btn.text == 'Excel' else print('f')
             for btn in btn_downs]

            WebDriverWait(driver, 3).until(
                EC.presence_of_element_located(
                    (By.XPATH, '/html/body/div[6]/div[2]/div[2]/\
                        div/div/div/div[2]/label/span'))).click()
            time.sleep(0.5)
            return driver, info

        WebDriverWait(driver, 3).until(
            EC.presence_of_element_located(
                (By.XPATH, '/html/body/div[6]/div[2]/div[2]/\
                        div/div/div/div[2]/label/span'))).click()
        time.sleep(0.5)
        download_formats = driver.find_element(By.ID, 'OBJECTION_DETAILS_IR_download_formats').\
            find_elements(By.TAG_NAME, 'li')
        [li.click() for li in download_formats if li.text == 'Excel']
    else:
        WebDriverWait(driver, 3).until(
            EC.presence_of_element_located(
                (By.XPATH, '/html/body/div[7]/div[2]/div[2]/div/div/div/div[2]/label/span'))).click()
        time.sleep(1)
        btns = driver.find_element(By.XPATH, '/html/body/div[7]/div[2]/ul')
        btns = btns.find_elements(By.TAG_NAME, 'li')
        for btn in btns:
            if btn.text == 'HTML':
                btn.click()
                break
        # WebDriverWait(driver, 3).until(
        #     EC.presence_of_element_located(
        #         (By.XPATH, '/html/body/div[7]/div[2]/ul/li'))).click()
    time.sleep(0.5)

    return driver, info


@wrap_it_with_params(1000, 1000000000, False, False, False, False)
def click_on_down_btn_excelsanimforheiatend(driver, info, xpath=download_excel_btn_2, report_type=None):
    if report_type == 'badvi_darjarian_dadrasi_hamarz':
        driver.find_element(
            By.XPATH, '/html/body/div[6]/div[3]/div/button[2]').click()

    elif report_type == 'amar_sodor_ray':
        driver.find_element(
            By.XPATH, '//*[@id="B2518985416752957249"]').click()
    elif report_type == 'amar_sodor_gharar_karshenasi':
        driver.find_element(
            By.XPATH, '//*[@id="B2518985130098957246"]').click()
    else:
        btn_excels = driver.find_element(By.XPATH, "/html/body/form/div[1]/div/div[2]/main/\
            div[2]/div/div[2]/div/div/div/div/div[2]/div[1]/div[2]")
        btn_excels = btn_excels.find_elements(By.TAG_NAME, 'button')
        [btn.click() for btn in btn_excels if btn.text == 'Excel']
    # WebDriverWait(driver, 2).until(
    #     EC.presence_of_element_located(
    #         (By.XPATH, xpath))).click()
    return driver, info


@wrap_it_with_params(1000, 1000000000, False, False, False, False)
def click_on_down_btn_excelsanim(driver, info, report_type=None):
    if report_type == "amar_sodor_gharar_karshenasi":
        excel_btn = "/html/body/form/div[1]/div/div[2]/main/div[2]/div/div[2]/div/div/div/div/div[2]/div[1]/div[2]"
    elif report_type == "amar_sodor_ray":
        excel_btn = "/html/body/form/div[1]/div/div[2]/main/div[2]/div/div/div/div/div[2]/div/div[2]/div[1]/div[2]"

    else:
        excel_btn = '/html/body/form/div[1]/div/div[2]/main/div[2]/div/div[2]/div/div/div/div/div[2]/div[1]/div[2]'
        elms = driver.find_elements(By.TAG_NAME, 'strong')
        is_found = False

        for elm in elms:
            if elm.text in [
                'گزارش اظهارنامه ها',
                'گزارش شکایات بدوی در جریان دادرسی',
                'گزارش برگ تشخیص های صادر شده اداره کل امورمالیاتی خوزستان -  مالیات بر درآمد شرکت ها',
                'گزارش برگ تشخیص های صادر شده اداره کل امورمالیاتی خوزستان - مالیات بر درآمد مشاغل',
                'گزارش برگ تشخیص های صادر شده اداره کل امورمالیاتی خوزستان - مالیات بر ارزش افزوده',
                'گزارش برگ تشخیص های ابلاغ شده اداره کل امورمالیاتی خوزستان - مالیات بر درآمد شرکت ها',
                'گزارش برگ تشخیص های ابلاغ شده اداره کل امورمالیاتی خوزستان - مالیات بر درآمد مشاغل',
                'گزارش برگ تشخیص های ابلاغ شده اداره کل امورمالیاتی خوزستان - مالیات بر ارزش افزوده',
                'گزارش برگ قطعی های صادر شده اداره کل امورمالیاتی خوزستان - مالیات بر درآمد شرکت ها',
                'گزارش برگ قطعی های صادر شده اداره کل امورمالیاتی خوزستان - مالیات بر درآمد مشاغل',
                'گزارش برگ قطعی های صادر شده اداره کل امورمالیاتی خوزستان - مالیات بر ارزش افزوده',
                'گزارش برگ قطعی های ابلاغ شده اداره کل امورمالیاتی خوزستان - مالیات بر درآمد شرکت ها',
                'گزارش برگ قطعی های ابلاغ شده اداره کل امورمالیاتی خوزستان - مالیات بر درآمد مشاغل',
                'گزارش برگ قطعی های ابلاغ شده اداره کل امورمالیاتی خوزستان - مالیات بر ارزش افزوده',
                'گزارش برگ تشخیص های صادر شده اداره کل امورمالیاتی خوزستان - مالیات بر درآمد شرکت ها',
            ]:
                is_found = True
                break

        if not is_found:
            raise Exception

    btns = WebDriverWait(driver, 2).until(
        EC.presence_of_element_located((By.XPATH,
                                        excel_btn)))

    elms = btns.find_elements(By.TAG_NAME, 'button')

    [elm.click() for elm in elms if elm.text == 'Excel']

    return driver, info


@wrap_it_with_params(50, 1000000000, False, False, False, False)
def list_details(driver=None, info=None, report_type='ezhar', manba='hoghoghi'):
    if (report_type == 'tashkhis_sader_shode' and manba == 'hoghoghi'):
        info['link_list'] = '/html/body/form/div[2]/div/div[2]\
                    /main/div[2]/div/div/div/div/font\
                    /div/div/div[2]/div[2]/div[5]/div[1]/div/div[2]/\
                    table/tbody/tr[2]/td[14]/a'

    elif (report_type == 'tashkhis_eblagh_shode' and manba == 'hoghoghi'):
        info['link_list'] = '/html/body/form/div[2]/div/div[2]/main/div[2]/div/\
                    div/div/div/font/div/div/div[2]/div[2]/\
                    div[5]/div[1]/div/div[2]/table/tbody/tr[2]/td[4]/a'

    elif (report_type == 'ezhar' and manba == 'hoghoghi'):
        info['link_list'] = '/html/body/form/div[2]/div/div[2]/\
                            main/div[2]/div/div/div/div/div/\
                            div/div[2]/div[2]/div[5]/div[1]/div/div[2]/\
                            table/tbody/tr[2]/td[5]/a'

    elif (report_type == 'ghatee_sader_shode' and manba == 'hoghoghi'):
        info['link_list'] = '/html/body/form/div[2]/div/div[2]/main/div[2]/\
                            div/div/div/div/font/div/div/div[2]/div[2]/\
                            div[5]/div[1]/div/div[3]/table/tbody/tr[2]/td[14]/a'

    elif (report_type == 'ghatee_sader_shode' and manba == 'haghighi'):
        info['link_list'] = '/html/body/form/div[2]/div/div[2]/main/div[2]/\
                            div/div/div/div/font/div/div/div[2]/\
                            div[2]/div[5]/div[1]/div/div[3]/table/tbody/tr[2]/td[15]/a'

    elif (report_type == 'ghatee_sader_shode' and manba == 'arzesh'):
        info['link_list'] = '/html/body/form/div[2]/div/div[2]/main/div[2]\
                            /div/div/div/div/font/div/div/div[2]/div[2]/div[5]\
                            /div[1]/div/div[3]/table/tbody/tr[2]/td[8]/a'

    WebDriverWait(driver, timeout_fifteen).until(
        EC.presence_of_element_located(
            (By.XPATH, info['link_list']))).click()

    return driver, info


@wrap_it_with_params(50, 1000000000, False, False, False, False)
def select_btn_type(driver=None,
                    info=None,
                    report_type=None):

    if report_type == 'ezhar':
        download_button = download_button_ezhar
    elif report_type == 'ghatee_sader_shode':
        download_button = download_button_ghatee_sader_shode
    else:
        download_button_rest
    WebDriverWait(driver, time_out_2).until(
        EC.presence_of_element_located(
            (By.XPATH, download_button))).click()
    return driver, info


@wrap_it_with_params(50, 1000000000, False, False, False, False)
def select_year(driver, info={}, year=None):

    WebDriverWait(driver, 32).until(
        EC.presence_of_element_located((By.ID, 'P1100_TAX_YEAR'))).click()

    WebDriverWait(driver, 32).until(
        EC.presence_of_element_located(
            (By.XPATH, '/html/body/div[6]/div[2]/div[1]/input'))).clear()

    WebDriverWait(driver, 32).until(
        EC.presence_of_element_located(
            (By.XPATH, '/html/body/div[6]/div[2]/div[1]/input'))).send_keys(
        year)

    driver.find_element(
        By.XPATH, '/html/body/div[6]/div[2]/div[1]/input').send_keys(Keys.ENTER)

    WebDriverWait(driver, 32).until(
        EC.presence_of_element_located((By.XPATH, '/html/body/div[6]/div[2]/div[1]/button'))).click()

    WebDriverWait(driver, 32).until(
        EC.presence_of_element_located((By.XPATH, year_button_3))).click()

    while (year != WebDriverWait(driver, 32).until(
        EC.presence_of_element_located((By.XPATH, '/html/body/form/div[2]/div/div[2]/main/\
            div[2]/div/div/div/div/font/div[2]/div\
            /div[2]/div[2]/div[5]/div[1]/div/div[2]/table/tbody/tr[2]/td[3]'))).text):
        time.sleep(1)
        print('waiting for the year to be selected')

    return driver, info


@wrap_it_with_params(50, 1000000000, False, False, False, False)
def select_column(driver, info={}, td_number=None):
    WebDriverWait(driver, 20).until(
        EC.presence_of_element_located(
            (By.XPATH, '%s/td[4]/a' % td_2)))
    driver.find_element(By.XPATH, '%s/td[%s]/a' %
                        (td_2, td_number)).click()
    return driver, info


@wrap_it_with_params(50, 1000000000, False, False, False, False)
def get_main_menu(driver, info={}):
    WebDriverWait(driver, 24).until(
        EC.presence_of_element_located((By.XPATH,
                                        '/html/body/form/header/div[2]/div/ul/li[2]/span/span'))).click()
    time.sleep(1)
    WebDriverWait(driver, 24).until(
        EC.presence_of_element_located((By.XPATH,
                                        '//*[@id="t_MenuNav_1_0i"]'))).click()
    return driver, info


class Scrape:

    def __init__(self,
                 path=None,
                 report_type=None,
                 year=None,
                 driver_type='firefox',
                 headless=True,
                 info={},
                 driver=None,
                 time_out=180,
                 lock=None,
                 table_name=None,
                 type_of=None):
        self.path = path
        self.report_type = report_type
        self.year = year
        self.driver_type = driver_type
        self.headless = headless
        self.info = info
        self.driver = driver
        self.time_out = time_out
        self.lock = lock
        self.table_name = table_name,
        self.type_of = type_of

        if isinstance(self.table_name, tuple):
            self.table_name = self.table_name[0]

    @log_the_func('none')
    def scrape_186(self, path=None, return_df=True, headless=False, fromdate='13910101',
                   todate='14040101', *args, **kwargs):
        remove_excel_files(file_path=path,
                           postfix=['.xls', '.html', 'xlsx'])
        self.driver = init_driver(pathsave=path,
                                  driver_type=self.driver_type,
                                  headless=headless)
        self.path = path
        self.driver = login_186(self.driver)
        time.sleep(3)
        titles = get186_titles()

        for name in titles:
            done = False
            date_downloaded = False
            dates = get_newdatefor186()
            fromdate, todate = next(dates)
            while not done:
                try:
                    if date_downloaded:
                        fromdate, todate = next(dates)
                        date_downloaded = False
                except Exception as e:
                    print(e)
                    done = True
                    # Move downloaded files
                    dir_to_move = os.path.join(path, name)
                    maybe_make_dir([dir_to_move])
                    srcs = glob.glob(path + "/*" + 'xlsx')
                    dsts = []
                    rename_files('path', path, file_list=srcs, postfix='.xlsx')
                    srcs = glob.glob(path + "/*" + 'xlsx')
                    for i in range(len(srcs)):
                        dsts.append(dir_to_move)
                    move_files(srcs, dsts)

                    continue
                url = get_url186(name, fromdate, todate)
                self.driver.get(url)
                try:
                    t1 = threading.Thread(
                        target=save_process, args=(self.driver, self.path))
                    t2 = threading.Thread(
                        target=watch_over, args=(self.path, 240, 2))
                    t1.start()
                    t2.start()
                    t1.join()
                    t2.join()
                    wait_for_download_to_finish(path, ['xls'])
                    time.sleep(3)
                    date_downloaded = True
                except Exception as e:
                    date_downloaded = False

        scrape186_helper(url)

    def scrape_tgju(self, path=None, return_df=True):
        self.driver = init_driver(pathsave=path,
                                  driver_type=self.driver_type,
                                  headless=True)
        self.path = path
        self.driver = login_tgju(self.driver)
        WebDriverWait(self.driver, 8).until(
            EC.presence_of_element_located(
                (By.XPATH,
                 '/html/body/main/div[1]/div[2]/div/ul/li[5]/span[1]/span')))
        price = self.driver.find_element(
            By.XPATH,
            '/html/body/main/div[1]/div[2]/div/ul/li[5]/span[1]/span').text
        WebDriverWait(self.driver, 540).until(
            EC.presence_of_element_located(
                (By.XPATH,
                 '/html/body/div[2]/header/div[2]/div[6]/ul/li/a/img')))
        try:
            if (self.driver.find_element(
                    By.XPATH,
                    '/html/body/div[2]/header/div[2]/div[6]/ul/li/a/img')):
                time.sleep(5)
                WebDriverWait(self.driver, 8).until(
                    EC.presence_of_element_located((
                        By.XPATH,
                        '/html/body/main/div[1]/div[2]/div/ul/li[5]/span[1]/span'
                    )))
                coin = self.driver.find_element(
                    By.XPATH,
                    '/html/body/main/div[1]/div[2]/div/ul/li[5]/span[1]/span'
                ).text

                WebDriverWait(self.driver, 8).until(
                    EC.presence_of_element_located((
                        By.XPATH,
                        '/html/body/main/div[1]/div[2]/div/ul/li[6]/span[1]/span'
                    )))
                dollar = self.driver.find_element(
                    By.XPATH,
                    '/html/body/main/div[1]/div[2]/div/ul/li[6]/span[1]/span'
                ).text

                WebDriverWait(self.driver, 8).until(
                    EC.presence_of_element_located((
                        By.XPATH,
                        '/html/body/main/div[1]/div[2]/div/ul/li[4]/span[1]/span'
                    )))
                gold = self.driver.find_element(
                    By.XPATH,
                    '/html/body/main/div[1]/div[2]/div/ul/li[4]/span[1]/span'
                ).text
        except Exception as e:
            print(e)

        self.driver.close()

        return coin, dollar, gold

    def scrape_soratmoamelat(self, path=None, return_df=False):

        def scrape_it():
            if del_prev_files:
                remove_excel_files(file_path=path,
                                   postfix=['.xls', '.html', 'xlsx'])
            self.driver = init_driver(pathsave=path,
                                      driver_type=self.driver_type,
                                      headless=headless)
            self.path = path
            self.driver = login_soratmoamelat(self.driver)
            WebDriverWait(self.driver, 8).until(
                EC.presence_of_element_located(
                    (By.XPATH,
                     '/html/body/form/table/tbody/tr[1]/td[1]/span/div[7]')))
            self.driver.find_element(
                By.XPATH,
                '/html/body/form/table/tbody/tr[1]/td[1]/span/div[7]').click()
            WebDriverWait(self.driver, 8).until(
                EC.presence_of_element_located((
                    By.XPATH,
                    '/html/body/form/table/tbody/tr[1]/td[1]/span/div[8]/a[3]/div'
                )))
            self.driver.find_element(
                By.XPATH,
                '/html/body/form/table/tbody/tr[1]/td[1]/span/div[8]/a[3]/div'
            ).click()

            WebDriverWait(self.driver, 8).until(
                EC.presence_of_element_located(
                    (By.ID, 'ctl00_ContentPlaceHolder1_chkAuditStatus_0')))
            self.driver.find_element(
                By.ID, 'ctl00_ContentPlaceHolder1_chkAuditStatus_0').click()

            WebDriverWait(self.driver, 8).until(
                EC.presence_of_element_located(
                    (By.ID, 'ctl00_ContentPlaceHolder1_chkAuditStatus_2')))
            self.driver.find_element(
                By.ID, 'ctl00_ContentPlaceHolder1_chkAuditStatus_2').click()
            WebDriverWait(self.driver, 8).until(
                EC.presence_of_element_located(
                    (By.ID, 'ctl00_ContentPlaceHolder1_chkAuditStatus_3')))
            self.driver.find_element(
                By.ID, 'ctl00_ContentPlaceHolder1_chkAuditStatus_3').click()

            def arzesh(i):
                WebDriverWait(self.driver, 8).until(
                    EC.presence_of_element_located(
                        (By.ID, 'ctl00_ContentPlaceHolder1_frm_year')))
                sel = Select(
                    self.driver.find_element(
                        By.ID, 'ctl00_ContentPlaceHolder1_frm_year'))
                sel.select_by_index(i)

                WebDriverWait(self.driver, 8).until(
                    EC.presence_of_element_located(
                        (By.ID, 'ctl00_ContentPlaceHolder1_frm_period')))
                sel = Select(
                    self.driver.find_element(
                        By.ID, 'ctl00_ContentPlaceHolder1_frm_period'))
                sel.select_by_index(0)

                WebDriverWait(self.driver, 8).until(
                    EC.presence_of_element_located(
                        (By.ID, 'ctl00_ContentPlaceHolder1_To_year')))
                sel = Select(
                    self.driver.find_element(
                        By.ID, 'ctl00_ContentPlaceHolder1_To_year'))
                sel.select_by_index(i)

                WebDriverWait(self.driver, 8).until(
                    EC.presence_of_element_located(
                        (By.ID, 'ctl00_ContentPlaceHolder1_To_period')))
                sel = Select(
                    self.driver.find_element(
                        By.ID, 'ctl00_ContentPlaceHolder1_To_period'))
                sel.select_by_index(3)

                WebDriverWait(self.driver, 8).until(
                    EC.presence_of_element_located(
                        (By.ID, 'ctl00_ContentPlaceHolder1_Button3')))
                time.sleep(10)
                self.driver.find_element(
                    By.ID, 'ctl00_ContentPlaceHolder1_Button3').click()

    # @retryV1
    # @log_the_func('none')

    def dadrasi(self, pathsave, driver_type, headless, info, urls, init=False, enseraf=False):
        with init_driver(
                pathsave=pathsave, driver_type=driver_type,
                headless=headless, info=info, prefs={'maximize': True,
                                                     'zoom': '0.7'}) as self.driver:
            self.driver, self.info = login_codeghtesadi(
                driver=self.driver, data_gathering=False, pred_captcha=False, info=self.info,
                user_name='1756914443', password='F@fa71791395')

            if not enseraf:
                self.driver, info = get_dadrasidata(self.driver, pathsave,
                                                    del_prev_files=False, urls=urls, info=info, init=init)
            else:
                self.driver, info = set_enseraf_heiat(self.driver, pathsave,
                                                      heiat_shenases=urls, info=info, init=init)

    def scrape_codeghtesadi(self,
                            path=None,
                            return_df=True,
                            data_gathering=False,
                            pred_captcha=False,
                            codeeghtesadi={
                                'state': True,
                                'params': {'set_important_corps': True,
                                           'getdata': False,
                                           'adam': False,
                                           'df_toadam': None,
                                           'del_prev_files': True,
                                           'merge': False,
                                           'get_info': False,
                                           'saving_dir': None,
                                           }},
                            df_toadam=None,
                            soratmoamelat={'state': True,
                                           'params': {'scrape': {'general': True, 'gomrok': True,
                                                                 'sorat': {'scrape': True, 'years': ['1397']},
                                                                 'sayer': False},

                                                      'unzip': False,
                                                      'dropdb': False,
                                                      'gen_report': True}},
                            headless=False,
                            *args,
                            **kwargs):

        self.path = path

        if soratmoamelat['state']:
            if soratmoamelat['params']['scrape']['general']:
                self.driver, self.info = scrape_it(
                    path, self.driver_type, headless=headless, driver=self.driver, info=self.info)
                self.driver, self.info = get_soratmoamelat_link(
                    driver=self.driver, info=self.info)

                self.driver, self.info = get_soratmoamelat_report_link(
                    driver=self.driver, info=self.info)

                if soratmoamelat['params']['scrape']['sorat']['scrape']:
                    for year in soratmoamelat['params']['scrape']['sorat']['years']:
                        self.driver, self.info = get_sorat_selected_year(
                            driver=self.driver, info=self.info, year=year)
                        sel = Select(
                            self.driver.find_element(
                                By.ID, 'CPC_Remained_Str_Rem_ddlTTMSCategory'))

                        options_count = len(sel.options) - 1
                        for i in range(101, 113):
                            time.sleep(2)

                            self.driver, self.info = get_sorat_selected_year(
                                driver=self.driver, info=self.info, year=year)
                            time.sleep(1)
                            self.driver, self.info = get_sorat_selected_report(
                                driver=self.driver, info=self.info, index=i)
                            time.sleep(1)
                            tbl_name = get_soratmoamelat_mapping()[
                                self.info['selected_option_text']]
                            tbl_name = tbl_name + \
                                '%s' % self.info['selected_year_text']

                            field = kwargs['field'] if 'field' in kwargs else None
                            soratmoamelat_helper(driver=self.driver,
                                                 info=self.info,
                                                 path=self.path,
                                                 table_name=tbl_name,
                                                 report_type='sorat',
                                                 selected_option_text=self.info['selected_option_text'],
                                                 index=i,
                                                 field=field)

                if soratmoamelat['params']['scrape']['gomrok']:
                    self.driver.find_element(
                        By.XPATH,
                        '/html/body/form/table/tbody/tr[1]/td[2]/div/div/div/div/div/ul/li[2]/a').click()
                    sel = Select(
                        self.driver.find_element(
                            By.ID, 'CPC_Remained_Str_Rem_ddlCuCategory'))
                    for i in range(10, 12):
                        sel = Select(
                            self.driver.find_element(
                                By.ID, 'CPC_Remained_Str_Rem_ddlCuCategory'))
                        selected_option = sel.select_by_value(str(i))
                        selected_option_text = sel.first_selected_option.text
                        tbl_name = get_soratmoamelat_mapping()[
                            selected_option_text]

                        soratmoamelat_helper(self.driver, self.path,
                                             tbl_name, selected_option_text=selected_option_text, report_type='gomrok', index=i, field=kwargs['field'])

                if soratmoamelat['params']['scrape']['sayer']:
                    self.driver.find_element(
                        By.XPATH,
                        '/html/body/form/table/tbody/tr[1]/td[2]/div/div/div/div/div/ul/li[3]/a').click()
                    sel = Select(
                        self.driver.find_element(
                            By.ID, 'CPC_Remained_Str_Rem_ddlExtCategory'))
                    for i in [800, 970, 830, 750, 980, 990, 640, 820, 900,
                              650, 69, 71, 70, 72, 270, 260, 680, 960, 660, 670]:
                        sel = Select(
                            self.driver.find_element(
                                By.ID, 'CPC_Remained_Str_Rem_ddlExtCategory'))
                        selected_option = sel.select_by_value(str(i))
                        selected_option_text = sel.first_selected_option.text
                        tbl_name = get_soratmoamelat_mapping()[
                            selected_option_text]

                        soratmoamelat_helper(self.driver, self.path,
                                             tbl_name, selected_option_text=selected_option_text,
                                             report_type='sayer', index=i, field=kwargs['field'])
                self.driver.close()

                # except Exception as e:
                #     return (self.driver, e)

            if soratmoamelat['params']['unzip']:
                unzip_files(self.path)

            if soratmoamelat['params']['dropdb']:
                save_in_db(
                    self.path, soratmoamelat['params']['scrape']['sorat']['years'][0])

            if soratmoamelat['params']['gen_report']:
                connect_to_sql(get_sql_agg_soratmoamelat(), sql_con=get_sql_con(
                    database='testdb'), num_runs=0)

        if codeeghtesadi['state']:

            if codeeghtesadi['params']['set_important_corps']:
                self.driver = scrape_it(path, self.driver_type, data_gathering)
                set_imp_corps(
                    self.driver, codeeghtesadi['params']['df'], codeeghtesadi['params']['saving_dir'])

            elif codeeghtesadi['params']['adam']:
                with init_driver(
                    pathsave=path, driver_type=self.driver_type,
                    headless=headless, info=self.info, prefs={'maximize': True,
                                                              'zoom': '0.9'}) as self.driver:
                    path = path
                    driver, info = login_codeghtesadi(
                        driver=self.driver, data_gathering=data_gathering, pred_captcha=pred_captcha, info=self.info)
                    adam(self.driver, codeeghtesadi['params']['df'])

            elif codeeghtesadi['params']['set_vosol_ejra']:
                with init_driver(
                    pathsave=path, driver_type=self.driver_type,
                    headless=headless, info=self.info, prefs={'maximize': True,
                                                              'zoom': '0.9'}) as self.driver:
                    path = path
                    driver, info = login_vosolejra(
                        driver=self.driver, info=self.info)

                    # Wait and click the "جستجو" link

                    search_link = WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.XPATH, "//a[@href='frmSearchPerson.aspx' and contains(@class, 'rmLink')]")))
                    search_link.click()

                    df = codeeghtesadi['params']['df']
                    wait = WebDriverWait(driver, 10)
                    first_time = True

                    for _, row in df.iterrows():

                        try:

                            shenasemeli = str(row.get('شناسه ملی', '')).strip()
                            kelasemeli = str(row.get('کلاسه ملی', '')).strip()
                            vahed_ejra = str(row.get('واحد اجرا', '')).strip()

                            if shenasemeli == '<NA>':
                                # حقیقی
                                tab_id = "__tab_ContentPlaceHolder1_TabContainer1_TabHagigi"
                                input_id = "ContentPlaceHolder1_TabContainer1_TabHagigi_txtHagigiCodeMelli"
                                select_id = "ContentPlaceHolder1_TabContainer1_TabHagigi_ddlHagigiVahed"
                                search_btn_id = "ContentPlaceHolder1_TabContainer1_TabHagigi_btnSearchHagigi"
                                select_link_xpath = "//table[@id='ContentPlaceHolder1_TabContainer1_TabHagigi_GridHagigiVahed']//a[contains(text(), 'انتخاب')]"
                                input_value = kelasemeli
                            else:
                                # حقوقی
                                tab_id = "__tab_ContentPlaceHolder1_TabContainer1_tabHogugi"
                                input_id = "ContentPlaceHolder1_TabContainer1_tabHogugi_txtHogugiShenase"
                                select_id = "ContentPlaceHolder1_TabContainer1_tabHogugi_ddlHogugiVahed"
                                search_btn_id = "ContentPlaceHolder1_TabContainer1_tabHogugi_btnSearchHogugi"
                                select_link_xpath = "//table[@id='ContentPlaceHolder1_TabContainer1_tabHogugi_GridHogugiVahed']//a[contains(text(), 'انتخاب')]"
                                input_value = shenasemeli

                            if not first_time:

                                driver.get(
                                    'http://ve.tax.gov.ir/forms/frmSearchPerson.aspx')

                            # Click tab
                            wait.until(EC.element_to_be_clickable(
                                (By.ID, tab_id))).click()

                            # Input value
                            input_field = wait.until(
                                EC.presence_of_element_located((By.ID, input_id)))
                            input_field.clear()
                            input_field.send_keys(input_value)

                            # Select dropdown option
                            select_elem = wait.until(
                                EC.presence_of_element_located((By.ID, select_id)))
                            select = Select(select_elem)
                            try:
                                select.select_by_visible_text(vahed_ejra)
                            except:
                                print(
                                    f"[⚠️] '{vahed_ejra}' not found in dropdown for row index {_}")
                                continue

                            # Click search button
                            search_button = wait.until(
                                EC.element_to_be_clickable((By.ID, search_btn_id)))
                            search_button.click()

                            select_link = wait.until(
                                EC.element_to_be_clickable((By.XPATH, select_link_xpath)))
                            select_link.click()

                            # Wait for the top-level "ثبت و شناسایی" menu and hover over it
                            main_menu = wait.until(EC.presence_of_element_located(
                                (By.XPATH,
                                 "//span[@class='rmLink rmRootLink rmExpand rmExpandDown' and contains(text(), 'ثبت و شناسایی')]")
                            ))
                            ActionChains(driver).move_to_element(
                                main_menu).perform()

                            # Wait for submenu item: "ثبت اطلاعات تکمیلی برگ اجرا"
                            submenu_item = wait.until(EC.element_to_be_clickable(
                                (By.XPATH, "//a[contains(@href, 'frmEjraExtraData.aspx') and contains(text(), 'ثبت اطلاعات تکمیلی برگ اجرا')]")
                            ))
                            submenu_item.click()

                            ejra_table_id = "ContentPlaceHolder1_gridEjra"
                            wait.until(EC.presence_of_element_located(
                                (By.ID, ejra_table_id)))

                            time.sleep(0.3)

                            # Count rows (excluding header)
                            rows_xpath = f"//table[@id='{ejra_table_id}']//tr[position()>1]"
                            row_count = len(
                                driver.find_elements(By.XPATH, rows_xpath))

                            # rows start at position 2
                            for row_index in range(2, row_count + 2):
                                try:
                                    # XPath to "انتخاب" button in specific row
                                    button_xpath = f"(//table[@id='{ejra_table_id}']//tr)[{row_index}]//input[@type='button' and @value='انتخاب']"
                                    select_button = wait.until(
                                        EC.element_to_be_clickable((By.XPATH, button_xpath)))
                                    select_button.click()

                                    # Handle dropdown selections
                                    wait.until(EC.presence_of_element_located(
                                        (By.ID, "ContentPlaceHolder1_ddlNoeFaliat")))
                                    wait.until(EC.presence_of_element_located(
                                        (By.ID, "ContentPlaceHolder1_ddlVosoulStatus")))

                                    Select(driver.find_element(
                                        By.ID, "ContentPlaceHolder1_ddlNoeFaliat")).select_by_visible_text("بازرگانی - استیجاری")
                                    Select(driver.find_element(
                                        By.ID, "ContentPlaceHolder1_ddlVosoulStatus")).select_by_visible_text("غیرقابل وصول")

                                    # Submit
                                    submit_btn_id = "ContentPlaceHolder1_btnSubmitExtraData"
                                    submit_button = wait.until(
                                        EC.element_to_be_clickable((By.ID, submit_btn_id)))
                                    submit_button.click()

                                    # Confirmation
                                    wait.until(EC.text_to_be_present_in_element(
                                        (By.ID, "ContentPlaceHolder1_lblMessage"),
                                        "اطلاعات تکمیلی با موفقیت ثبت گردید"
                                    ))
                                    print(
                                        f"[✔] Row {row_index - 1}: اطلاعات ثبت شد.")

                                except Exception as e:
                                    print(
                                        f"⚠️ Error on row {row_index - 1}: {e}")
                                    continue

                                finally:
                                    first_time = False

                            sql_query = f"""UPDATE tblmoavaghat SET [done]='success' where [ردیف] = '{str(row['ردیف'])}'"""
                            connect_to_sql(sql_query, read_from_sql=False, return_df=False,
                                           sql_con=get_sql_con(database='TestDb'))
                        except:

                            sql_query = f"""UPDATE tblmoavaghat SET [done]='failed' where [ردیف] = '{str(row['ردیف'])}'"""
                            connect_to_sql(sql_query, read_from_sql=False, return_df=False,
                                           sql_con=get_sql_con(database='TestDb'))
                            continue

            elif codeeghtesadi['params']['set_chargoon_info']:
                with init_driver(
                    pathsave=path, driver_type=self.driver_type,
                    headless=headless, info=self.info, prefs={'maximize': True,
                                                              'zoom': '0.9'}) as self.driver:
                    path = path
                    driver, info = login_chargoon(
                        driver=self.driver, info=self.info)

                    WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.XPATH, '//li[@title="شروع"]'))).click()

                    WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.XPATH, '//li[@title="مرکز مديريت"]'))).click()

                    iframe = WebDriverWait(driver, 10).until(
                        EC.frame_to_be_available_and_switch_to_it(
                            (By.XPATH,
                             '//iframe[contains(@src, "CheckPassword-Index")]')
                        )
                    )

                    time.sleep(1)

                    # Now find the element inside the iframe
                    password_input = WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located(
                            (By.ID, "txtClientPassword"))
                    )

                    # Optional: Fill in the password
                    password_input.send_keys("A3233404")

                    WebDriverWait(driver, 2).until(
                        EC.presence_of_element_located((By.ID, "cmdCheckPassword"))).click()

                    # Switch back to main document
                    driver.switch_to.default_content()

                    time.sleep(2)
                    # Now locate the target <div> inside the iframe
                    WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located(
                            (By.XPATH, '//div[@class="list-title" and text()="عمومی"]'))).click()

                    time.sleep(1)

                    WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located(
                            (By.XPATH, '//div[@class="list-title" and text()="کاربران"]'))).click()

                    df = codeeghtesadi['params']['df']

                    from automation.helpers import leading_zero

                    df['mellicode'] = df['mellicode'].apply(
                        lambda x: leading_zero(x))

                    for index, row in df.iterrows():
                        try:
                            user_input = WebDriverWait(driver, 10).until(
                                EC.presence_of_element_located(
                                    (By.ID,
                                     'UserName')))

                            user_input.clear()

                            user_input.send_keys(row['userid'])

                            user_input.send_keys(Keys.ENTER)

                            # Wait for div.didgah-table-body to appear

                            time.sleep(2)

                            table_container = WebDriverWait(driver, 10).until(
                                EC.presence_of_element_located(
                                    (By.CSS_SELECTOR, 'div.didgah-table-body'))
                            )

                            # Check if table exists inside the container
                            table = table_container.find_element(
                                By.CSS_SELECTOR, 'table.didgah-table-fixed')

                            # Select first row in tbody
                            first_row = table.find_element(
                                By.CSS_SELECTOR, 'tbody.didgah-table-tbody tr')

                            # Optional: Click or print something
                            print("First row found:",
                                  first_row.get_attribute("id"))

                            WebDriverWait(driver, 10).until(
                                EC.presence_of_element_located(
                                    (By.XPATH, '/html/body/form/div[3]/div/div/div/div[3]/div[1]/div[4]/div[8]/div[3]/div/div/div/div/div[2]/div/div[2]/div[2]/div/div/div/div[2]/div/div/div/div/div/span/div[2]/table/tbody/tr/td[3]/div/div/p'))
                            ).click()

                            WebDriverWait(driver, 10).until(
                                EC.presence_of_element_located(
                                    (By.XPATH, "//span[text()='اطلاعات اصلی']"))).click()

                            time.sleep(2)

                            element = WebDriverWait(driver, 10).until(
                                lambda d: d.find_element(
                                    By.ID, "DidgahUserUsername")
                                if d.find_elements(By.ID, "DidgahUserUsername")
                                else d.find_element(By.ID, "UserName")
                                if d.find_elements(By.ID, "UserName")
                                else False
                            )

                            # Clear the field
                            element.clear()

                            element.send_keys(row['mellicode'])

                            time.sleep(0.2)

                            element.send_keys((Keys.ENTER))

                            time.sleep(4)

                            # Find all divs with class 'window-title' and loop through them
                            window_titles = driver.find_elements(
                                By.CLASS_NAME, "bar-container")

                            for window_title in window_titles:
                                # Check if the text "کاربران(عمومی)" is present in the window title
                                # if "کاربران(عمومی)" in window_title.text:

                                if "تعريف کاربران" in window_title.text:
                                    # Find the close button in the same container and click it
                                    close_button = window_title.find_element(
                                        By.CSS_SELECTOR, "i.didgahicon.didgahicon-close")
                                    time.sleep(1)
                                    close_button.click()

                            sql_query = f"""UPDATE tblchargoon SET [done]='success' where [userid] = '{str(row['userid'])}'"""
                            connect_to_sql(sql_query, read_from_sql=False, return_df=False,
                                           sql_con=get_sql_con(database='testdbV2'))

                        except Exception as e:
                            sql_query = f"""UPDATE tblchargoon SET [done]='failure' where [userid] = '{str(row['userid'])}'"""
                            connect_to_sql(sql_query, read_from_sql=False, return_df=False,
                                           sql_con=get_sql_con(database='testdbV2'))
                            print(e)

                        print('...')

            elif codeeghtesadi['params']['set_hoze']:
                with init_driver(
                    pathsave=path, driver_type=self.driver_type,
                    headless=headless, info=self.info, prefs={'maximize': True,
                                                              'zoom': '0.9'}) as self.driver:
                    path = path
                    driver, info = login_codeghtesadi(
                        driver=self.driver, data_gathering=data_gathering, pred_captcha=pred_captcha, info=self.info)

                    set_hoze(
                        self.driver, codeeghtesadi['params']['df_set_hoze'])

            elif codeeghtesadi['params']['set_arzesh']:
                with init_driver(
                    pathsave=path, driver_type=self.driver_type,
                    headless=headless, info=self.info, prefs={'maximize': True,
                                                              'zoom': '0.9'}) as self.driver:
                    path = path
                    driver, info = login_codeghtesadi(
                        driver=self.driver, data_gathering=data_gathering, pred_captcha=pred_captcha, info=self.info)

                    set_arzesh(
                        self.driver, codeeghtesadi['params']['df'], get_date=False)

            elif codeeghtesadi['params']['ravand_residegi']:

                remove_excel_files(file_path=path,
                                   postfix=['.xls', '.html', 'xlsx'])

                with init_driver(
                            pathsave=path,
                            driver_type=self.driver_type,
                            headless=headless,
                            info=self.info,
                            prefs={'maximize': True, 'zoom': '0.9'}
                        ) as self.driver:
                    

                    driver, info = login_codeghtesadi(
                        driver=self.driver,
                        data_gathering=data_gathering,
                        pred_captcha=pred_captcha,
                        user_name='1756914443',
                        password='14579Ali.@',
                        info=self.info
                    )

                    ravand_residegi(driver=self.driver, path=path)

            elif codeeghtesadi['params']['dar_jarian_dadrasi']:

                remove_excel_files(file_path=path,
                                   postfix=['.xls', '.html', 'xlsx'])

                with init_driver(
                    pathsave=path, driver_type=self.driver_type,
                    headless=headless, info=self.info, prefs={'maximize': True,
                                                              'zoom': '0.9'}) as self.driver:
                    path = path
                    driver, info = login_codeghtesadi(
                        driver=self.driver, data_gathering=data_gathering, user_name='1751164187',
                        password='Aa@123456',
                        pred_captcha=pred_captcha, info=self.info)

                    get_heiat_data(driver=self.driver, path=path)

            elif codeeghtesadi['params']['find_hoghogh_info']:
                with init_driver(
                    pathsave=path, driver_type=self.driver_type,
                    headless=headless, info=self.info, prefs={'maximize': True,
                                                              'zoom': '0.9'}) as self.driver:
                    path = path
                    driver, info = login_codeghtesadi(
                        driver=self.driver, data_gathering=data_gathering, pred_captcha=pred_captcha, info=self.info)

                    find_hoghogh_info(
                        self.driver, codeeghtesadi['params']['df'], get_date=False,
                        from_scratch=codeeghtesadi['params']['find_hoghogh_info_from_scratch'])

            elif codeeghtesadi['params']['set_user_permissions']:
                # self.driver = scrape_it(path, self.driver_type, data_gathering)
                with init_driver(
                    pathsave=path, driver_type=self.driver_type,
                    headless=headless, info=self.info, prefs={'maximize': True,
                                                              'zoom': '0.9'}) as self.driver:
                    path = path
                    driver, info = login_codeghtesadi(
                        driver=self.driver, data_gathering=data_gathering, pred_captcha=pred_captcha, info=self.info, user_name='tax16', password='I@nh1157396')
                    set_user_permissions(
                        self.driver, codeeghtesadi['params']['df'])

            elif codeeghtesadi['params']['get_sabtenamCodeEghtesadiData']:
                # self.driver = scrape_it(path, self.driver_type, data_gathering)
                with init_driver(
                    pathsave=path, driver_type=self.driver_type,
                    headless=headless, info=self.info, prefs={'maximize': True,
                                                              'zoom': '0.9'}) as self.driver:
                    path = path
                    self.driver, self.info = login_codeghtesadi(
                        driver=self.driver, data_gathering=data_gathering,
                        user_name='1756914443', password='1756914443',
                        pred_captcha=pred_captcha, info=self.info)
                    self.driver, self.info = get_sabtenamCodeEghtesadiData(path=path,
                                                                           driver=self.driver,
                                                                           info=self.info,
                                                                           down_url=codeeghtesadi['params']['down_url'])

            elif codeeghtesadi['params']['getdata']:
                with init_driver(
                        pathsave=path, driver_type=self.driver_type, headless=headless, info=self.info) as self.driver:
                    path = path
                    self.driver, self.info = login_codeghtesadi(
                        driver=self.driver, data_gathering=data_gathering, pred_captcha=pred_captcha, info=self.info)
                # self.driver = scrape_it(path=path, driver_type=self.driver_type,
                #                         headless=headless, data_gathering=data_gathering, pred_captcha=pred_captcha)
                    get_eghtesadidata(self.driver, self.path,
                                      del_prev_files=codeeghtesadi['params']['del_prev_files'])

            elif codeeghtesadi['params']['set_enseraf']:

                self.dadrasi(pathsave=path, driver_type=self.driver_type, headless=False,
                             info=self.info, urls=codeeghtesadi['params']['df'], init=False, enseraf=True)

                num_procs = min(len(df_data), 18)
                dfs_users = np.array_split(df_data.iloc[index:, :], num_procs)

                with ProcessPoolExecutor(len(dfs_users)) as executor:

                    try:

                        jobs = [executor.submit(self.dadrasi, path, self.driver_type, False, self.info, item)
                                for index, item in enumerate(dfs_users)]

                        wait(jobs)
                        print('f')

                    except Exception as e:
                        print(e)

            elif codeeghtesadi['params']['get_dadrasi_new']:
                with init_driver(
                    pathsave=path, driver_type=self.driver_type,
                    headless=headless, info=self.info, prefs={'maximize': True,
                                                              'zoom': '0.9'}) as self.driver:
                    path = path
                    driver, info = login_codeghtesadi(
                        driver=self.driver, data_gathering=data_gathering, pred_captcha=pred_captcha, info=self.info)
                    get_dadrasi_new(self.driver, self.path,
                                    codeeghtesadi['params']['df'], codeeghtesadi['params']['saving_dir'])

            elif codeeghtesadi['params']['get_dadrasi']:

                from_scratch = False

                df_data = connect_to_sql('SELECT * FROM tbldadrasiUrls WHERE [تاریخ بروزرسانی] IS NULL',
                                         read_from_sql=True,
                                         return_df=True,)

                index = 0

                # If You want to start scraping from the beggining on pages
                if from_scratch:

                    self.dadrasi(path, self.driver_type, False,
                                 self.info, df_data.head(1), init=from_scratch)
                    index += 1

                    return

                num_procs = min(len(df_data), 18)
                dfs_users = np.array_split(df_data.iloc[index:, :], num_procs)

                with ProcessPoolExecutor(len(dfs_users)) as executor:

                    try:

                        jobs = [executor.submit(self.dadrasi, path, self.driver_type, False, self.info, item)
                                for index, item in enumerate(dfs_users)]

                        wait(jobs)
                        print('f')

                    except Exception as e:
                        print(e)

            elif codeeghtesadi['params']['get_amlak']:
                with init_driver(
                        pathsave=path, driver_type=self.driver_type,
                        headless=headless, info=self.info, prefs={'maximize': True,
                                                                  'zoom': '0.7'}) as self.driver:
                    path = path
                    self.driver, self.info = login_codeghtesadi(
                        driver=self.driver, data_gathering=data_gathering, pred_captcha=pred_captcha, info=self.info,
                        user_name='1930841086', password='marzi1930841')
                # self.driver = scrape_it(path=path, driver_type=self.driver_type,
                #                         headless=headless, data_gathering=data_gathering, pred_captcha=pred_captcha)
                    get_amlak(self.driver, self.path,
                              del_prev_files=codeeghtesadi['params']['del_prev_files'], info=self.info)

            elif codeeghtesadi['params']['get_info']:
                with init_driver(
                    pathsave=path, driver_type=self.driver_type,
                    headless=headless, info=self.info, prefs={'maximize': True,
                                                              'zoom': '0.9'}) as self.driver:
                    path = path
                    driver, info = login_codeghtesadi(
                        driver=self.driver, data_gathering=data_gathering, pred_captcha=pred_captcha, info=self.info)
                    get_modi_info(self.driver, self.path,
                                  codeeghtesadi['params']['df'], codeeghtesadi['params']['saving_dir'])
                # self.driver = scrape_it(path, self.driver_type, data_gathering)

            if codeeghtesadi['params']['merge']:
                merge_multiple_excel_sheets(path,
                                            dest=path,
                                            table='tblCodeeghtesadi',
                                            multithread=True)
        return self.driver, self.info

    def scrape_mostaghelat(self,
                           path=None,
                           scrape=False,
                           read_from_repo=True,
                           report_type='Tashkhis',
                           return_df=False,
                           table_name=None,
                           drop_to_sql=False,
                           append_to_prev=False,
                           del_prev_files=True,
                           db_name='testdbV2',
                           headless=False):

        # Scrape process
        def scrape_it():
            with init_driver(pathsave=path, driver_type=self.driver_type, headless=headless) as self.driver:
                self.path = path
                self.driver, self.info = login_mostaghelat(self.driver)

                @wrap_it_with_params(50, 1000000000, False, True, True, False)
                def select_menu(driver, info):
                    WebDriverWait(self.driver, 66).until(
                        EC.presence_of_element_located(
                            (By.XPATH,
                             '/html/body/form/div[4]/div[1]/ul[1]/li[10]/a/span'))).click()
                    return self.driver, self.info

                self.driver, self.info = select_menu(
                    driver=self.driver, info=self.info)
                time.sleep(1)
                if report_type == 'AmadeGhatee':
                    index = '11'
                    select_type = 'Dro_S_TaxOffice'
                elif report_type == 'Tashkhis':
                    index = '3'
                    select_type = 'Drop_S_TaxUnitCode'
                elif report_type == 'Ghatee':
                    path_second_date = '/html/body/form/div[4]/div[2]/div/div[2]/div/div[2]/div/div/div/div/div[2]/div[3]/div/div/div[2]/table[1]/tbody/tr[3]/td[4]/button'
                    index = '4'
                    select_type = 'Drop_S_TaxUnitCode'
                elif report_type == 'Ezhar':
                    path_second_date = '/html/body/form/div[4]/div[2]/div/div[2]/div/div[2]/div/div/div/div/div[2]/div[3]/div/div/div[2]/table[1]/tbody/tr[3]/td[4]/button'
                    index = '2'

                WebDriverWait(self.driver, 24).until(
                    EC.presence_of_element_located((
                        By.XPATH,
                        '/html/body/form/div[4]/div[1]/ul[1]/li[10]/ul/li[%s]/a/i[2]'
                        % index))).click()

                if report_type == 'Ezhar':
                    Select(self.driver.find_element(
                        By.ID, 'Drop_S_Year')).select_by_value('1401')

                    dict_ezhar = {'residegi_nashode': '1',
                                  'residegi_shode': '2'}
                    prev_text = ''
                    for key, value in dict_ezhar.items():

                        # select ezhar residegi_nashode
                        Select(self.driver.find_element(
                            By.ID, 'Drop_S_PossessionName')).select_by_value(value)

                        time.sleep(3)

                        WebDriverWait(self.driver, 24).until(
                            EC.presence_of_element_located((
                                By.ID,
                                'Btn_Search'))).click()

                        @wrap_it_with_params(50, 1000000000, False, True, True, False)
                        def wait_for_res(driver, info={}, prev_text=''):
                            info['text'] = WebDriverWait(driver, 1).until(
                                EC.presence_of_element_located((
                                    By.ID,
                                    'ContentPlaceHolder1_Lbl_Count'))).text
                            while (prev_text == info['text']):
                                raise Exception
                            return driver, info

                        self.driver, self.info = wait_for_res(
                            driver=self.driver, info=self.info, prev_text=prev_text)
                        prev_text = self.info['text']

                        time.sleep(1)

                        def down(driver, path):

                            WebDriverWait(driver, 24).until(
                                EC.presence_of_element_located((
                                    By.ID,
                                    'ContentPlaceHolder1_Btn_Export'))).click()

                        try:
                            t1 = threading.Thread(
                                target=down, args=(self.driver, self.path))
                            t2 = threading.Thread(
                                target=watch_over, args=(self.path, 240, 2))
                            t1.start()
                            t2.start()
                            t1.join()
                            t2.join()
                            wait_for_download_to_finish(self.path, ['xls'])
                            time.sleep(3)
                            date_downloaded = True

                        except Exception as e:
                            date_downloaded = False

                    self.driver.close()
                    return

                if select_type == 'Drop_S_TaxUnitCode':
                    time.sleep(3)
                    WebDriverWait(self.driver, 48).until(
                        EC.presence_of_element_located(
                            (By.ID, 'Txt_RegisterDateAz')))
                    # time.sleep(5)
                    self.driver.find_element(
                        By.ID, 'Txt_RegisterDateAz').click()
                    time.sleep(1)
                    sel = Select(
                        self.driver.find_element(By.ID,
                                                 'bd-year-Txt_RegisterDateAz'))
                    sel.select_by_index(0)
                    WebDriverWait(self.driver, 24).until(
                        EC.presence_of_element_located((By.CLASS_NAME, 'day-1')))
                    self.driver.find_element(By.CLASS_NAME, 'day-1').click()

                    WebDriverWait(self.driver, 24).until(
                        EC.presence_of_element_located(
                            (By.ID, 'Txt_RegisterDateTa')))
                    self.driver.find_element(
                        By.ID, 'Txt_RegisterDateTa').click()
                    sel = Select(
                        self.driver.find_element(By.ID,
                                                 'bd-year-Txt_RegisterDateTa'))
                    sel.select_by_index(99)
                    if report_type == 'Tashkhis':
                        WebDriverWait(self.driver, 8).until(
                            EC.presence_of_element_located((
                                By.XPATH,
                                '/html/body/form/div[4]/div[2]/div/div[2]/div/div/div[2]/div/div/\
                                    div/div/div[2]/div[3]/div/div/div[2]/table[1]/tbody/tr[1]/td[7]/button'
                            ))).click()

                    else:
                        WebDriverWait(self.driver, 8).until(
                            EC.presence_of_element_located(
                                (By.XPATH, path_second_date)))
                        self.driver.find_element(By.XPATH,
                                                 path_second_date).click()

                if report_type == 'amade_ghatee':

                    sel = Select(self.driver.find_element(By.ID, select_type))
                    count = len(sel.options) - 1
                else:
                    count = 1

                def mostagh(i, select_type=select_type):
                    try:
                        if report_type == 'amade_ghatee':
                            sel = Select(
                                self.driver.find_element(By.ID, select_type))
                            sel.select_by_index(i)

                        sel = Select(
                            self.driver.find_element(By.ID, 'Drop_S_TypeAnnunciation'))
                        sel.select_by_index(i)

                        WebDriverWait(self.driver, 4).until(
                            EC.presence_of_element_located((By.ID, 'Btn_Search')))
                        self.driver.find_element(By.ID, 'Btn_Search').click()
                        if_true = self.driver.find_element(
                            By.ID, 'ContentPlaceHolder1_Lbl_Count').text != 'تعداد : 0 مورد'
                        if report_type == 'amade_ghatee':
                            try:
                                if (self.driver.find_element(
                                        By.ID, 'ContentPlaceHolder1_Btn_Export')):
                                    time.sleep(4)
                                    self.driver.find_element(
                                        By.ID,
                                        'ContentPlaceHolder1_Btn_Export').click()
                            except Exception as e:
                                global start_index
                                start_index += 1
                                mostagh(start_index, select_type=select_type)

                        elif (if_true):
                            try:
                                if (self.driver.find_element(
                                        By.ID, 'ContentPlaceHolder1_Btn_Export')):
                                    time.sleep(4)
                                    self.driver.find_element(
                                        By.ID,
                                        'ContentPlaceHolder1_Btn_Export').click()
                            except Exception as e:
                                print(e)
                    except Exception as e:
                        print(e)
                        return

                global start_index
                while start_index <= count:
                    for i in range(2):
                        try:
                            t1 = threading.Thread(
                                target=mostagh, args=(i, ))
                            t2 = threading.Thread(target=watch_over,
                                                  args=(self.path, 15))
                            t1.start()
                            t2.start()
                            t1.join()
                            t2.join()
                            start_index += 1
                            wait_for_download_to_finish(
                                self.path, ['xls', 'xlsx'])
                        except Exception as e:
                            print(e)
                            continue
                start_index = 1
                wait_for_download_to_finish(self.path, ['xls', 'xlsx'])

        if scrape:
            scrape_it()

            df = merge_multiple_excel_files(path,
                                            path,
                                            table_name=table_name,
                                            delete_after_merge=True,
                                            postfix='xls',
                                            return_df=True)

        if read_from_repo:
            dest = os.path.join(path, 'merged', table_name + '.xlsx')
            df = pd.read_excel(dest)
            df = df.astype(str)

        if drop_to_sql:
            drop_into_db(table_name=table_name,
                         columns=df.columns.tolist(),
                         values=df.values.tolist(),
                         append_to_prev=append_to_prev,
                         db_name=db_name)

        if return_df:
            return df

    @log_the_func('none')
    def scrape_arzeshafzoodeh(self,
                              path=None,
                              return_df=True,
                              del_prev_files=True,
                              headless=False,
                              scrape=False,
                              merge_df=False,
                              inser_to_db=True,
                              insert_sabtenamArzesh=False,
                              insert_codeEghtesad=False,
                              insert_gash=False,
                              *args,
                              **kwargs):

        if scrape:
            scrape_arzeshafzoodeh_helper(
                path=path, del_prev_files=del_prev_files, headless=headless, field=kwargs['field'])

        # Move the files to the temp folder
        if merge_df:
            df_arzesh = get_mergeddf_arzesh(path, return_df=True)
        else:
            df_arzesh = None

        if inser_to_db:
            insert_arzeshafzoodelSonati(df_arzesh=df_arzesh)

        if insert_sabtenamArzesh:
            insert_sabtenamArzeshAfzoodeh()

        if insert_codeEghtesad:
            insert_codeEghtesadi()

        if insert_gash:
            insert_gashtPosti()

        insert_into_tbl(sql_query=get_sql_arzeshAfzoodeSonatiV2(),
                        tbl_name='tbl_ArzeshAfzoodeSonati', convert_to_str=True)

        # df_merged_1 = df_merged.merge(df_codeeghtesadi, how='left',
        #                               left_on='کدرهگیری', right_on='کد رهگیری')

        # Scrape contents of arzesh afzoode from the website
        # scrape_it()
        time.sleep(2)
        # Merge files and return df
        # df_merged = get_mergeddf()

        return driver, info

    def scrape_1000parvande(self, path, scrape=True,
                            headless=False,
                            del_prev_files=True,
                            merge=True,
                            create_report=True,
                            open_and_save_excel=True,
                            *args, **kwargs):
        if del_prev_files:
            remove_excel_files(file_path=path,
                               postfix=['.xls', '.html', 'xlsx'])

        if scrape:
            self.driver = init_driver(pathsave=path,
                                      driver_type=self.driver_type,
                                      headless=headless)
            self.driver, self.info = login_sanim(
                driver=self.driver, info=self.info)

            scrape_1000_helper(self.driver)

        if open_and_save_excel:

            open_and_save_excel_files(path=path)

        if merge:

            df = merge_multiple_excel_files(path=path,
                                            dest=path,
                                            delete_after_merge=False,
                                            return_df=True)

        if create_report:
            create_1000parvande_report(path, 'finals.xlsx', df, path)

    # @retry
    @wrap_it_with_params(50, 1000000000, False, True, True, False)
    def scrape_sanim(self, *args, **kwargs):
        with init_driver(pathsave=self.path, driver_type=self.driver_type, headless=self.headless) as self.driver:
            self.driver, self.info = login_sanim(
                driver=self.driver, info=self.info)
            links = return_sanim_download_links(
                self.info['cur_instance'], self.report_type, self.year)
            for link in links:
                self.driver.get(link)

            if self.report_type not in ['badvi_darjarian_dadrasi',
                                        'badvi_takmil_shode',
                                        'tajdidnazer_darjarian_dadrasi',
                                        'tajdidnazar_takmil_shode',
                                        'badvi_darjarian_dadrasi_hamarz',
                                        'amar_sodor_gharar_karshenasi',
                                        'amar_sodor_ray',
                                        'imp_parvand']:
                download_excel(func=lambda: click_on_down_btn_excelsanim(driver=self.driver, info=self.info),
                               path=self.path,
                               report_type=self.report_type,
                               type_of_excel=self.report_type,
                               no_files_in_path=0,
                               excel_file=badvi_file_names[0],
                               year=self.year,
                               table_name=self.table_name,
                               type_of=self.type_of)
            else:
                btn_id = 'OBJECTION_DETAILS_IR_actions_button'
                xpath_down = download_excel_btn_2

                if self.report_type == 'amar_sodor_ray':
                    btn_id = 'TaxOffice_Income_actions_button'
                    xpath_down = '//*[@id="B2518985416752957249"]'
                    # WebDriverWait(self.driver, 2).until(
                    #     EC.presence_of_element_located(
                    #         (By.XPATH,
                    #             '/html/body/form/div[1]/div/div[2]/main/div[2]/div/div/div/div/\
                    #                 div[2]/div/div[2]/div[2]/div[2]/div[2]/ul/li/span[4]/button'))).click()
                    self.driver, self.info = get_amar_sodor_ray(
                        driver=self.driver, info=self.info)
                    download_excel(func=lambda: click_on_down_btn_excelsanim(driver=self.driver, info=self.info, report_type=self.report_type),
                                   path=self.path,
                                   report_type=self.report_type,
                                   type_of_excel=self.report_type,
                                   no_files_in_path=0,
                                   excel_file=badvi_file_names[0],
                                   year=self.year,
                                   table_name=self.table_name,
                                   type_of=self.type_of)

                    return self.driver, self.info

                if self.report_type == 'amar_sodor_gharar_karshenasi':
                    btn_id = 'TaxOffice_Income_actions_button'
                    xpath_down = '/html/body/div[7]/div[3]/div/button[2]'
                    self.driver, self.info = get_sodor_gharar_karshenasi(
                        driver=self.driver, info=self.info)
                    download_excel(func=lambda: click_on_down_btn_excelsanim(driver=self.driver, info=self.info, report_type=self.report_type),
                                   path=self.path,
                                   report_type=self.report_type,
                                   type_of_excel=self.report_type,
                                   no_files_in_path=0,
                                   excel_file=badvi_file_names[0],
                                   year=self.year,
                                   table_name=self.table_name,
                                   type_of=self.type_of)

                    return self.driver, self.info

                if self.report_type == 'imp_parvand':
                    self.driver, self.info = get_imp_parvand(
                        driver=self.driver, info=self.info)

                if self.report_type == 'badvi_darjarian_dadrasi_hamarz':
                    WebDriverWait(self.driver, 8).until(
                        EC.presence_of_element_located(
                            (By.XPATH,
                                '/html/body/form/div[1]/div/div[2]/main/div[2]/div/div[2]/div/div\
                                /div/div/div[2]/div[2]/div[5]/div[1]/div/div[1]/table/tr/th[4]/a'))).click()
                    time.sleep(7)
                    edares = WebDriverWait(self.driver, 8).until(
                        EC.presence_of_element_located(
                            (By.ID,
                                'OBJECTION_DETAILS_IR_sort_widget_rows')))

                    # Get the HTML content of the WebElement
                    edares = edares.get_attribute('outerHTML')

                    # Parse the HTML content
                    soup = BeautifulSoup(edares, 'html.parser')

                    # Find all anchor tags within the div with class 'a-IRR-sortWidget-rows'
                    edares = soup.find_all('a', class_='a-IRR-sortWidget-row')

                    # Extract the text of href items and create a list
                    href_texts = [tag.text.strip() for tag in edares]

                    for index, item in enumerate(href_texts):
                        # Open a new tab using JavaScript
                        # self.driver.execute_script("window.open('');")
                        # # Example: Open Google in the new tab
                        # self.driver.switch_to.window(
                        #     self.driver.window_handles[1])
                        # self.driver.get(link)

                        WebDriverWait(self.driver, 8).until(
                            EC.presence_of_element_located(
                                (By.ID,
                                 'OBJECTION_DETAILS_IR_search_field'))).send_keys(item)
                        time.sleep(2)
                        WebDriverWait(self.driver, 8).until(
                            EC.presence_of_element_located(
                                (By.ID,
                                 'OBJECTION_DETAILS_IR_search_button'))).click()
                        try:
                            while (WebDriverWait(self.driver, 6).until(
                                EC.presence_of_element_located(
                                    (By.CLASS_NAME,
                                     'u-Processing-spinner'))).is_displayed()):
                                print('waiting')

                        except:
                            self.driver, self.info = click_on_down_btn_excelsanimforheiat(
                                driver=self.driver, info=self.info, btn_id=btn_id, report_type=self.report_type)
                            download_excel(func=lambda: click_on_down_btn_excelsanimforheiatend(driver=self.driver, info=self.info,
                                                                                                xpath=xpath_down, report_type=self.report_type),
                                           path=self.path,
                                           report_type=self.report_type,
                                           type_of_excel=self.report_type,
                                           no_files_in_path=0,
                                           excel_file=badvi_file_names[0],
                                           year=self.year,
                                           table_name=self.table_name,
                                           type_of=self.type_of)

                            self.driver.find_element(
                                By.XPATH, '//*[@title="Close"]').click()
                            time.sleep(2)
                            WebDriverWait(self.driver, 8).until(
                                EC.presence_of_element_located(
                                    (By.XPATH,
                                     '/html/body/form/div[1]/div/div[2]/main/div[2]/div/div[2]/div\
                                     /div/div/div/div[2]/div[2]/div[2]/div[2]/ul/li/span[4]/button'))).click()
                            try:
                                while (WebDriverWait(self.driver, 6).until(
                                    EC.presence_of_element_located(
                                        (By.CLASS_NAME,
                                         'u-Processing-spinner'))).is_displayed()):
                                    print('waiting')
                            except:
                                time.sleep(1)

                    return self.driver, self.info

                self.driver, self.info = click_on_down_btn_excelsanimforheiat(
                    driver=self.driver, info=self.info, btn_id=btn_id, report_type=self.report_type)

                download_excel(func=lambda: click_on_down_btn_excelsanimforheiatend(driver=self.driver, info=self.info,
                                                                                    xpath=xpath_down, report_type=self.report_type),
                               path=self.path,
                               report_type=self.report_type,
                               type_of_excel=self.report_type,
                               no_files_in_path=0,
                               excel_file=badvi_file_names[0],
                               year=self.year,
                               table_name=self.table_name,
                               type_of=self.type_of)

        return self.driver, self.info

    def scrape_hoghogh(self, path=None, return_df=False, del_prev_files=True, headless=False):
        if del_prev_files:
            remove_excel_files(file_path=path,
                               postfix=['.xls', '.html', 'xlsx'])
        with init_driver(pathsave=path,
                         driver_type=self.driver_type,
                         headless=headless) as self.driver:
            self.path = path
            self.driver = login_hoghogh(self.driver)
            WebDriverWait(self.driver, 8).until(
                EC.presence_of_element_located(
                    (By.XPATH,
                     '/html/body/form/table/tbody/tr[1]/td[1]/span/div[7]')))
            self.driver.find_element(
                By.XPATH,
                '/html/body/form/table/tbody/tr[1]/td[1]/span/div[7]').click()
            WebDriverWait(self.driver, 8).until(
                EC.presence_of_element_located((
                    By.XPATH,
                    '/html/body/form/table/tbody/tr[1]/td[1]/span/div[8]/a[3]/div'
                )))
            self.driver.find_element(
                By.XPATH,
                '/html/body/form/table/tbody/tr[1]/td[1]/span/div[8]/a[3]/div'
            ).click()

            WebDriverWait(self.driver, 8).until(
                EC.presence_of_element_located(
                    (By.ID, 'ctl00_ContentPlaceHolder1_chkAuditStatus_0'))).click()
            WebDriverWait(self.driver, 8).until(
                EC.presence_of_element_located(
                    (By.ID, 'ctl00_ContentPlaceHolder1_chkAuditStatus_2'))).click()
            WebDriverWait(self.driver, 8).until(
                EC.presence_of_element_located(
                    (By.ID, 'ctl00_ContentPlaceHolder1_chkAuditStatus_3'))).click()

    @wrap_a_wrapper
    @wrap_it_with_params(50, 1000000000, False, True, False, False)
    def scrape_list_hoghogh(self, path=None, headless=True, creds={}, *args, **kwargs):
        with init_driver(pathsave=path,
                         driver_type=self.driver_type,
                         headless=headless) as self.driver:

            self.driver, self.info = login_list_hoghogh(
                driver=self.driver, info=self.info, creds=creds)

            WebDriverWait(self.driver, 8).until(
                EC.presence_of_element_located(
                    (By.XPATH, '/html/body/div[2]/div[1]/nav/ul/li[3]/a'))).click()
            time.sleep(1)
        # WebDriverWait(self.driver, 8).until(
        #     EC.presence_of_element_located(
        #         (By.XPATH, '/html/body/div[2]/div[2]/div/div[3]/fieldset/table/tbody/tr[2]/td[10]/div[1]/a'))).click()
        # WebDriverWait(self.driver, 8).until(
        #     EC.presence_of_element_located(
        #         (By.XPATH, '/html/body/div[2]/div[2]/form/fieldset/a[4]'))).click()
        # sel = Select(
        #     self.driver.find_element(
        #         By.NAME, 'hoghoughtable_length'))
        # sel.select_by_index(3)
            sel = Select(
                self.driver.find_element(
                    By.XPATH, '/html/body/div[2]/div[2]/div/fieldset/div/div/div[1]/label/select'))
            sel.select_by_value("100")
            text_info = WebDriverWait(self.driver, 8).until(
                EC.presence_of_element_located(
                    (By.ID, 'karmandstable_info'))).text
            nums = extract_nums(text_info, expression=r'(\d+,\d+)')
            nums = extract_nums(nums[0])
            num = ''
            for item in nums:
                num += item
            num = math.ceil(int(num) / 100)
            lst_modis = []
            append_to_prev = False
            for i in range(num):
                # **********************************************************************************
                table = WebDriverWait(self.driver, 8).until(
                    EC.presence_of_element_located(
                        (By.ID, 'karmandstable')))

                columns = table.find_elements(By.TAG_NAME, 'thead')[0].\
                    find_elements(By.TAG_NAME, 'tr')[0].\
                    find_elements(By.TAG_NAME, 'th')
                columns_names = list(
                    filter(None, [name.text for name in columns]))
                # columns_names = [name.text for name in columns]

                table_rows = table.find_elements(By.TAG_NAME, 'tbody')[0].\
                    find_elements(By.TAG_NAME, 'tr')
                table_tds = [item.find_elements(
                    By.XPATH, 'td') for item in table_rows]

                lst = []
                for items in table_tds:
                    lst_tmp = []
                    for index, item in enumerate(items[:-1]):
                        # if index not in [13]:
                        #     if ('...' in item.text and index != 10):
                        #         text = unquote(item.
                        #                        find_element(By.CLASS_NAME, 'text-ellipsise').
                        #                        get_attribute('data-text'))
                        #     else:
                        text = item.text
                        lst_tmp.append(text)

                    lst.append(pd.Series(data=lst_tmp, index=columns_names))

                df = pd.concat(lst, axis=1).T
                df['تاریخ بروزرسانی'] = get_update_date()
                drop_into_db('tbllisthoghogh',
                             df.columns.tolist(),
                             df.values.tolist(),
                             append_to_prev=append_to_prev)
                append_to_prev = True

                # *********************************************************************************

                # Go to next page

                WebDriverWait(self.driver, 8).until(
                    EC.presence_of_element_located(
                        (By.ID, 'karmandstable_next'))).click()
                time.sleep(1)

        print('f')

        return self.driver

    @wrap_a_wrapper
    @wrap_it_with_params(50, 1000000000, False, True, False, False)
    def scrape_nezam_mohandesi(self, path=None, headless=True, creds={}, *args, **kwargs):
        append_to_prev = True
        sql_query = """
        IF OBJECT_ID('tblNezamMohandesiMohndes', 'U') IS NOT NULL
            SELECT max(CAST([تاریخ]  AS INT))  FROM [TestDb].[dbo].[tblNezamMohandesiMohndes]
        ELSE
            SELECT 13980116
        """
        current_date = str(connect_to_sql(
            sql_query,
            sql_con=get_sql_con(database='TestDb'),
            read_from_sql=True,
            return_df=True).loc[0][0])
        if current_date == '13980116':
            append_to_prev = False
        today = get_update_date()

        while int(current_date) < int(today):
            current_date = add_one_day(current_date)
            with init_driver(pathsave=path,
                             driver_type=self.driver_type,
                             headless=headless, use_proxy=True) as self.driver:

                self.driver, self.info = login_nezam_mohandesi(
                    driver=self.driver, info=self.info, creds=creds)
                while (int(current_date) < int(today)):
                    data_malek = []
                    data_mohandes = []
                    WebDriverWait(self.driver, 8).until(
                        EC.presence_of_element_located(
                            (By.ID, 'ContentPlaceHolder1_TextBox1'))).\
                        clear()
                    WebDriverWait(self.driver, 8).until(
                        EC.presence_of_element_located(
                            (By.ID, 'ContentPlaceHolder1_TextBox1'))).\
                        send_keys(
                            f'{current_date[:4]}/{current_date[4:6]}/{current_date[6:8]}')
                    WebDriverWait(self.driver, 8).until(
                        EC.presence_of_element_located(
                            (By.ID, 'ContentPlaceHolder1_Button1'))).click()
                    time.sleep(1)
                    frames = self.driver.find_elements(
                        By.TAG_NAME, 'iframe')
                    if len(frames) > 0:
                        for frame in frames:
                            self.driver.switch_to.frame(frame)
                            # Generate a random UUID (UUID4)
                            unique_id = uuid.uuid4()

                            # Now you can use BeautifulSoup to parse the content inside the iframe
                            iframe_content = self.driver.page_source
                            soup = BeautifulSoup(iframe_content, 'html.parser')
                            # Find all tables in the HTML
                            tables = soup.find_all('table')

                            # Iterate through the tables and print their content
                            for table in tables:
                                table_id = table.get('id')
                                if table_id == 'GridView1':
                                    data1 = []
                                    for row in table.find_all('tr'):
                                        cells = row.find_all(['th', 'td'])
                                        data1.append([cell.text.strip()
                                                      for cell in cells])
                                    df = pd.DataFrame(
                                        data1[1:], columns=data1[:1])
                                    # Convert the UUID to a string
                                    df['uuid'] = str(unique_id)
                                    df['تاریخ'] = current_date
                                    data_malek.append(df)
                                    # drop_into_db(table_name='tblNezamMohandesiMalek',
                                    #              columns=df.columns.tolist(),
                                    #              values=df.values.tolist(),
                                    #              append_to_prev=append_to_prev,
                                    #              db_name='TestDb')
                                if table_id == 'GridView2':
                                    data2 = []
                                    for row in table.find_all('tr'):
                                        cells = row.find_all(['th', 'td'])
                                        data2.append([cell.text.strip()
                                                      for cell in cells])
                                    df = pd.DataFrame(
                                        data2[1:], columns=data2[:1])
                                    df['uuid'] = str(unique_id)
                                    df['تاریخ'] = current_date
                                    data_mohandes.append(df)
                                    # drop_into_db(table_name='tblNezamMohandesiMohndes',
                                    #              columns=df.columns.tolist(),
                                    #              values=df.values.tolist(),
                                    #              append_to_prev=append_to_prev,
                                    #              db_name='TestDb')

                            self.driver.switch_to.default_content()
                        df_malek = pd.concat(data_malek)
                        df_mohandes = pd.concat(data_mohandes)
                        for k, v in {'tblNezamMohandesiMalek': df_malek, 'tblNezamMohandesiMohndes': df_mohandes}.items():
                            drop_into_db(table_name=k,
                                         columns=v.columns.tolist(),
                                         values=v.values.tolist(),
                                         append_to_prev=append_to_prev,
                                         db_name='TestDb')
                        append_to_prev = True
                    current_date = add_one_day(current_date)

        return self.driver, self.info

    # @wrap_a_wrapper
    @wrap_it_with_paramsv1(20, 180, True, True, True, False)
    def scrape_iris(self,
                    path=None,
                    df=None,
                    return_df=False,
                    del_prev_files=True,
                    headless=True,
                    time_out=130,
                    role='manager_phase2',
                    end_time=22,
                    shenase_dadrasi='no',
                    table_name='tbltempheiat',
                    info={}):
        self.info = info
        self.path = path

        if (role == 'manager_phase2'):
            df_final = df[0].loc[(df[0]['shenase_dadrasi_no'].str.len() < 4) & (
                df[0]['msg'].str.len() < 4)
                & (df[0]['second_phase'] == 'yes')]
            # df_final = df[0]

        elif (role == 'manager_phase1'):
            df_final = df[0].loc[(df[0]['is_done'] != 'yes')]
            # df_final = df[0].loc[(df[0]['shenase_dadrasi_no'].str.len() < 6)
            #                      & (df[0]['is_done'] != 'yes')]
            # & (
            #     df[0]['is_done'] != 'no')]
            # df_final = df[0]
        else:
            # df_final = df[0].loc[(df[0]['shenase_dadrasi_no'].str.len() < 4) & (
            #     df[0]['msg'].str.len() < 4)]
            df_final = df[0]
            # df_final = df[0].loc[(df[0]['is_done'] == 'yes')
            #                      & (df[0]['second_phase'] != 'yes')]
            # df_final = df[0]

        if not df_final.empty:
            init = True
            with init_driver(pathsave=path, driver_type=self.driver_type,
                             headless=headless, prefs={'maximize': True,
                                                       'zoom': '0.73'}, disable_popups=True, info=self.info,
                             ) as self.driver:

                for index, item in tqdm(df_final.iterrows()):
                    if init:
                        self.driver, self.info = login_iris(self.driver, creds={'username': str(df[1][role].iloc[0]),
                                                                                'pass': str(df[1]['pass'].iloc[0])}, info=self.info)
                        # self.driver, self.info = login_iris(self.driver, creds={'username': "16950010",
                        #                                                         'pass': "123456"}, info=self.info)
                        if (not self.info['success']):
                            raise Exception

                        self.driver, self.info = find_obj_and_click(
                            driver=self.driver, info=self.info, elm='OBJ', linktext='موارد اعتراض/ شکایت')
                        init = False

                    end_it = False
                    if end_it:

                        cur_time = datetime.datetime.now().hour
                        if cur_time == end_time:
                            self.driver, self.info = cleanup(
                                driver=self.driver, info=self.info)
                            return self.driver
                    stop_threads = False

                    t = CustomThread(target=scrape_iris_helper, args=(
                        lambda: stop_threads, index, item, self.driver, df,
                        role, shenase_dadrasi, table_name))

                    t.start()
                    res = t.join(time_out)
                    if ((res is None)):
                        self.driver, self.info = cleanup(
                            driver=self.driver, info=self.info, close_driver=False)
                        raise Exception
                    if (not res[1]['success']):
                        try:
                            init = True
                            self.driver, self.info = cleanup(
                                driver=self.driver, info=self.info, close_driver=False)
                        except:
                            continue

        return self.driver, self.info
