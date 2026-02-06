import time
import glob
import os
import uuid
import datetime
import math
import threading
import numpy as np
import pandas as pd
from tqdm import tqdm
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support.select import Select
from selenium.webdriver.support import expected_conditions as EC
from concurrent.futures import ThreadPoolExecutor, wait, ProcessPoolExecutor

from automation.custom_thread import CustomThread
from automation.helpers import (
    login_sanim, login_hoghogh, login_list_hoghogh, login_nezam_mohandesi, add_one_day,
    login_iris, login_arzeshafzoodeh, login_tgju, get_update_date, connect_to_sql,
    login_mostaghelat, login_codeghtesadi, login_soratmoamelat, login_vosolejra,
    login_186, open_and_save_excel_files, maybe_make_dir, input_info, merge_multiple_excel_sheets,
    return_start_end, wait_for_download_to_finish, move_files, remove_excel_files,
    init_driver, insert_into_tbl, extract_nums, retryV1, check_driver_health, login_chargoon,
    is_updated_to_download, drop_into_db, unzip_files, is_updated_to_save, rename_files,
    merge_multiple_html_files, merge_multiple_excel_files, log_the_func, wrap_it_with_params,
    wrap_it_with_paramsv1, cleanup, wrap_a_wrapper, leading_zero
)

from automation.constants import (
    get_dict_years, get_sql_con, lst_years_arzeshafzoodeSonati,
    get_soratmoamelat_mapping, return_sanim_download_links,
    get_newdatefor186, get_url186, get186_titles, ModiHoghogh, ModiHoghoghLst, get_report_links,
    TIMEOUT_1, TIMEOUT_2, TIMEOUT_15, EXCEL_FILE_NAMES, BADVI_FILE_NAMES,
    REPORT_TYPE_TD_MAPPING, get_td_number
)
from automation.selectors import XPATHS
from automation.watchdog_186 import watch_over, is_downloaded
from automation.sql_queries import get_sql_arzeshAfzoodeSonatiV2, get_sql_agg_soratmoamelat

from automation.scrape_helpers import (
    scrape_iris_helper, soratmoamelat_helper, find_obj_and_click, download_excel,
    adam, set_hoze, set_arzesh, find_hoghogh_info, set_user_permissions, set_imp_corps,
    select_from_dropdown, set_start_date, get_sodor_gharar_karshenasi, scrape_it,
    insert_gashtPosti, save_process, get_amar_sodor_ray, get_imp_parvand,
    insert_sabtenamArzeshAfzoodeh, insert_codeEghtesadi, get_heiat_data, ravand_residegi,
    scrape_1000_helper, create_1000parvande_report, get_sabtenamCodeEghtesadiData,
    save_in_db, get_eghtesadidata, get_dadrasidata, set_enseraf_heiat, get_amlak,
    get_modi_info, get_dadrasi_new, scrape_arzeshafzoodeh_helper, get_mergeddf_arzesh,
    insert_arzeshafzoodelSonati, save_excel, check_health, retry_selenium,
    process_vosol_ejra, process_chargoon_info, scrape_mostaghelat_helper, reliable_download
)
from automation.soratmoamelat_helpers import (
    get_soratmoamelat_link, get_soratmoamelat_report_link, get_sorat_selected_year, get_sorat_selected_report
)

# Moving these to constants.py


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
def click_on_down_btn_excelsanimforheiatend(driver, info, report_type=None):
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

    WebDriverWait(driver, TIMEOUT_15).until(
        EC.presence_of_element_located(
            (By.XPATH, info['link_list']))).click()

    return driver, info


@wrap_it_with_params(50, 1000000000, False, False, False, False)
def select_btn_type(driver=None,
                    info=None,
                    report_type=None):

    if report_type == 'ezhar':
        download_button = XPATHS["download_button_ezhar"]
    elif report_type == 'ghatee_sader_shode':
        download_button = XPATHS["download_button_ghatee_sader_shode"]
    else:
        download_button = XPATHS["download_button_rest"]
    WebDriverWait(driver, TIMEOUT_2).until(
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
            (By.XPATH, '%s/td[4]/a' % XPATHS["td_2"])))
    driver.find_element(By.XPATH, '%s/td[%s]/a' %
                        (XPATHS["td_2"], td_number)).click()
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
    def scrape_186(self, path=None, headless=False, *args, **kwargs):
        remove_excel_files(file_path=path, postfix=['.xls', '.html', 'xlsx'])
        with init_driver(pathsave=path, driver_type=self.driver_type, headless=headless) as self.driver:
            scrape_186_helper_v2(self.driver, path)

    def scrape_tgju(self, path=None):
        with init_driver(pathsave=path, driver_type=self.driver_type, headless=True) as driver:
            coin, dollar, gold = login_tgju(driver)
            return coin, dollar, gold

    def scrape_soratmoamelat(self, path=None, headless=False, del_prev_files=True):
        if del_prev_files:
            remove_excel_files(file_path=path, postfix=['.xls', '.html', 'xlsx'])
        with init_driver(pathsave=path, driver_type=self.driver_type, headless=headless) as self.driver:
            scrape_soratmoamelat_helper(self.driver, path, self.info)

    # @retryV1
    # @log_the_func('none')

    def dadrasi(self, pathsave, driver_type, headless, info, urls, init=False, enseraf=False):
        with init_driver(
                pathsave=pathsave, driver_type=driver_type,
                headless=headless, info=info, prefs={'maximize': True,
                                                     'zoom': '0.7'}) as self.driver:
            self.driver, self.info = login_codeghtesadi(
                driver=self.driver, data_gathering=False, pred_captcha=False, info=self.info,
                user_name=os.getenv('LOGIN_CODEGHTESADI_USER', '1756914443'),
                password=os.getenv('LOGIN_CODEGHTESADI_DADRASI_PASS', 'F@fa71791395'))

            if not enseraf:
                self.driver, info = get_dadrasidata(self.driver, pathsave,
                                                    del_prev_files=False, urls=urls, info=info, init=init)
            else:
                self.driver, info = set_enseraf_heiat(self.driver, pathsave,
                                                      heiat_shenases=urls, info=info, init=init)

    def _handle_soratmoamelat(self, path, params, headless, **kwargs):
        if params['scrape']['general']:
            self.driver, self.info = scrape_it(
                path, self.driver_type, headless=headless, driver=self.driver, info=self.info)
            self.driver, self.info = get_soratmoamelat_link(
                driver=self.driver, info=self.info)

            self.driver, self.info = get_soratmoamelat_report_link(
                driver=self.driver, info=self.info)

            if params['scrape']['sorat']['scrape']:
                for year in params['scrape']['sorat']['years']:
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

                        field = kwargs.get('field')
                        soratmoamelat_helper(driver=self.driver,
                                             info=self.info,
                                             path=path,
                                             table_name=tbl_name,
                                             report_type='sorat',
                                             selected_option_text=self.info['selected_option_text'],
                                             index=i,
                                             field=field)

            if params['scrape']['gomrok']:
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

                    soratmoamelat_helper(self.driver, path,
                                         tbl_name, selected_option_text=selected_option_text, report_type='gomrok', index=i, field=kwargs.get('field'))

            if params['scrape']['sayer']:
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

                    soratmoamelat_helper(self.driver, path,
                                         tbl_name, selected_option_text=selected_option_text,
                                         report_type='sayer', index=i, field=kwargs.get('field'))
            self.driver.close()

        if params['unzip']:
            unzip_files(path)

        if params['dropdb']:
            save_in_db(
                path, params['scrape']['sorat']['years'][0])

        if params['gen_report']:
            connect_to_sql(get_sql_agg_soratmoamelat(), sql_con=get_sql_con(
                database='testdb'), num_runs=0)

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
            self._handle_soratmoamelat(path, soratmoamelat['params'], headless, **kwargs)

        if codeeghtesadi['state']:

        if codeeghtesadi['state']:
            params = codeeghtesadi['params']
            
            if params.get('set_important_corps'):
                self._handle_important_corps(path, params, data_gathering)
            elif params.get('adam'):
                self._handle_adam(path, params, data_gathering, headless)
            elif params.get('set_vosol_ejra'):
                self._handle_vosol_ejra(path, codeeghtesadi, headless)
            elif params.get('set_chargoon_info'):
                self._handle_chargoon_info(path, codeeghtesadi, headless)
            elif params.get('set_hoze'):
                self._handle_hoze(path, params, data_gathering, headless)
            elif params.get('set_arzesh'):
                self._handle_arzesh(path, params, data_gathering, headless)
            elif params.get('ravand_residegi'):
                self._handle_ravand_residegi(path, data_gathering, pred_captcha, headless)
            elif params.get('dar_jarian_dadrasi'):
                self._handle_dar_jarian_dadrasi(path, data_gathering, pred_captcha, headless)
            elif params.get('find_hoghogh_info'):
                self._handle_find_hoghogh_info(path, params, data_gathering, headless)
            elif params.get('set_user_permissions'):
                self._handle_set_user_permissions(path, params, data_gathering, headless)
            elif params.get('get_sabtenamCodeEghtesadiData'):
                self._handle_sabtenam_code_eghtesadi(path, params, data_gathering, pred_captcha, headless)
            elif params.get('getdata'):
                self._handle_getdata(path, params, data_gathering, pred_captcha, headless)
            elif params.get('set_enseraf'):
                self._handle_set_enseraf(path, params, df_toadam)
            elif params.get('get_dadrasi_new'):
                self._handle_get_dadrasi_new(path, params, data_gathering, pred_captcha, headless)
            elif params.get('get_dadrasi'):
                self._handle_get_dadrasi(path)
            elif params.get('get_amlak'):
                self._handle_get_amlak(path, params, data_gathering, pred_captcha, headless)
            elif params.get('get_info'):
                self._handle_get_info(path, params, data_gathering, pred_captcha, headless)

            if params.get('merge'):
                merge_multiple_excel_sheets(path, dest=path, table='tblCodeeghtesadi', multithread=True)
        return self.driver, self.info

    def _handle_important_corps(self, path, params, data_gathering):
        self.driver = scrape_it(path, self.driver_type, data_gathering)
        set_imp_corps(self.driver, params['df'], params['saving_dir'])

    def _handle_adam(self, path, params, data_gathering, headless):
        with init_driver(pathsave=path, driver_type=self.driver_type, headless=headless, 
                         info=self.info, prefs={'maximize': True, 'zoom': '0.9'}) as self.driver:
            login_codeghtesadi(driver=self.driver, data_gathering=data_gathering, pred_captcha=False, info=self.info)
            adam(self.driver, params['df'])

    def _handle_vosol_ejra(self, path, codeeghtesadi, headless):
        with init_driver(pathsave=path, driver_type=self.driver_type, headless=headless, 
                         info=self.info, prefs={'maximize': True, 'zoom': '0.9'}) as self.driver:
            process_vosol_ejra(self.driver, self.info, codeeghtesadi)

    def _handle_chargoon_info(self, path, codeeghtesadi, headless):
        with init_driver(pathsave=path, driver_type=self.driver_type, headless=headless, 
                         info=self.info, prefs={'maximize': True, 'zoom': '0.9'}) as self.driver:
            process_chargoon_info(self.driver, self.info, codeeghtesadi)

    def _handle_hoze(self, path, params, data_gathering, headless):
        with init_driver(pathsave=path, driver_type=self.driver_type, headless=headless, 
                         info=self.info, prefs={'maximize': True, 'zoom': '0.9'}) as self.driver:
            login_codeghtesadi(driver=self.driver, data_gathering=data_gathering, pred_captcha=False, info=self.info)
            set_hoze(self.driver, params['df_set_hoze'])

    def _handle_arzesh(self, path, params, data_gathering, headless):
        with init_driver(pathsave=path, driver_type=self.driver_type, headless=headless, 
                         info=self.info, prefs={'maximize': True, 'zoom': '0.9'}) as self.driver:
            login_codeghtesadi(driver=self.driver, data_gathering=data_gathering, pred_captcha=False, info=self.info)
            set_arzesh(self.driver, params['df'], get_date=False)

    def _handle_ravand_residegi(self, path, data_gathering, pred_captcha, headless):
        remove_excel_files(file_path=path, postfix=['.xls', '.html', 'xlsx'])
        with init_driver(pathsave=path, driver_type=self.driver_type, headless=headless, 
                         info=self.info, prefs={'maximize': True, 'zoom': '0.9'}) as self.driver:
            self.driver, self.info = login_codeghtesadi(
                driver=self.driver, data_gathering=data_gathering, pred_captcha=pred_captcha,
                user_name=os.getenv('LOGIN_CODEGHTESADI_USER', '1756914443'),
                password=os.getenv('LOGIN_CODEGHTESADI_PASS', '14579Ali.@'),
                info=self.info)
            ravand_residegi(driver=self.driver, path=path)

    def _handle_dar_jarian_dadrasi(self, path, data_gathering, pred_captcha, headless):
        remove_excel_files(file_path=path, postfix=['.xls', '.html', 'xlsx'])
        with init_driver(pathsave=path, driver_type=self.driver_type, headless=headless, 
                         info=self.info, prefs={'maximize': True, 'zoom': '0.9'}) as self.driver:
            self.driver, self.info = login_codeghtesadi(
                driver=self.driver, data_gathering=data_gathering,
                user_name=os.getenv('LOGIN_DADRASI_USER', '1751164187'),
                password=os.getenv('LOGIN_DADRASI_PASS', 'Aa@123456'),
                pred_captcha=pred_captcha, info=self.info)
            get_heiat_data(driver=self.driver, path=path)

    def _handle_find_hoghogh_info(self, path, params, data_gathering, headless):
        with init_driver(pathsave=path, driver_type=self.driver_type, headless=headless, 
                         info=self.info, prefs={'maximize': True, 'zoom': '0.9'}) as self.driver:
            login_codeghtesadi(driver=self.driver, data_gathering=data_gathering, pred_captcha=False, info=self.info)
            find_hoghogh_info(self.driver, params['df'], get_date=False, 
                              from_scratch=params['find_hoghogh_info_from_scratch'])

    def _handle_set_user_permissions(self, path, params, data_gathering, headless):
        with init_driver(pathsave=path, driver_type=self.driver_type, headless=headless, 
                         info=self.info, prefs={'maximize': True, 'zoom': '0.9'}) as self.driver:
            login_codeghtesadi(driver=self.driver, data_gathering=data_gathering, pred_captcha=False, info=self.info,
                               user_name=os.getenv('LOGIN_TAX16_USER', 'tax16'),
                               password=os.getenv('LOGIN_TAX16_PASS', 'I@nh1157396'))
            set_user_permissions(self.driver, params['df'])

    def _handle_sabtenam_code_eghtesadi(self, path, params, data_gathering, pred_captcha, headless):
        with init_driver(pathsave=path, driver_type=self.driver_type, headless=headless, 
                         info=self.info, prefs={'maximize': True, 'zoom': '0.9'}) as self.driver:
            self.driver, self.info = login_codeghtesadi(
                driver=self.driver, data_gathering=data_gathering,
                user_name=os.getenv('LOGIN_SORAT_USER', '1756914443'),
                password=os.getenv('LOGIN_SORAT_PASS', '1756914443'),
                pred_captcha=pred_captcha, info=self.info)
            get_sabtenamCodeEghtesadiData(path=path, driver=self.driver, info=self.info,
                                          down_url=params['down_url'])

    def _handle_getdata(self, path, params, data_gathering, pred_captcha, headless):
        with init_driver(pathsave=path, driver_type=self.driver_type, headless=headless, info=self.info) as self.driver:
            login_codeghtesadi(driver=self.driver, data_gathering=data_gathering, pred_captcha=pred_captcha, info=self.info)
            get_eghtesadidata(self.driver, self.path, del_prev_files=params['del_prev_files'])

    def _handle_set_enseraf(self, path, params, df_data):
        self.dadrasi(pathsave=path, driver_type=self.driver_type, headless=False,
                     info=self.info, urls=params['df'], init=False, enseraf=True)
        num_procs = min(len(df_data or []), 18)
        if num_procs > 0:
            dfs_users = np.array_split(df_data, num_procs)
            with ProcessPoolExecutor(len(dfs_users)) as executor:
                try:
                    jobs = [executor.submit(self.dadrasi, path, self.driver_type, False, self.info, item)
                            for item in dfs_users]
                    wait(jobs)
                except Exception as e:
                    print(e)

    def _handle_get_dadrasi_new(self, path, params, data_gathering, pred_captcha, headless):
        with init_driver(pathsave=path, driver_type=self.driver_type, headless=headless, 
                         info=self.info, prefs={'maximize': True, 'zoom': '0.9'}) as self.driver:
            login_codeghtesadi(driver=self.driver, data_gathering=data_gathering, pred_captcha=pred_captcha, info=self.info)
            get_dadrasi_new(self.driver, self.path, params['df'], params['saving_dir'])

    def _handle_get_dadrasi(self, path):
        df_data = connect_to_sql('SELECT * FROM tbldadrasiUrls WHERE [تاریخ بروزرسانی] IS NULL',
                                 read_from_sql=True, return_df=True)
        if df_data.empty: return
        num_procs = min(len(df_data), 18)
        dfs_users = np.array_split(df_data, num_procs)
        with ProcessPoolExecutor(len(dfs_users)) as executor:
            try:
                jobs = [executor.submit(self.dadrasi, path, self.driver_type, False, self.info, item)
                        for item in dfs_users]
                wait(jobs)
            except Exception as e:
                print(e)

    def _handle_get_amlak(self, path, params, data_gathering, pred_captcha, headless):
        with init_driver(pathsave=path, driver_type=self.driver_type, headless=headless, 
                         info=self.info, prefs={'maximize': True, 'zoom': '0.7'}) as self.driver:
            self.driver, self.info = login_codeghtesadi(
                driver=self.driver, data_gathering=data_gathering, pred_captcha=pred_captcha, info=self.info,
                user_name=os.getenv('LOGIN_MOSTAGHELAT_USER', '1930841086'),
                password=os.getenv('LOGIN_MOSTAGHELAT_PASS', 'marzi1930841'))
            get_amlak(self.driver, self.path, del_prev_files=params['del_prev_files'], info=self.info)

    def _handle_get_info(self, path, params, data_gathering, pred_captcha, headless):
        with init_driver(pathsave=path, driver_type=self.driver_type, headless=headless, 
                         info=self.info, prefs={'maximize': True, 'zoom': '0.9'}) as self.driver:
            login_codeghtesadi(driver=self.driver, data_gathering=data_gathering, pred_captcha=pred_captcha, info=self.info)
            get_modi_info(self.driver, self.path, params['df'], params['saving_dir'])

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

        if scrape:
            with init_driver(pathsave=path, driver_type=self.driver_type, headless=headless) as driver:
                scrape_mostaghelat_helper(driver, path, report_type, self.info)

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

        time.sleep(2)
        return self.driver, self.info

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

            objection_reports = ['badvi_darjarian_dadrasi', 'badvi_takmil_shode',
                                'tajdidnazer_darjarian_dadrasi', 'tajdidnazar_takmil_shode',
                                'badvi_darjarian_dadrasi_hamarz', 'amar_sodor_gharar_karshenasi',
                                'amar_sodor_ray', 'imp_parvand']

            if self.report_type not in objection_reports:
                self._handle_sanim_standard_download()
            else:
                self._handle_sanim_objection_reports()

        return self.driver, self.info

    def _handle_sanim_standard_download(self):
        download_excel(func=lambda: click_on_down_btn_excelsanim(driver=self.driver, info=self.info),
                       path=self.path,
                       report_type=self.report_type,
                       type_of_excel=self.report_type,
                       no_files_in_path=0,
                       excel_file=BADVI_FILE_NAMES[0],
                       year=self.year,
                       table_name=self.table_name,
                       type_of=self.type_of)

    def _handle_sanim_objection_reports(self):
        btn_id = 'OBJECTION_DETAILS_IR_actions_button'
        xpath_down = XPATHS["download_excel_btn_2"]

        if self.report_type == 'amar_sodor_ray':
            self._handle_amar_sodor_ray()
            return

        if self.report_type == 'amar_sodor_gharar_karshenasi':
            self._handle_sodor_gharar_karshenasi()
            return

        if self.report_type == 'imp_parvand':
            get_imp_parvand(driver=self.driver, info=self.info)

        if self.report_type == 'badvi_darjarian_dadrasi_hamarz':
            self._handle_badvi_hamarz()
            return

        click_on_down_btn_excelsanimforheiat(driver=self.driver, info=self.info, btn_id=btn_id, report_type=self.report_type)
        download_excel(func=lambda: click_on_down_btn_excelsanimforheiatend(driver=self.driver, info=self.info,
                                                                            report_type=self.report_type),
                       path=self.path,
                       report_type=self.report_type,
                       type_of_excel=self.report_type,
                       no_files_in_path=0,
                       excel_file=BADVI_FILE_NAMES[0],
                       year=self.year,
                       table_name=self.table_name,
                       type_of=self.type_of)

    def _handle_amar_sodor_ray(self):
        get_amar_sodor_ray(driver=self.driver, info=self.info)
        download_excel(func=lambda: click_on_down_btn_excelsanim(driver=self.driver, info=self.info, report_type=self.report_type),
                       path=self.path,
                       report_type=self.report_type,
                       type_of_excel=self.report_type,
                       no_files_in_path=0,
                       excel_file=BADVI_FILE_NAMES[0],
                       year=self.year,
                       table_name=self.table_name,
                       type_of=self.type_of)

    def _handle_sodor_gharar_karshenasi(self):
        get_sodor_gharar_karshenasi(driver=self.driver, info=self.info)
        download_excel(func=lambda: click_on_down_btn_excelsanim(driver=self.driver, info=self.info, report_type=self.report_type),
                       path=self.path,
                       report_type=self.report_type,
                       type_of_excel=self.report_type,
                       no_files_in_path=0,
                       excel_file=BADVI_FILE_NAMES[0],
                       year=self.year,
                       table_name=self.table_name,
                       type_of=self.type_of)

    def _handle_badvi_hamarz(self):
        WebDriverWait(self.driver, 8).until(
            EC.presence_of_element_located(
                (By.XPATH,
                    '/html/body/form/div[1]/div/div[2]/main/div[2]/div/div[2]/div/div\
                    /div/div/div[2]/div[2]/div[5]/div[1]/div/div[1]/table/tr/th[4]/a'))).click()
        time.sleep(7)
        edares_elm = WebDriverWait(self.driver, 8).until(
            EC.presence_of_element_located((By.ID, 'OBJECTION_DETAILS_IR_sort_widget_rows')))
        
        soup = BeautifulSoup(edares_elm.get_attribute('outerHTML'), 'html.parser')
        edares = soup.find_all('a', class_='a-IRR-sortWidget-row')
        href_texts = [tag.text.strip() for tag in edares]

        btn_id = 'OBJECTION_DETAILS_IR_actions_button'
        for item in href_texts:
            WebDriverWait(self.driver, 8).until(
                EC.presence_of_element_located((By.ID, 'OBJECTION_DETAILS_IR_search_field'))).send_keys(item)
            time.sleep(2)
            WebDriverWait(self.driver, 8).until(
                EC.presence_of_element_located((By.ID, 'OBJECTION_DETAILS_IR_search_button'))).click()
            
            try:
                while (WebDriverWait(self.driver, 6).until(
                    EC.presence_of_element_located((By.CLASS_NAME, 'u-Processing-spinner'))).is_displayed()):
                    time.sleep(1)
            except:
                click_on_down_btn_excelsanimforheiat(driver=self.driver, info=self.info, btn_id=btn_id, report_type=self.report_type)
                download_excel(func=lambda: click_on_down_btn_excelsanimforheiatend(driver=self.driver, info=self.info,
                                                                                    report_type=self.report_type),
                               path=self.path,
                               report_type=self.report_type,
                               type_of_excel=self.report_type,
                               no_files_in_path=0,
                               excel_file=BADVI_FILE_NAMES[0],
                               year=self.year,
                               table_name=self.table_name,
                               type_of=self.type_of)

                self.driver.find_element(By.XPATH, '//*[@title="Close"]').click()
                time.sleep(2)
                WebDriverWait(self.driver, 8).until(
                    EC.presence_of_element_located(
                        (By.XPATH,
                         '/html/body/form/div[1]/div/div[2]/main/div[2]/div/div[2]/div\
                         /div/div/div/div[2]/div[2]/div[2]/div[2]/ul/li/span[4]/button'))).click()
                try:
                    while (WebDriverWait(self.driver, 6).until(
                        EC.presence_of_element_located((By.CLASS_NAME, 'u-Processing-spinner'))).is_displayed()):
                        time.sleep(1)
                except:
                    time.sleep(1)

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
