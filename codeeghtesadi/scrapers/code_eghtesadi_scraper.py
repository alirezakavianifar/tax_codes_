import os
import time
import datetime
import math
import numpy as np
from concurrent.futures import ThreadPoolExecutor, wait, ProcessPoolExecutor
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from automation.helpers import (
    login_codeghtesadi, scrape_it, set_imp_corps, adam, process_vosol_ejra,
    process_chargoon_info, set_hoze, set_arzesh, ravand_residegi, get_heiat_data,
    find_hoghogh_info, set_user_permissions, get_sabtenamCodeEghtesadiData,
    get_eghtesadidata, set_enseraf_heiat, get_dadrasidata, get_dadrasi_new,
    get_amlak, get_modi_info, merge_multiple_excel_sheets, unzip_files,
    save_in_db, connect_to_sql, remove_excel_files, init_driver
)
from automation.scrape_helpers import soratmoamelat_helper
from automation.soratmoamelat_helpers import (
    get_soratmoamelat_link, get_soratmoamelat_report_link,
    get_sorat_selected_year, get_sorat_selected_report
)
from automation.constants import get_soratmoamelat_mapping, get_sql_agg_soratmoamelat, get_sql_con
from .base_scraper import BaseScraper
from automation.helpers import wrap_it_with_params # Needed if I usedecorators, but scrape_codeghtesadi wasn't decorated in scrape.py, only helper functions were.

class CodeEghtesadiScraper(BaseScraper):

    def dadrasi(self, pathsave, driver_type, headless, info, urls, init=False, enseraf=False):
        with self.init_driver(
                path=pathsave, headless=headless) as self.driver: # Using base methods
            
            # Using defaults only if not sensitive, but plan says remove hardcoded.
            # Assuming env vars are set.
            user_name = os.getenv('LOGIN_CODEGHTESADI_USER')
            password = os.getenv('LOGIN_CODEGHTESADI_DADRASI_PASS')
            
            self.driver, self.info = login_codeghtesadi(
                driver=self.driver, data_gathering=False, pred_captcha=False, info=self.info,
                user_name=user_name,
                password=password)

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
            self._handle_soratmoamelat(path, soratmoamelat['params'], headless, **kwargs)

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

    def _handle_important_corps(self, path, params, data_gathering):
        self.driver = scrape_it(path, self.driver_type, data_gathering)
        set_imp_corps(self.driver, params['df'], params['saving_dir'])

    def _handle_adam(self, path, params, data_gathering, headless):
        with self.init_driver(path=path, headless=headless) as self.driver: # Using base method potentially but preserving extra args if possible. Wait, base init_driver only takes path/headless. 
             # Original: with init_driver(...) as self.driver.
             # I need to pass prefs.
             # I should update base init_driver to accept **kwargs or just use init_driver directly from helper if I need specifics.
             # Or I can just pass prefs to automation.helpers.init_driver directly here.
             # I'll use automation.helpers.init_driver to be safe with prefs.
             pass
        
        # Correction: I will use init_driver from helpers for complex cases for now or update BaseScraper.
        # Let's stick to using `init_driver` from helpers directly when extra params are needed, to respect original code.
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
                user_name=os.getenv('LOGIN_CODEGHTESADI_USER'),
                password=os.getenv('LOGIN_CODEGHTESADI_PASS'),
                info=self.info)
            ravand_residegi(driver=self.driver, path=path)

    def _handle_dar_jarian_dadrasi(self, path, data_gathering, pred_captcha, headless):
        remove_excel_files(file_path=path, postfix=['.xls', '.html', 'xlsx'])
        with init_driver(pathsave=path, driver_type=self.driver_type, headless=headless, 
                         info=self.info, prefs={'maximize': True, 'zoom': '0.9'}) as self.driver:
            self.driver, self.info = login_codeghtesadi(
                driver=self.driver, data_gathering=data_gathering,
                user_name=os.getenv('LOGIN_DADRASI_USER'),
                password=os.getenv('LOGIN_DADRASI_PASS'),
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
                               user_name=os.getenv('LOGIN_TAX16_USER'),
                               password=os.getenv('LOGIN_TAX16_PASS'))
            set_user_permissions(self.driver, params['df'])

    def _handle_sabtenam_code_eghtesadi(self, path, params, data_gathering, pred_captcha, headless):
        with init_driver(pathsave=path, driver_type=self.driver_type, headless=headless, 
                         info=self.info, prefs={'maximize': True, 'zoom': '0.9'}) as self.driver:
            self.driver, self.info = login_codeghtesadi(
                driver=self.driver, data_gathering=data_gathering,
                user_name=os.getenv('LOGIN_SORAT_USER'),
                password=os.getenv('LOGIN_SORAT_PASS'),
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
                user_name=os.getenv('LOGIN_MOSTAGHELAT_USER'),
                password=os.getenv('LOGIN_MOSTAGHELAT_PASS'))
            get_amlak(self.driver, self.path, del_prev_files=params['del_prev_files'], info=self.info)

    def _handle_get_info(self, path, params, data_gathering, pred_captcha, headless):
        with init_driver(pathsave=path, driver_type=self.driver_type, headless=headless, 
                         info=self.info, prefs={'maximize': True, 'zoom': '0.9'}) as self.driver:
            login_codeghtesadi(driver=self.driver, data_gathering=data_gathering, pred_captcha=pred_captcha, info=self.info)
            get_modi_info(self.driver, self.path, params['df'], params['saving_dir'])
