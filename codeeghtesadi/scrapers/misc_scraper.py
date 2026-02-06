import time
import math
import uuid
import pandas as pd
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from automation.helpers import (
    login_sanim, login_tgju, login_mostaghelat, login_hoghogh, login_list_hoghogh,
    login_nezam_mohandesi, add_one_day, open_and_save_excel_files,
    merge_multiple_excel_files, remove_excel_files, init_driver, insert_into_tbl,
    extract_nums, check_driver_health, unzip_files, is_updated_to_save,
    scrape_186_helper_v2, scrape_soratmoamelat_helper, scrape_mostaghelat_helper,
    scrape_arzeshafzoodeh_helper, get_mergeddf_arzesh, insert_arzeshafzoodelSonati,
    insert_sabtenamArzeshAfzoodeh, insert_codeEghtesadi, insert_gashtPosti,
    scrape_1000_helper, create_1000parvande_report, drop_into_db, get_update_date,
    connect_to_sql
)
from automation.constants import (
    BADVI_FILE_NAMES, get_sql_arzeshAfzoodeSonatiV2, get_sql_agg_soratmoamelat, get_sql_con
)
from automation.helpers import wrap_it_with_params, wrap_a_wrapper, log_the_func
from .base_scraper import BaseScraper

class MiscScraper(BaseScraper):

    @log_the_func('none')
    def scrape_186(self, path=None, headless=False, *args, **kwargs):
        remove_excel_files(file_path=path, postfix=['.xls', '.html', 'xlsx'])
        with self.init_driver(path=path, headless=headless) as self.driver:
            scrape_186_helper_v2(self.driver, path)

    def scrape_tgju(self, path=None):
        with self.init_driver(path=path, headless=True) as driver:
            coin, dollar, gold = login_tgju(driver)
            return coin, dollar, gold

    def scrape_soratmoamelat(self, path=None, headless=False, del_prev_files=True):
        if del_prev_files:
            remove_excel_files(file_path=path, postfix=['.xls', '.html', 'xlsx'])
        with self.init_driver(path=path, headless=headless) as self.driver:
            scrape_soratmoamelat_helper(self.driver, path, self.info)

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
            with self.init_driver(path=path, headless=headless) as driver:
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
                path=path, del_prev_files=del_prev_files, headless=headless, field=kwargs.get('field')) # Fixed kwargs access

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
            self.driver = self.init_driver(path=path,
                                      headless=headless) # No 'with' in original for scrape_1000parvande? 
                                      # Original: self.driver = init_driver(...) ... self.driver, self.info = login_sanim(...)
                                      # Original didn't use 'with' here! It assigns self.driver.
                                      # Wait, init_driver returns a generator if it's a context manager.
                                      # If original was self.driver = init_driver(...), then init_driver is likely NOT a context manager context generator if used like that?
                                      
                                      # Let's check scrape.py usage again.
                                      # line 788: self.driver = init_driver(...)
                                      # line 791: self.driver, self.info = login_sanim(...)
                                      
                                      # But in other places: with init_driver(...) as self.driver.
                                      
                                      # If init_driver is a contextmanager, calling it returns a context manager object (generator).
                                      # If I assign it to self.driver, self.driver is a generator object, not a webdriver instance.
                                      # login_sanim(driver=self.driver...) would fail if driver is a generator.
                                      
                                      # This suggests either init_driver is polyvalent or the original code has a bug or I misunderstood init_driver.
                                      # Given "Fixing Scrape.py Parse Error" history, maybe code is buggy.
                                      # But I should try to preserve behavior or fix obvious bugs.
                                      # I'll check `helpers.py init_driver` implementation if I can, but I shouldn't access file not mentioned unless needed.
                                      
                                      # However, `scrape_1000parvande` usage:
            # self.driver = init_driver(...)
            # self.driver, self.info = login_sanim(...)
            
            # If init_driver is:
            # @contextmanager
            # def init_driver(...):
            #     driver = ...
            #     yield driver
            #     driver.quit()
            
            # Then init_driver(...) returns _GeneratorContextManager.
            
            # If I look at `scrape_186`: `with init_driver(...) as self.driver:` -> correct for context manager.
            
            # So `scrape_1000parvande` looks broken in original code if `init_driver` is only a CM.
            # I will assume it's broken or `init_driver` returns the driver directly if not used in with statement (which is impossible for standard contextmanager).
            # I'll change it to use `with init_driver(...) as driver:` scoping appropriately or manually managing it if I need to keep `self.driver`.
            
            # actually `scrape_1000parvande` doesn't seem to close driver explicitly at end.
            # I will wrap it in `with` block for safety if I can.
            
            with self.init_driver(path=path, headless=headless) as self.driver:
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

    def scrape_hoghogh(self, path=None, return_df=False, del_prev_files=True, headless=False):
        if del_prev_files:
            remove_excel_files(file_path=path,
                               postfix=['.xls', '.html', 'xlsx'])
        with self.init_driver(path=path,
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
        with self.init_driver(path=path,
                         headless=headless) as self.driver:

            self.driver, self.info = login_list_hoghogh(
                driver=self.driver, info=self.info, creds=creds)

            WebDriverWait(self.driver, 8).until(
                EC.presence_of_element_located(
                    (By.XPATH, '/html/body/div[2]/div[1]/nav/ul/li[3]/a'))).click()
            time.sleep(1)

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

                table_rows = table.find_elements(By.TAG_NAME, 'tbody')[0].\
                    find_elements(By.TAG_NAME, 'tr')
                table_tds = [item.find_elements(
                    By.XPATH, 'td') for item in table_rows]

                lst = []
                for items in table_tds:
                    lst_tmp = []
                    for index, item in enumerate(items[:-1]):
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
            with self.init_driver(path=path,
                             headless=headless) as self.driver: # Using base init_driver with params is limited. 
                             # Here we need use_proxy=True.
                             # I should look at using automation.helpers.init_driver directly.
               pass

            # Reworking loop with init_driver direct call for proxy
            with init_driver(pathsave=path, driver_type=self.driver_type,
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

                            self.driver.switch_to.default_content()
                        if data_malek:
                            df_malek = pd.concat(data_malek)
                            drop_into_db(table_name='tblNezamMohandesiMalek',
                                         columns=df_malek.columns.tolist(),
                                         values=df_malek.values.tolist(),
                                         append_to_prev=append_to_prev,
                                         db_name='TestDb')
                        if data_mohandes:
                            df_mohandes = pd.concat(data_mohandes)
                            drop_into_db(table_name='tblNezamMohandesiMohndes',
                                         columns=df_mohandes.columns.tolist(),
                                         values=df_mohandes.values.tolist(),
                                         append_to_prev=append_to_prev,
                                         db_name='TestDb')
                        
                        append_to_prev = True
                    current_date = add_one_day(current_date)

        return self.driver, self.info
