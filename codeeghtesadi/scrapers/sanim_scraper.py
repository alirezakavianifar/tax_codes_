import time
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from automation.helpers import (
    login_sanim, download_excel, wrap_it_with_params, 
    get_imp_parvand, get_amar_sodor_ray, get_sodor_gharar_karshenasi
)
from automation.constants import (
    BADVI_FILE_NAMES, return_sanim_download_links
)
from automation.selectors import XPATHS
from .base_scraper import BaseScraper

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

    btns = driver.find_elements(By.TAG_NAME, 'button')

    for btn in btns:
        if btn.text == 'دانلود کردن':
            btn.click()
            break

    time.sleep(0.5)
    if btn_id == "OBJECTION_DETAILS_IR_actions_button":

        if report_type == 'imp_parvand':
            btn_downs = driver.find_element(
                By.XPATH, XPATHS["sanim_dw_ul"])
            btn_downs = btn_downs.find_elements(
                By.TAG_NAME, 'li')

            [btn.click() if btn.text == 'Excel' else print('f')
             for btn in btn_downs]

            WebDriverWait(driver, 3).until(
                EC.presence_of_element_located(
                    (By.XPATH, XPATHS["sanim_label_span"]))).click()
            time.sleep(0.5)
            return driver, info

        WebDriverWait(driver, 3).until(
            EC.presence_of_element_located(
                (By.XPATH, XPATHS["sanim_label_span"]))).click()
        time.sleep(0.5)
        download_formats = driver.find_element(By.ID, 'OBJECTION_DETAILS_IR_download_formats').\
            find_elements(By.TAG_NAME, 'li')
        [li.click() for li in download_formats if li.text == 'Excel']
    else:
        WebDriverWait(driver, 3).until(
            EC.presence_of_element_located(
                (By.XPATH, XPATHS["sanim_label_span_2"]))).click()
        time.sleep(1)
        btns = driver.find_element(By.XPATH, XPATHS["sanim_dw_ul_2"])
        btns = btns.find_elements(By.TAG_NAME, 'li')
        for btn in btns:
            if btn.text == 'HTML':
                btn.click()
                break
    time.sleep(0.5)

    return driver, info


@wrap_it_with_params(1000, 1000000000, False, False, False, False)
def click_on_down_btn_excelsanimforheiatend(driver, info, report_type=None):
    if report_type == 'badvi_darjarian_dadrasi_hamarz':
        driver.find_element(
            By.XPATH, XPATHS["badvi_darjarian_dadrasi_hamarz_btn"]).click()

    elif report_type == 'amar_sodor_ray':
        driver.find_element(
            By.XPATH, XPATHS["amar_sodor_ray_btn"]).click()
    elif report_type == 'amar_sodor_gharar_karshenasi':
        driver.find_element(
            By.XPATH, XPATHS["amar_sodor_gharar_karshenasi_btn"]).click()
    else:
        btn_excels = driver.find_element(By.XPATH, XPATHS["sanim_excel_btn_main"])
        btn_excels = btn_excels.find_elements(By.TAG_NAME, 'button')
        [btn.click() for btn in btn_excels if btn.text == 'Excel']

    return driver, info


@wrap_it_with_params(1000, 1000000000, False, False, False, False)
def click_on_down_btn_excelsanim(driver, info, report_type=None):
    if report_type == "amar_sodor_gharar_karshenasi":
        excel_btn = XPATHS["sanim_excel_btn_main"]
    elif report_type == "amar_sodor_ray":
        excel_btn = XPATHS["sanim_excel_btn_ray"]

    else:
        excel_btn = XPATHS["sanim_excel_btn_main"]
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


class SanimScraper(BaseScraper):

    def __init__(self, *args, report_type=None, year=None, table_name=None, type_of=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.report_type = report_type
        self.year = year
        self.table_name = table_name
        self.type_of = type_of

    @wrap_it_with_params(50, 1000000000, False, True, True, False)
    def scrape_sanim(self, *args, **kwargs):
        with self.init_driver(path=self.path, headless=self.headless) as self.driver:
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
        # xpath_down = XPATHS["download_excel_btn_2"] # Unused

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
                    XPATHS["sanim_badvi_hamarz_link"]))).click()
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

                self.driver.find_element(By.XPATH, XPATHS["close_btn"]).click()
                time.sleep(2)
                WebDriverWait(self.driver, 8).until(
                    EC.presence_of_element_located(
                        (By.XPATH, XPATHS["sanim_badvi_hamarz_close_target"]))).click()
                try:
                    while (WebDriverWait(self.driver, 6).until(
                        EC.presence_of_element_located((By.CLASS_NAME, 'u-Processing-spinner'))).is_displayed()):
                        time.sleep(1)
                except:
                    time.sleep(1)

        return self.driver, self.info
