import time
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from automation.helpers import wrap_it_with_params
from automation.selectors import XPATHS
from automation.constants import TIMEOUT_15, TIMEOUT_2, year_button_3

@wrap_it_with_params(50, 1000000000, False, False, False, False)
def list_details(driver=None, info=None, report_type='ezhar', manba='hoghoghi'):
    if (report_type == 'tashkhis_sader_shode' and manba == 'hoghoghi'):
        info['link_list'] = XPATHS["list_details_tashkhis_sader_shode"]

    elif (report_type == 'tashkhis_eblagh_shode' and manba == 'hoghoghi'):
        info['link_list'] = XPATHS["list_details_tashkhis_eblagh_shode"]

    elif (report_type == 'ezhar' and manba == 'hoghoghi'):
        info['link_list'] = XPATHS["list_details_ezhar"]

    elif (report_type == 'ghatee_sader_shode' and manba == 'hoghoghi'):
        info['link_list'] = XPATHS["list_details_ghatee_sader_shode"]

    elif (report_type == 'ghatee_sader_shode' and manba == 'haghighi'):
        info['link_list'] = XPATHS["list_details_ghatee_sader_shode_haghighi"]

    elif (report_type == 'ghatee_sader_shode' and manba == 'arzesh'):
        info['link_list'] = XPATHS["list_details_ghatee_sader_shode_arzesh"]

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
            (By.XPATH, XPATHS["select_year_input"]))).clear()

    WebDriverWait(driver, 32).until(
        EC.presence_of_element_located(
            (By.XPATH, XPATHS["select_year_input"]))).send_keys(
        year)

    driver.find_element(
        By.XPATH, XPATHS["select_year_input"]).send_keys(Keys.ENTER)

    WebDriverWait(driver, 32).until(
        EC.presence_of_element_located((By.XPATH, XPATHS["select_year_btn"]))).click()

    WebDriverWait(driver, 32).until(
        EC.presence_of_element_located((By.XPATH, year_button_3))).click()

    while (year != WebDriverWait(driver, 32).until(
        EC.presence_of_element_located((By.XPATH, XPATHS["select_year_verify"]))).text):
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
                                        XPATHS["get_main_menu_1"]))).click()
    time.sleep(1)
    WebDriverWait(driver, 24).until(
        EC.presence_of_element_located((By.XPATH,
                                        XPATHS["get_main_menu_2"]))).click()
    return driver, info
