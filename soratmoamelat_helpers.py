
import os
import time
import glob
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support.select import Select
from selenium.webdriver.support import expected_conditions as EC

from automation.watchdog_186 import watch_over, is_downloaded
from automation.helpers import move_files, wait_for_download_to_finish, maybe_make_dir, extract_nums, wrap_it_with_params


def back_it(driver):
    """Sorat moamelat helper method"""
    WebDriverWait(driver, 15).until(
        EC.presence_of_element_located(
            (By.XPATH,
                '/html/body/form/table/tbody/tr[1]/td[2]/div[1]/button')))
    driver.find_element(
        By.XPATH,
        '/html/body/form/table/tbody/tr[1]/td[2]/div[1]/button').click()
    WebDriverWait(driver, 15).until(
        EC.presence_of_element_located(
            (By.XPATH,
                '/html/body/form/table/tbody/tr[1]/td[2]/div[1]/ul/li/a')))
    driver.find_element(
        By.XPATH,
        '/html/body/form/table/tbody/tr[1]/td[2]/div[1]/ul/li/a').click()
    return driver


@wrap_it_with_params(num_tries=15, driver_based=True, detrimental=True)
def recur_down(driver, info, start_p, end_p, index):
    """Sorat moamelat helper method"""
    WebDriverWait(driver, 1).until(
        EC.presence_of_element_located((By.XPATH, '//*[@id="ddmImpurePrice"]'))).click()

    price1 = 'CPC_urvRemainView%s_ImpurePrice1' % index
    price2 = 'CPC_urvRemainView%s_ImpurePrice2' % index
    WebDriverWait(driver, 1).until(
        EC.presence_of_element_located(
            (By.ID, price1))).clear()
    driver.find_element(By.ID,
                        price1).send_keys(start_p)

    WebDriverWait(driver, 3).until(
        EC.presence_of_element_located(
            (By.ID,
                price2))).clear()
    driver.find_element(
        By.ID, price2).send_keys(end_p)
    driver.find_element(
        By.ID, price2).send_keys(Keys.RETURN)

    is_displayed = True
    while is_displayed:
        try:
            is_displayed = WebDriverWait(driver, 2).until(
                EC.presence_of_element_located(
                    (By.XPATH, '/html/body/form/table/tbody/tr[1]/td[2]/div[2]/div\
                        /div/div/div/div/div[1]/div/div/div[2]/div/div/div/div/div/img'))).is_displayed()
            print('waiting')
        except:
            break
    try:
        time.sleep(1)
        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((
                By.XPATH,
                '/html/body/form/table/tbody/tr[1]/td[2]/div[2]/div/div/div/div/div/div[1]/div/div/div[1]/table/tbody/tr/td[4]'
            )))
        while not count_changed:
            count = driver.find_element(
                By.XPATH,
                '/html/body/form/table/tbody/tr[1]/td[2]/div[2]/div/div/div/div/div/div[1]/div/div/div[1]/table/tbody/tr/td[4]/span/b'
            ).text
            info['count'] = int(extract_nums(count.replace(',', ''))[0])
            if info['count'] == count:
                continue

        return driver, info
    except Exception as e:
        return 0


@wrap_it_with_params(num_tries=15, driver_based=True, detrimental=True)
def if_less_then_down(driver, info={}, name=None, path=None, report_type='gomrok', index=None, *args, **kwargs):
    try:
        if report_type == 'gomrok':
            down_loc = '//*[@id="ddlExcel"]'
            btn_down_loc = 'CPC_repStrRemCu_Act_btnTotalOutputExcel'
        else:
            down_loc = '/html/body/form/table/tbody/tr[1]/td[2]/div[2]/div/div/div/div/div/div[1]/div/div/div[1]/table/tbody/tr/td[3]/div/div/button'
            btn_down_loc = 'CPC_urvRemainView%s_btnTotalOutputExcel' % index

        WebDriverWait(driver, 2).until(
            EC.presence_of_element_located((
                By.XPATH,
                down_loc
            ))).click()

    except Exception as e:
        ...

    time.sleep(2)
    while True:
        try:
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((
                    By.ID,
                    btn_down_loc
                )))
            btn_down = driver.find_element(
                By.ID,
                btn_down_loc
            )
            time.sleep(2)
            driver.execute_script("arguments[0].click();", btn_down)
            break
        except:
            btn_down_loc = 'CPC_urvRemainViewSolr%s_btnTotalOutputExcel' % index

    watch_over(path, 3500, field=kwargs['field'])
    wait_for_download_to_finish(path, 'zip')
    time.sleep(3)
    dir_to_move = os.path.join(path, name)
    maybe_make_dir([dir_to_move])
    srcs = glob.glob(path + "/*" + 'zip')
    dsts = []
    for i in range(len(srcs)):
        dsts.append(dir_to_move)
    move_files(srcs, dsts, field=kwargs['field'])

    return driver, info


@wrap_it_with_params(4, 10, True, True, False, False)
def get_sorat_selected_year(driver=None, info={}, year=None):
    info['sel_year'] = Select(
        driver.find_element(
            By.ID, 'CPC_Remained_Str_Rem_ddlTTMSYear'))
    info['sel_year'].select_by_value(str(year))
    info['selected_year_text'] = info['sel_year'].first_selected_option.text
    return driver, info


@wrap_it_with_params(4, 10, True, True, False, False)
def get_sorat_selected_report(driver=None, info={}, index=None):
    sel = Select(driver.find_element(
        By.ID, 'CPC_Remained_Str_Rem_ddlTTMSCategory'))
    sel.select_by_value(str(index))

    info['selected_option_text'] = sel.first_selected_option.text
    return driver, info


@wrap_it_with_params(4, 10, True, True, False, False)
def get_soratmoamelat_report_link(driver, info):
    WebDriverWait(driver, 2).until(
        EC.presence_of_element_located(
            (By.XPATH,
             '/html/body/form/table/tbody/tr[1]/td[1]/span/div[9]'))).click()
    WebDriverWait(driver, 2).until(
        EC.presence_of_element_located((
            By.XPATH,
            '/html/body/form/table/tbody/tr[1]/td[1]/span/div[10]/a[3]/div'
        ))).click()
    return driver, info


@wrap_it_with_params(1, 10, True, True, False, False)
def get_soratmoamelat_link(driver, info):
    WebDriverWait(driver, 16).until(
        EC.presence_of_element_located((
            By.XPATH,
            '/html/body/div/div[3]/div[2]/div/div/div[2]/div/div[2]/div/a/h6'
        ))).click()
    try:
        if (WebDriverWait(driver, 1).until(
            EC.presence_of_element_located(
                (By.XPATH,
                    '/html/body/form/div[3]/div/div/div[1]/span')))):
            info['success'] = False
            raise Exception
    except Exception as e:
        return driver, info


def recur_until_less(start_p=None, end_p=None, selected_option_text=None, index=None,
                     accepted_number=None, driver=None, path=None, *args, **kwargs):
    new_end_p = end_p
    while count > accepted_number:

        if (int(new_end_p * 0.8) > start_p):
            new_end_p = int(int(new_end_p) * 0.8)

        elif (int(new_end_p * 0.7) < start_p):
            new_end_p = int((int(start_p)) * 2)

        driver, count = recur_down(driver, start_p, new_end_p, index)

    if (count != 0 and count < accepted_number):
        driver, count = recur_down(driver, start_p, new_end_p, index)
        driver = if_less_then_down(
            driver, selected_option_text, path, index=index, field=kwargs['field'])

        return new_end_p + 1, count, driver
