import time
import glob
import os
import tqdm
import numpy as np
import pandas as pd
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support.select import Select
from selenium.webdriver.support import expected_conditions as EC
from functools import wraps
from datetime import datetime
from automation.helpers import init_driver


def wrap_a_wrapper(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            result, success = func(*args, **kwargs)
            if (success):
                return result
            else:
                return result, False
        except:
            raise Exception

    return wrapper


def wrap_it_with_params(num_tries=3, time_out=10, driver_based=False, detrimental=True, clean_up=False, keep_alive=False):
    def wrap_it(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            nonlocal num_tries
            result = None
            i = 0
            while (True):
                if i < num_tries:
                    print(
                        f'function {func.__name__} is called at {datetime.now()}')
                    try:
                        start = time.process_time()
                        result = func(*args, **kwargs)
                        end = time.process_time()
                        elapsed_time = end - start
                        if elapsed_time > time_out:
                            result = None
                        if result is None:
                            raise Exception
                        else:
                            print(
                                f'function {func.__name__} is finished at {datetime.now()}')
                            break
                    except Exception as e:
                        print(f'Exception occured in function {func.__name__}')
                        time.sleep(1)
                        print(e)
                        i += 1
                        if i < num_tries:
                            print(
                                f'retyring function {func.__name__} for {i} times at {datetime.now()}')
                else:
                    try:
                        if (driver_based):

                            if 'driver' in kwargs:
                                driver = kwargs['driver']
                            else:
                                for item in args:
                                    if isinstance(item, selenium.webdriver.firefox.webdriver.WebDriver):
                                        driver = item
                            result = driver
                        if clean_up:
                            cleanup(driver)

                        if detrimental:
                            driver.close()

                        if keep_alive:
                            return result, True

                        return result, False
                    except Exception as e:
                        return result, False

            return result, True
        return wrapper
    return wrap_it


@wrap_a_wrapper
@wrap_it_with_params(2, 10, True, False, False, True)
def check_if_shenase_exists(driver):
    WebDriverWait(driver, 8).until(
        EC.element_to_be_clickable(
            (By.XPATH, '/html/body/table/tbody/tr/td/div[1]/div[2]/table/tbody/tr[4]/td/table/tbody/tr[2]/td/div/div[2]/div/div[2]/table/tbody/tr/td[2]/div')))
    return driver


@wrap_a_wrapper
@wrap_it_with_params(10000, 60, True, False, False, False)
def login_iris(driver, creds=None):
    driver.get("https://its.tax.gov.ir/flexform2/logineris/form2login.jsp")

    time.sleep(4)
    WebDriverWait(driver, 2).until(
        EC.element_to_be_clickable(
            (By.NAME,
                'userName'))).send_keys(creds['username'])

    WebDriverWait(driver, 2).until(
        EC.element_to_be_clickable(
            (By.NAME,
                'password'))).send_keys(creds['pass'])

    time.sleep(2)

    WebDriverWait(driver, 3).until(
        EC.element_to_be_clickable(
            (By.ID,
                'ok_but'))).click()
    time.sleep(0.5)

    try:
        alert_obj = driver.switch_to.alert
        alert_obj.accept()
        error = True
    except:
        error = False

    if error:
        raise Exception

    driver = find_obj_login(driver)

    return driver


@wrap_a_wrapper
@wrap_it_with_params(10, 10, True, False, False, False)
def clear_and_send_keys(driver, item):
    WebDriverWait(driver, 2).until(
        EC.element_to_be_clickable(
            (By.XPATH,
             '/html/body/table/tbody/tr/td/div[1]/div[2]/table/tbody/tr[3]/td/table/tbody/tr[2]/td[2]/div/input'))).clear()
    time.sleep(2)
    WebDriverWait(driver, 2).until(
        EC.element_to_be_clickable(
            (By.XPATH,
             '/html/body/table/tbody/tr/td/div[1]/div[2]/table/tbody/tr[3]/td/table/tbody/tr[2]/td[2]/div/input'))).send_keys(str(item[0]))
    time.sleep(1)
    WebDriverWait(driver, 6).until(
        EC.element_to_be_clickable(
            (By.XPATH, '/html/body/table/tbody/tr/td/div[1]/div[1]/div/div[1]/ul/li[4]/div/div[1]/div[2]/input'))).click()
    elm = WebDriverWait(driver, 2).until(
        EC.presence_of_element_located(
            (By.XPATH, '/html/body/div[6]'))).is_displayed()

    # while (WebDriverWait(driver, 2).until(
    #     EC.presence_of_element_located(
    #         (By.XPATH, '/html/body/div[6]'))).is_displayed()):
    #     try:
    #         continue
    #     except:
    #         break

    driver = check_if_shenase_exists(driver=driver)

    return driver


@wrap_a_wrapper
@wrap_it_with_params(15, 10, True, False, False, True)
def select_row(driver):
    WebDriverWait(driver, 7).until(
        EC.element_to_be_clickable(
            (By.XPATH, '/html/body/table/tbody/tr/td/div[1]/div[2]/table/tbody/tr[4]/td/table/tbody/tr[2]/td/div/div[2]/div/div[2]/table/tbody/tr/td[8]/div'))).click()

    time.sleep(2)
    return driver


@wrap_a_wrapper
@wrap_it_with_params(12, 12, True, False, False, True)
def assign_btn(driver):
    time.sleep(0.5)
    WebDriverWait(driver, 1).until(
        EC.element_to_be_clickable(
            (By.XPATH, '/html/body/table/tbody/tr/td/div[1]/div[2]/table/tbody/tr[4]/td/table/tbody/tr[2]/td/div/div[2]/div/div[2]/table/tbody/tr/td[3]/div')))
    driver.find_element(
        By.XPATH, '/html/body/table/tbody/tr/td/div[1]/div[2]/table/tbody/tr[4]/td/table/tbody/tr[2]/td/div/div[2]/div/div[2]/table/tbody/tr/td[3]/div').click()

    time.sleep(1)
    return driver


@wrap_a_wrapper
@wrap_it_with_params(12, 30, True, False, False, True)
def get_info(driver):
    info = driver.find_element(
        By.XPATH, '/html/body/table/tbody/tr/td/div[1]/div[2]/table/tbody/tr[4]/td/table/tbody/tr[2]/td/div/div[2]/div/div[2]/table/tbody/tr/td[8]/div').text
    return driver, info


@wrap_a_wrapper
@wrap_it_with_params(1, 60, False, False, False)
def save_excel(index, df, file_name, backup_File, done='yes', success='yes',
               driver=None, second_phase='no', role='manager'):
    if role == 'manager':
        df[0].loc[index, 'is_done'] = done
        df[0].loc[index, 'success'] = success
        df[0].loc[index, 'assigned_to'] = str(
            df[1]['employee'].head(1).item())
    else:
        df[0].loc[index, 'second_phase'] = done

    if index % 2 == 0:

        remove_excel_files(files=[file_name],
                           postfix='xlsx')
        df[0].to_excel(
            file_name, index=False)

    if index % 10 == 0:
        df[0].to_excel(
            backup_File, index=False)

    return driver


@wrap_a_wrapper
@wrap_it_with_params(11, 3, True, False, False, False)
def go_to_next_frame(driver):
    WebDriverWait(driver, 1).until(
        EC.element_to_be_clickable(
            (By.XPATH, '/html/body/table/tbody/tr/td/div[1]/div[1]/div/div[1]/ul/li[6]/div/div[1]/div[2]/input'))).click()
    time.sleep(4)
    driver.switch_to.default_content()
    frame = driver.find_element(
        By.XPATH, '/html/body/div[1]/div[1]/div/div/div/div/div/iframe[3]')
    driver.switch_to.frame(frame)

    return driver


@wrap_a_wrapper
@wrap_it_with_params(1, 20, True, False, False, True)
def handle_error(driver):

    while True:
        fraud = 'nofraud'
        try:

            driver.switch_to.default_content()

            if (WebDriverWait(driver, 2).until(
                    EC.element_to_be_clickable(
                    (By.XPATH, '//*[@id="flexPopupErrMsgBtn"]')))):
                driver.find_element(
                    By.XPATH, '//*[@id="flexPopupErrMsgBtn"]').click()
                time.sleep(2)
                driver.find_element(
                    By.XPATH, '/html/body/div[2]/div[3]/table/tbody/tr/td/ul/li[3]/a[2]').click()

                time.sleep(1)

                fraud = 'isfraud'

                frame = driver.find_element(
                    By.XPATH, '/html/body/div[1]/div[1]/div/div/div/div/div/iframe[2]')
                driver.switch_to.frame(frame)
                return driver, fraud
            # else:
            #     fraud = 'isfraud'
            #     frame = driver.find_element(
            #         By.XPATH, '/html/body/div[1]/div[1]/div/div/div/div/div/iframe[3]')
            #     driver.switch_to.frame(frame)
            #     return driver, fraud

        except Exception as e:
            try:
                frame = driver.find_element(
                    By.XPATH, '/html/body/div[1]/div[1]/div/div/div/div/div/iframe[3]')
                driver.switch_to.frame(frame)
                WebDriverWait(driver, 1).until(
                    EC.element_to_be_clickable(
                        (By.XPATH, '/html/body/table/tbody/tr/td/div[1]/div[2]/table/tbody/tr[8]/td/table/tbody/tr[2]/td[4]/div/input[2]')))
                fraud = 'isfraud'
                return driver, fraud
            except:
                return driver, fraud
        except:

            return driver, fraud


@wrap_a_wrapper
@wrap_it_with_params(1, 20, True, False, False, True)
def if_apply_new_assignment(driver):
    try:
        try:
            WebDriverWait(driver, 2).until(
                EC.presence_of_element_located(
                    (By.XPATH,
                        '/html/body/table/tbody/tr/td/div[1]/div[2]/table/tbody/tr[8]/td/table/tbody/tr[2]/td[4]/div/input[2]')))
        except:
            return driver
        raise Exception
    except:
        raise Exception

    # if text == 'مورد اعتراض/شکایت':
    #     raise Exception


@wrap_a_wrapper
@wrap_it_with_params(1, 20, True, False, False, True)
def apply_new_assignment(driver):
    WebDriverWait(driver, 1).until(
        EC.element_to_be_clickable(
            (By.XPATH,
             '/html/body/table/tbody/tr/td/div[1]/div[1]/div/div[1]/ul/li[2]/div/div[1]/div[2]/input'))).click()
    time.sleep(2)
    driver, fraud = handle_error(driver)
    if fraud == 'isfraud':
        # return driver, fraud
        raise Exception
    driver = if_apply_new_assignment(driver)
    return driver


@wrap_a_wrapper
@wrap_it_with_params(5, 20, True, False, False, False)
def insert_new_assignment(driver, df):
    time.sleep(2)
    WebDriverWait(driver, 2).until(
        EC.element_to_be_clickable(
            (By.XPATH,
             '/html/body/table/tbody/tr/td/div[1]/div[2]/table/tbody/tr[8]/td/table/tbody/tr[2]/td[4]/div/input[2]'))).click()
    time.sleep(2)
    driver.find_element(
        By.XPATH,
        '/html/body/table/tbody/tr/td/div[1]/div[2]/table/tbody/tr[8]/td/table/tbody/tr[2]/td[4]/div/input[2]').send_keys(str(df[1]['employee'].head(1).item()))
    time.sleep(2)
    driver = apply_new_assignment(driver)

    if isinstance(driver, tuple):
        driver, success = driver
        if isinstance(success, str):
            if success == 'isfraud':
                raise Exception
            return driver, success
        raise Exception
    time.sleep(1)

    return driver


def check_health(driver, index, df, file_name, backup_File, serious=True, role='manager'):
    if isinstance(driver, tuple):
        driver, success = driver

        if success is not None:
            if not success:
                driver = save_excel(index, df, file_name, backup_File,
                                    'yes', 'no', driver, 'no', role)
                if serious:
                    driver = login_iris(driver, creds={'username': df[1][role].head(1).item(),
                                                       'pass': df[1]['pass'].head(1).item()})
                    driver = find_obj_and_click(driver)

                return driver, False
            else:
                return driver, True

    return driver, True


@wrap_a_wrapper
@wrap_it_with_params(9, 10, True, False, True)
def find_obj_and_click(driver):
    WebDriverWait(driver, 8).until(
        EC.element_to_be_clickable(
            (By.ID,
                'OBJ')))
    driver.find_element(
        By.ID,
        'OBJ').click()

    time.sleep(3)

    WebDriverWait(driver, 8).until(
        EC.element_to_be_clickable(
            (By.LINK_TEXT,
                'موارد اعتراض/ شکایت'))).click()

    time.sleep(3)

    frame = driver.find_element(
        By.XPATH, '/html/body/div[1]/div[1]/div/div/div/div/div/iframe[2]')
    driver.switch_to.frame(frame)

    WebDriverWait(driver, 16).until(
        EC.element_to_be_clickable(
            (By.XPATH,
                '//*[@id="RequestNo"]'))).click()
    time.sleep(1)

    return driver


@wrap_a_wrapper
@wrap_it_with_params(1, 70, True, False, False, False)
def scrape_iris_helper(stop, index, item, driver, df, file_name, backup_file, role):
    # while (True):
    # try:

    time.sleep(1)
    driver = clear_and_send_keys(driver=driver, item=item)

    driver, success = check_health(
        driver, index, df, file_name, backup_file, role)

    if not success:
        return driver

    # driver = clear_and_send_keys(driver=driver, item=item)

    time.sleep(2)

    driver = select_row(driver=driver)

    driver, success = check_health(
        driver, index, df, file_name, backup_file)

    if not success:
        return driver

    driver, info = get_info(driver=driver)

    driver, success = check_health(
        driver, index, df, file_name, backup_file)

    if not success:
        return driver

    # if condition true then continue with the next item
    if info == str(df[1]['employee'].head(1).item()):

        driver = save_excel(index, df, file_name, backup_file,
                            'yes', 'yes', driver, 'no', role)
        return driver

    # if condition false, then assign taskto the new user
    driver = assign_btn(driver=driver)

    driver, success = check_health(
        driver, index, df, file_name, backup_file)

    if not success:
        return driver

    time.sleep(1)

    driver = go_to_next_frame(driver=driver)

    driver, success = check_health(
        driver, index, df, file_name, backup_file, False)

    if not success:
        return driver

    time.sleep(2)

    driver = insert_new_assignment(driver=driver, df=df)

    driver, success = check_health(
        driver, index, df, file_name, backup_file)
    if not success:
        return driver

    time.sleep(1)

    frame = driver.find_element(
        By.XPATH, '/html/body/div[1]/div[1]/div/div/div/div/div/iframe[2]')
    driver.switch_to.frame(frame)

    driver = save_excel(index, df, file_name, backup_file,
                        'yes', 'yes', driver, 'no', role)

    return driver


@wrap_a_wrapper
@wrap_it_with_params(50, 1000000000, False, False, False, False)
def scrape_iris(path=None, df=None,
                return_df=False, del_prev_files=True,
                headless=True, file_name='file.xlsx', backup_file='file.xlsx',
                time_out=130, role='manager'):
    # print(df[1]['user'].head(1).item())
    # try:

    if df is None:
        data_file_path = r'C:\Users\alkav\Documents\file.xlsx'
        users_file_path = r'C:\Users\alkav\Documents\users.xlsx'
        df_data = pd.read_excel(
            data_file_path)
        df_users = pd.read_excel(users_file_path)

        df = [df_data, df_users]

        if (df[0].loc[df[0]['is_done'].isna()].empty):
            return

    if del_prev_files:
        remove_excel_files(file_path=path,
                           postfix=['.xls', '.html', 'xlsx'])

    path = path

    driver = init_driver(pathsave=path,
                         driver_type='firefox',
                         headless=headless)
    driver = login_iris(driver, creds={'username': df[1][role].head(1).item(),
                                       'pass': df[1]['pass'].head(1).item()})
    driver = find_obj_and_click(driver)

    time.sleep(3)

    if role == 'manager':
        df_final = df[0].loc[df[0]['is_done'].isna()]
    else:
        df_final = df[0].loc[df[0]['success'] == 'yes']

    for index, item in tqdm(df_final.iterrows()):
        stop_threads = False

        t = CustomThread(target=scrape_iris_helper, args=(
            lambda: stop_threads, index, item, driver, df, file_name, backup_file, role))

        t.start()
        res = t.join(time_out)

        if isinstance(res, tuple):
            cleanup(driver)
            driver = save_excel(index, df, file_name, backup_file,
                                'yes', 'no', driver=driver)
            t.kill()
            res = t.join()
            driver = login_iris(driver, creds={'username': df[1][role].head(1).item(),
                                               'pass': df[1]['pass'].head(1).item()})
            driver = find_obj_and_click(driver)

        if driver is None:
            return driver

    return driver
