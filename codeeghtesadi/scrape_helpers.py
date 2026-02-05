# ======================
# Standard library
# ======================
import os
import re
import time
import glob
import json
import math
import pickle
import logging
import threading
import urllib.parse
from functools import wraps
from urllib.parse import urlparse, parse_qs, unquote

# ======================
# Third-party libraries
# ======================
import numpy as np
import pandas as pd
from tqdm import tqdm
from bs4 import BeautifulSoup

# ======================
# Selenium
# ======================
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support.select import Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException,
    StaleElementReferenceException,
    WebDriverException,
)

# ======================
# Internal imports – automation
# ======================
from automation.design_patterns.strategy_pattern.check_health import (
    CheckHealth,
    CheckHealthOne,
    CheckHealthTWO,
    CheckHealthThree,
)

from automation.helpers import (
    add_days_to_persian_date,
    check_update,
    connect_to_sql,
    df_to_excelsheet,
    drop_into_db,
    get_update_date,
    init_driver,
    insert_into_tbl,
    input_info,
    is_updated_to_download,
    is_updated_to_save,
    leading_zero,
    list_files,
    log_it,
    log_the_func,
    maybe_make_dir,
    merge_multiple_excel_files,
    merge_multiple_excel_sheets,
    merge_multiple_html_files,
    move_files,
    open_and_save_excel_files,
    remove_excel_files,
    rename_files,
    return_start_end,
    time_it,
    wait_for_download_to_finish,
    wrap_a_wrapper,
    wrap_it_with_params,
    wrap_it_with_paramsv1,
    extract_nums,
)

from automation.download_helpers import download_1000_parvandeh
from automation.selectors import BASE_URL, XPATHS
from automation.watchdog_186 import watch_over, is_downloaded
from automation.sql_queries import get_sql_arzeshAfzoodeSonatiV2
from automation.logger import log_it

from automation.constants import (
    Modi,
    Modi_pazerande,
    get_dict_years,
    get_sql_con,
    get_soratmoamelat_mapping,
    get_soratmoamelat_mapping_address,
    lst_years_arzeshafzoodeSonati,
)

# ======================
# ⚠ Avoid wildcard imports if possible
# ======================
# from automation.soratmoamelat_helpers import *

RETRY_EXCEPTIONS = (
    TimeoutException,
    StaleElementReferenceException,
    WebDriverException,
)


def retry_selenium(
    retries=3,
    delay=2,
    refresh_on_fail=True,
):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            driver = args[0]  # convention: driver is first arg

            for attempt in range(1, retries + 1):
                try:
                    return func(*args, **kwargs)

                except RETRY_EXCEPTIONS as e:
                    logging.warning(
                        f"[{func.__name__}] attempt {attempt}/{retries} failed: {e}"
                    )

                    if attempt == retries:
                        logging.error(
                            f"[{func.__name__}] giving up after {retries} attempts"
                        )
                        raise

                    if refresh_on_fail:
                        try:
                            driver.refresh()
                        except Exception:
                            pass

                    time.sleep(delay * attempt)

        return wrapper
    return decorator


@log_it
def log(row, success):
    print('log is called')


@log_the_func('none', soratmoamelat=True)
@check_update
@time_it(log=True, db={'db_name': 'testdbV2', 'tbl_name': 'tblLog', 'append_to_prev': True})
def soratmoamelat_helper(driver=None, info={}, path=None, table_name=None, report_type=None,
                         selected_option_text=None, index=1,
                         accepted_number=10000, *args, **kwargs):

    selected_option_text = get_soratmoamelat_mapping(
    )[selected_option_text.split('\\')[-1]]

    remove_excel_files(file_path=os.path.join(
        path, selected_option_text), postfix=['xlsx', 'zip'])
    try:
        if report_type == 'gomrok':
            count_loc = '/html/body/form/table/tbody/tr[1]/td[2]/div[2]/div/div/div/div/div/div/div[1]/div/div/div[1]/table/tbody/tr/td[4]'
            btn_search = 'CPC_Remained_Str_Rem_btnCu'

        elif report_type == 'sayer':
            count_loc = '/html/body/form/table/tbody/tr[1]/td[2]/div[2]/div/div/div[2]/div/div/div[1]/div/div/div[1]/table/tbody/tr/td[4]'
            btn_search = 'CPC_Remained_Str_Rem_btnExt'

        else:
            count_loc = '/html/body/form/table/tbody/tr[1]/td[2]/div[2]/div/div/div/div/div/div[1]/div/div/div[1]/table/tbody/tr/td[4]'
            btn_search = 'CPC_Remained_Str_Rem_btnTTMS'

        WebDriverWait(driver, 8).until(
            EC.presence_of_element_located(
                (By.ID, btn_search))).click()

        WebDriverWait(driver, 25).until(
            EC.presence_of_element_located((
                By.XPATH,
                count_loc
            )))
        count = driver.find_element(
            By.XPATH,
            count_loc
        ).text
        count = count.replace(',', '')
        count = int(extract_nums(count)[0])

    except Exception as e:

        try:
            count = driver.find_element(
                By.XPATH,
                '/html/body/form/table/tbody/tr[1]/td[2]/div[2]/div/div/div/div/div/div[1]/div/div/div[1]/table/tbody/tr/td[4]/span/b'
            ).text
            count = int(extract_nums(count)[0])
        except Exception as e:
            try:
                count = driver.find_element(
                    By.XPATH,
                    count_loc
                ).text
                count = int(extract_nums(count)[0])
            except Exception as e:
                count = 0
                driver = back_it(driver)
                return

    if count < accepted_number:
        driver = if_less_then_down(
            driver, selected_option_text, path, report_type, index=index, field=kwargs['field'])
        driver = back_it(driver)

    else:

        price_ranges = return_start_end(0, 1000000000000000, 1000000)

        for price in price_ranges:
            start_p = price[0]
            end_p = price[1]
            driver, info = recur_down(driver=driver, info=info,
                                      start_p=start_p, end_p=end_p, index=index)
            if info['count'] == 0:
                back_it()
                break
            elif info['count'] > accepted_number:
                while count > accepted_number:
                    start_p, down_count, driver = recur_until_less(
                        start_p, end_p, count, selected_option_text, index, accepted_number, driver, path, field=kwargs['field'])
                    info['count'] -= down_count

                _, _, driver = recur_until_less(start_p, end_p, count,
                                                selected_option_text, index, accepted_number, driver, path, field=kwargs['field'])

            elif (info['count'] != 0 and info['count'] < accepted_number):
                driver, info = if_less_then_down(
                    driver=driver, info=info, name=selected_option_text, path=path,
                    report_type=report_type, index=index, field=kwargs['field'])


# @time_it(log=True, db={'db_name': 'testdbV2', 'tbl_name': 'tblLog', 'append_to_prev': True})
def download_excel(func, path=None, report_type=None, type_of_excel=None,
                   no_files_in_path=None, excel_file=None, table_name=None, year=None,
                   type_of=None, file_postfixes=['html', 'xlsx']):
    # i = 0

    # while len(glob.glob1(path, '%s' % excel_file)) == no_files_in_path:
    #     if i % 60 == 0:
    #         print('waiting %s seconds for the file to be downloaded' % i)
    #     i += 1
    #     time.sleep(1)
    while True:
        try:
            t1 = threading.Thread(target=func)
            t2 = threading.Thread(
                target=watch_over, args=(path, 2200, 2))
            t1.start()
            t2.start()
            t1.join()
            t2.join()
            res = wait_for_download_to_finish(path, file_postfixes)
            if res == 0:
                raise Exception
            print('****************%s done*******************************' %
                  type_of_excel)
            break
        except:
            remove_excel_files(file_path=path, postfix=[
                               '.xlsx', '.part', '.xls', '.html'])
            continue

    # for prefix in file_postfixes:
    #     file_list = glob.glob(path + "/*" + prefix)

    # for item in file_list:
    #     os.rename(item, os.path.join(item.rsplit('\\', 1)[0],
    #                                  item.rsplit('\\', 1)[1].split('.')[0]+report_type+year+))
    # # return path + '\\' + excel_file


def adam(driver, df, check_last_date_modified=True):

    WebDriverWait(driver, 32).until(
        EC.presence_of_element_located((
            By.XPATH, '//a[.//div[text()="management.tax.gov.ir"]]'
        ))).click()

    time.sleep(1)

    for index, row in tqdm(df.iterrows()):
        try:

            WebDriverWait(driver, 32).until(
                EC.presence_of_element_located(
                    (By.ID,
                     "TextboxPublicSearch"))).clear()

            WebDriverWait(driver, 32).until(
                EC.presence_of_element_located(
                    (By.ID,
                     "TextboxPublicSearch"))).send_keys(str(row["کد رهگیری"]))
            WebDriverWait(driver, 32).until(
                EC.presence_of_element_located(
                    (By.ID,
                     "publicSearchLink"))).click()

            if WebDriverWait(driver, 1).until(
                EC.presence_of_element_located(
                    (By.XPATH,
                     "/html/body/form/table/tbody/tr[2]/td[2]/span/table/tbody/tr[2]/td[7]/span"))).text == "غيرفعال":

                sql_query = f"""UPDATE tbladam SET message='success' where [کد رهگیری] = '{str(row['کد رهگیری'])}'"""
                connect_to_sql(sql_query, read_from_sql=False, return_df=False,
                               sql_con=get_sql_con(database='testdbV2'))
                continue

            time.sleep(1)
            try:
                if WebDriverWait(driver, 32).until(
                    EC.presence_of_element_located(
                        (By.XPATH,
                         "//span[contains(text(),'نمایش شناسنامه')]"))):
                    driver.find_element(
                        By.XPATH,
                        "//span[contains(text(),'نمایش شناسنامه')]").click()
            except:
                continue

            if check_last_date_modified:
                try:
                    WebDriverWait(driver, 32).until(
                        EC.presence_of_element_located(
                            (By.XPATH,
                             "/html/body/form/table/tbody/tr[2]/td[2]/span[3]/table[1]/tbody/tr/td[2]/a/div"))).click()
                    table = driver.find_element(
                        By.ID, 'idt1')

                    tr = table.find_elements(By.TAG_NAME, 'tr')[-1]

                    condition = tr.find_elements(By.TAG_NAME, 'td')[1].text
                    last_date_mod = tr.find_elements(
                        By.TAG_NAME, 'td')[2].text

                    sql_query = f"""UPDATE tbladam SET last_date_modified='{last_date_mod}' 
                                     WHERE [کد رهگیری] = '{str(row['کد رهگیری'])}'"""
                    connect_to_sql(sql_query, read_from_sql=False, return_df=False,
                                   sql_con=get_sql_con(database='testdbV2'))
                    continue
                except Exception as e:
                    print(e)
                    continue

            try:

                def check_for_errors(func):
                    def wrapper(driver, *args, **kwargs):
                        if "error" in driver.current_url.lower():
                            raise Exception(
                                "Error detected in URL, operation aborted.")
                        return func(driver, *args, **kwargs)
                    return wrapper

                @check_for_errors
                def click_element(driver, xpath, timeout=32):
                    WebDriverWait(driver, timeout).until(
                        EC.element_to_be_clickable((By.XPATH, xpath))
                    ).click()

                @check_for_errors
                def check_accessibility_and_click(driver):
                    click_element(
                        driver, "/html/body/form/table/tbody/tr[2]/td[2]/span[3]/table[1]/tbody/tr/td[7]/a/div")
                    try:
                        time.sleep(0.001)
                        restricted_message = driver.find_element(
                            By.XPATH, "/html/body/form/table/tbody/tr[2]/td[2]/span[3]/table[2]/tbody/tr/td/div/div[7]/div").text
                        if restricted_message == 'این بخش برای شما غیر قابل دسترس می باشد. برای نمایش و دسترسی به این اطلاعات مودی باید در حوزه اداره کل شما باشد.':
                            return False
                    except:
                        pass
                    return True

                @check_for_errors
                def wait_and_send_keys(driver, xpath, keys, timeout=32):
                    WebDriverWait(driver, timeout).until(
                        EC.element_to_be_clickable((By.XPATH, xpath))
                    ).send_keys(keys)

                @check_for_errors
                def find_and_click(driver, by, identifier):
                    driver.find_element(by, identifier).click()

                @check_for_errors
                def find_and_send_keys(driver, xpath, keys):
                    driver.find_element(By.XPATH, xpath).send_keys(keys)

                @check_for_errors
                def process_form(driver):
                    belongs_edare = check_accessibility_and_click(driver)

                    if belongs_edare:

                        click_element(
                            driver, "/html/body/form/table/tbody/tr[2]/td[2]/span[3]/table[2]/tbody/tr/td/div/div[7]/table[1]/tbody/tr[1]/td[7]/a/div")
                        wait_and_send_keys(
                            driver, '//*[@id="CPC_TextboxDisableTaxpayerDateFa"]', "1403/02/10")
                        click_element(
                            driver, "/html/body/form/table/tbody/tr[2]/td[2]")
                        find_and_click(
                            driver, By.ID, "CPC_DropDownCommentType_chosen")
                        find_and_send_keys(
                            driver, '/html/body/form/table/tbody/tr[2]/td[2]/div/table/tbody/tr[2]/td[2]/div/div/div/input', "سایر موارد")
                        find_and_send_keys(
                            driver, '/html/body/form/table/tbody/tr[2]/td[2]/div/table/tbody/tr[2]/td[2]/div/div/div/input', Keys.RETURN)
                        find_and_send_keys(driver, '/html/body/form/table/tbody/tr[2]/td[2]/div/table/tbody/tr[3]/td[2]/input',
                                           "با توجه به اینکه زمان زیادی از تکمیل نشدن پرونده می گذرد با درخواست اداره مربوطه غیرفعال گردید")
                        find_and_click(driver, By.XPATH,
                                       '//*[@id="CPC_CheckBoxDisableTaxpayer"]')
                        find_and_click(driver, By.XPATH,
                                       '//*[@id="CPC_ButtonDisableTaxpayer"]')
                    return belongs_edare

                belongs_edare = process_form(driver)

                if not belongs_edare:

                    sql_query = f"""UPDATE tbladam SET message='success' where [کد رهگیری] = '{str(row['کد رهگیری'])}'"""
                    connect_to_sql(sql_query, read_from_sql=False, return_df=False,
                                   sql_con=get_sql_con(database='testdbV2'))
                    continue

            except Exception as e:
                print(e)
                driver.get('https://management.tax.gov.ir/Public/Search')
                continue

            try:

                message = WebDriverWait(driver, 64).until(
                    EC.presence_of_element_located(
                        (By.XPATH,
                         '/html/body/form/table/tbody/tr[2]/td[2]/span[3]\
                        /table[2]/tbody/tr/td/div/div[1]/table[1]/tbody/tr[1]/td[1]/div'))).text
                confirm_message = "این مودی غیرفعال می باشد. دلایل و تاریخ غیر فعالسازی در برگه وضعیت ها قابل نمایش می باشد. جهت فعالسازی از برگه ابزارها می توان استفاده نمود."

                if message == confirm_message:

                    sql_query = f"""UPDATE tbladam SET message='success' where [کد رهگیری] = '{str(row['کد رهگیری'])}'"""
                    connect_to_sql(sql_query, read_from_sql=False, return_df=False,
                                   sql_con=get_sql_con(database='testdbV2'))
            except:

                pass

        except Exception as e:

            print(e)
            time.sleep(1)
            driver.get('https://management.tax.gov.ir/Public/Search')
            continue

        time.sleep(0.5)


def set_imp_corps(driver, df, save_path):
    # df_6.status = ""
    driver.find_element(
        By.XPATH,
        "/html/body/div/div[3]/div[2]/div/div/div[1]/div/div[2]/div/a/h6"
    ).click()
    time.sleep(0.5)

    # for index, row in df.loc[df['is_set'].isna()].iterrows():
    for index, row in df.loc[df['is_set'] == 'no'].iterrows():
        print(index)
        try:

            WebDriverWait(driver, 4).until(
                EC.presence_of_element_located((
                    By.ID,
                    "TextboxPublicSearch"
                ))).clear()

            WebDriverWait(driver, 4).until(
                EC.presence_of_element_located((
                    By.ID,
                    "TextboxPublicSearch"
                ))).send_keys(str(row['کدرهگيري ثبت نام']))

            WebDriverWait(driver, 3).until(
                EC.presence_of_element_located((
                    By.ID,
                    "publicSearchLink"
                ))).click()

            time.sleep(0.5)

            WebDriverWait(driver, 4).until(
                EC.presence_of_element_located((
                    By.XPATH,
                    "/html/body/form/table/tbody/tr[2]/td[2]/span/table/tbody/tr[2]/td[8]/a[1]/span"
                ))).click()

            try:

                title = WebDriverWait(driver, 2).until(
                    EC.presence_of_element_located((
                        By.XPATH,
                        "/html/body/form/table/tbody/tr[2]/td[2]/span[3]/table[2]/tbody/tr/td/div/div[1]/table[1]/tbody/tr[1]/td[2]/table[1]/tbody/tr[1]/td[4]/span"
                    ))).text

                if title == "پرونده مهم":
                    df.loc[index, 'is_set'] = 'yes'
                    remove_excel_files(files=[save_path], postfix=['xlsx'])
                    df.to_excel(save_path, index=False)
                continue

            except:

                WebDriverWait(driver, 3).until(
                    EC.presence_of_element_located((
                        By.XPATH,
                        "/html/body/form/table/tbody/tr[2]/td[2]/span[3]/table[1]/tbody/tr/td[7]/a/div"
                    ))).click()

                WebDriverWait(driver, 3).until(
                    EC.presence_of_element_located((
                        By.XPATH,
                        "/html/body/form/table/tbody/tr[2]/td[2]/span[3]/table[2]/tbody/tr/td/div/div[7]/table[1]/tbody/tr[2]/td/a/div"
                    ))).click()

                time.sleep(0.4)

                alert = driver.switch_to.alert
                alert.accept()
                time.sleep(3)

                try:

                    title = WebDriverWait(driver, 8).until(
                        EC.presence_of_element_located((
                            By.XPATH,
                            "/html/body/form/table/tbody/tr[2]/td[2]/span[3]/table[2]/tbody/tr/td/div/div[1]/table[1]/tbody/tr[1]/td[2]/table[1]/tbody/tr[1]/td[4]/span"
                        ))).text

                    if title == "پرونده مهم":
                        df.loc[index, 'is_set'] = 'yes'

                    remove_excel_files(files=[save_path], postfix=['xlsx'])

                    df.to_excel(save_path, index=False)

                except Exception as e:
                    print(e)
                    df.loc[index, 'is_set'] = 'no'

                    remove_excel_files(
                        files=[save_path], postfix=['xlsx'])
                    df.to_excel(save_path, index=False)

                print('f')

        except Exception as e:
            df.loc[index, 'is_set'] = 'no'
            remove_excel_files(
                files=[save_path], postfix=['xlsx'])
            df.to_excel(save_path, index=False)
            continue

    remove_excel_files(
        files=[save_path], postfix=['xlsx'])

    df.to_excel(save_path, index=False)

    driver.close()


def set_user_permissions(driver, df):
    from urllib.parse import urlparse
    # df_6.status = ""
    driver.get(
        'https://management.tax.gov.ir/UserManagmentSystem/userSetting/manageusers')
    # # --- Find all <a> elements with class 'loader' ---
    # from urllib.parse import urlparse
    links = driver.find_elements(By.CSS_SELECTOR, "a.loader")

    pattern = re.compile(
        r"^/UserManagmentSystem/UserSetting/setpermission/\d+$")

    matching_hrefs = []
    for link in links:
        href = link.get_attribute("href")
        if href:
            path = urlparse(href).path
            if pattern.search(path):
                matching_hrefs.append(href)

    df = pd.DataFrame(matching_hrefs, columns=["href"])

    # # add the two extra columns with default None values
    df["done"] = None
    df["isvalid"] = None

    # # remove duplicate hrefs while keeping the first occurrence
    df = df.drop_duplicates(
        subset=["href"], keep="first").reset_index(drop=True)

    drop_into_db('tblMojavezha',
                 df.columns.tolist(),
                 df.values.tolist(),
                 append_to_prev=False)

    # df['melli_code'] = df['melli'].apply(lambda x: leading_zero(x))

    time.sleep(0.5)
    driver.find_element(
        By.XPATH,
        "/html/body/form/table/tbody/tr[2]/td[1]/div[16]"
    ).click()

    driver.find_element(
        By.XPATH,
        "/html/body/form/table/tbody/tr[2]/td[1]/div[17]/a[2]/div"
    ).click()

    time.sleep(1)
    for index, row in df.iterrows():
        isvalid = 'False'
        done = 'False'
        url = row["href"]

        try:
            print(f"Opening {url} ...")
            driver.get(url)

            html = driver.page_source
            soup = BeautifulSoup(html, "html.parser")

            pattern = re.compile(r"\[\d+\]")

            # Find all nested tables inside table.tree
            nested_tables = []
            for table in soup.select("table.tree table"):
                nested_tables.append(table)

            # Collect <tr> elements from those nested tables that match the pattern
            trs = []
            for table in nested_tables:
                trs.extend([tr for tr in table.find_all("tr")
                           if pattern.search(tr.get_text())])

            # Divide them based on whether they have the <img src="/images/true.png">
            trs_with_true = []
            trs_without_true = []

            for tr in trs:
                img = tr.find("img", {"src": "/images/true.png"})
                if img:
                    trs_with_true.append(tr)
                else:
                    trs_without_true.append(tr)

           # Define the codes to look for
            target_codes = ["[101101]",]

            pattern_target = re.compile(
                "|".join(re.escape(code) for code in target_codes))

            # Collect matching <tr>s
            matching_trs = [
                tr for tr in trs_with_true if pattern_target.search(tr.get_text())]

            # Boolean flag if any found
            has_target_permission = len(matching_trs) > 0

            if len(matching_trs) > 0:
                isvalid = 'True'

            print("✅ Found target permission:", has_target_permission)
            print(f"🧩 Total matching rows: {len(matching_trs)}")

            if has_target_permission:
                try:
                    # Wait for the target <span> to appear in the DOM
                    target_span = WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located(
                            (By.XPATH,
                             "//span[contains(@rel, '/UserManagmentSystem/UserSetting/jSetPermissionSpecificItem/') "
                             "and contains(@rel, '/101101')]")
                        )
                    )

                    # Find the <img> inside the span
                    img_element = target_span.find_element(By.TAG_NAME, "img")
                    img_src = img_element.get_attribute("src")

                    # Check if it's unknown
                    if "/images/true.png" in img_src:
                        print(
                            "🔸 [101101] currently unknown → clicking to enable...")
                        driver.execute_script(
                            "arguments[0].scrollIntoView({block: 'center'});", target_span)
                        driver.execute_script(
                            "arguments[0].click();", target_span)

                        # Wait until the image turns into true.png (confirm successful change)
                        WebDriverWait(driver, 10).until(
                            lambda d: img_element.get_attribute(
                                "src").endswith("/images/unknown.png")
                        )
                        print("✅ [100200] permission enabled successfully.")
                    else:
                        print("✅ [100200] already enabled — no click needed.")
                        done = '[100200] already enabled — no click needed.'

                except Exception as e:
                    print("❌ Could not click on [100200]:", e)
                    done = 'Failure'

                finally:
                    connect_to_sql(
                        sql_query=f"UPDATE tblMojavezha SET done = '{done}', isvalid='{isvalid}' WHERE href = '{url}'",
                        sql_con=get_sql_con(database='TestDb'),
                        read_from_sql=False, return_df=False)

            else:
                print("ℹ️ has_target_permission is False — no click performed.")
                done = 'has_target_permission is False — no click performed.'

            time.sleep(1)  # optional: wait a bit for page to load

            # You can perform checks here
            # Example: mark as visited
            df.at[index, "done"] = True
            df.at[index, "isvalid"] = True

        except Exception as e:
            print(f"Error loading {url}: {e}")
            df.at[index, "done"] = False
            df.at[index, "isvalid"] = False

        print(index)
        try:
            driver.find_element(
                By.ID,
                "CPC_TextboxAdminUsernameNationalID"
            ).clear()

            driver.find_element(
                By.ID,
                "CPC_TextboxAdminUsernameNationalID"
            ).send_keys(str(row["melli_code"]))

            driver.find_element(
                By.ID,
                'CPC_ButtonSearchuser').click()

            time.sleep(5)

            driver.find_element(
                By.XPATH,
                "/html/body/form/table/tbody/tr[2]/td[2]/div/div/table/tbody/tr[2]/td/span/table/tbody/tr[3]/td[3]/a"
            ).click()

            driver.find_element(
                By.XPATH,
                "/html/body/form/table/tbody/tr[2]/td[2]/span[2]/table[1]/tbody/tr/td[2]/a/div"
            ).click()

            lst_mojavez = driver.find_element(
                By.XPATH,
                "/html/body/form/table/tbody/tr[2]/td[2]/span[2]/table[2]/tbody/tr/td/div/div[2]/table/tbody/tr/td[1]/table/tbody"
            )

            lst_all = lst_mojavez.find_elements(
                By.TAG_NAME,
                'td'
            )

            made77 = False
            # tagheer_parvande_pezeshk = False
            for item in lst_all:
                if item.text == '149':
                    made77 = True
                # if item.text == '6346':
                    # tagheer_parvande_pezeshk = True

            if made77:
                print('already granted')
                driver.find_element(
                    By.XPATH,
                    "/html/body/form/table/tbody/tr[2]/td[2]/span[2]/a/div"
                ).click()
                time.sleep(1)
                continue

            driver.find_element(
                By.XPATH,
                "/html/body/form/table/tbody/tr[2]/td[2]/span[2]/table[2]/tbody/tr/td/div/div[2]/a[1]/div"
            ).click()

            time.sleep(7)

            try:
                if not made77:
                    driver.find_element(
                        By.XPATH,
                        "/html/body/form/table/tbody/tr[2]/td[2]/div/table/tbody/tr[2]/td[1]/table[2]\
                            /tbody/tr[1]/td[2]/table/tbody/tr[26]/td[1]/span/img"
                    ).click()

                    time.sleep(3)
            except Exception as e:
                print(e)
                print('not found')
                continue

            driver.find_element(
                By.XPATH,
                "/html/body/form/table/tbody/tr[2]/td[2]/div/table/tbody/tr[1]/td/div/a/div"
            ).click()

            WebDriverWait(driver, 28).until(
                EC.presence_of_element_located((
                    By.XPATH,
                    "/html/body/form/table/tbody/tr[2]/td[2]/span[2]/a/div"
                )))
            driver.find_element(
                By.XPATH,
                "/html/body/form/table/tbody/tr[2]/td[2]/span[2]/a/div").click()

        except Exception as e:
            print(f'Error for code melli {str(row["melli_code"])}')
            print(e)
            continue

    driver.close()


def set_hoze(driver, df):
    # df_6.status = ""
    driver.find_element(
        By.XPATH,
        "/html/body/div/div[3]/div[2]/div/div/div[1]/div/div[2]/div/a/h6"
    ).click()
    time.sleep(1)
    for index, val in df.iterrows():
        is_active = 'no'
        done = 'Success'
        try:
            try:
                driver.find_element(
                    By.XPATH,
                    "/html/body/form/table/tbody/tr[1]/td[2]/div/div/table/tbody/tr[2]/td[1]/input"
                ).send_keys(str(val["rahgiri_code"]))

            except:
                print('...')

            WebDriverWait(driver, 28).until(
                EC.presence_of_element_located((
                    By.XPATH,
                    "/html/body/form/table/tbody/tr[1]/td[2]/div/div/table/tbody/tr[2]/td[2]/a/span"
                )))
            driver.find_element(
                By.XPATH,
                "/html/body/form/table/tbody/tr[1]/td[2]/div/div/table/tbody/tr[2]/td[2]/a/span"
            ).click()

            if driver.find_element(
                    By.XPATH,
                    "/html/body/form/table/tbody/tr[2]/td[2]/span/table/tbody/tr[2]/td[7]/span"
            ).text == "غيرفعال":

                is_Active = 'yes'

            time.sleep(1)

            try:
                tables = driver.find_elements(
                    By.CSS_SELECTOR, "table[id^='Grid_']")

                # The value you're looking for in the "کدرهگیری" column
                target_value = str(val["rahgiri_code"])

                found = False  # To track if the value is found
                office_name = None

                for table in tables:
                    headers = table.find_elements(By.TAG_NAME, "th")

                    # Check if the table has the "کدرهگیری" column
                    code_column_index = None
                    office_column_index = None
                    for index, header in enumerate(headers):
                        if header.text == "کدرهگیری":
                            code_column_index = index
                        if header.text == "اداره مالیاتی":
                            office_column_index = index

                    # Continue only if both "کدرهگیری" and "اداره مالیاتی" columns are found
                    if code_column_index is not None and office_column_index is not None:
                        # Get all rows in the table (excluding the header)
                        rows = table.find_elements(By.TAG_NAME, "tr")[1:]

                        for row in rows:
                            columns = row.find_elements(By.TAG_NAME, "td")
                            if columns[code_column_index].text == target_value:
                                # Found the row with the target value in "کدرهگیری"
                                office_name = columns[office_column_index].text
                                found = True
                                try:
                                    # Find and click the 'نمایش شناسنامه' span in the same row
                                    show_profile_button = row.find_element(
                                        By.XPATH, ".//span[contains(text(),'نمایش شناسنامه')]")
                                    show_profile_button.click()
                                    break
                                except NoSuchElementException:
                                    print(
                                        "Could not find 'نمایش شناسنامه' button in the row.")
                                    break

                    if found:
                        break

            except:
                done = city
                raise Exception

            WebDriverWait(driver, 28).until(
                EC.presence_of_element_located((
                    By.XPATH,
                    "/html/body/form/table/tbody/tr[2]/td[2]/span[3]/table[1]/tbody/tr/td[5]/a/div"
                ))).click()

            WebDriverWait(driver, 28).until(
                EC.presence_of_element_located((
                    By.XPATH, "//a//div[text()='تعیین اداره مالیاتی عملکرد و ارزش افزوده']"
                ))).click()

            WebDriverWait(driver, 28).until(
                EC.presence_of_element_located((
                    By.ID, "CPC_DDLAddSanimOfficeID_chosen"
                ))).click()

            edare_elm = WebDriverWait(driver, 28).until(
                EC.presence_of_element_located((
                    By.XPATH, "/html/body/form/table/tbody/tr[2]/td[2]/div/table/tbody/tr[4]/td[2]/div/div/div/input"
                )))

            edare_elm.send_keys(val['edare'][:4])

            edare_elm.send_keys(Keys.RETURN)

            WebDriverWait(driver, 28).until(
                EC.presence_of_element_located((
                    By.ID, "CPC_TextboxAddSanimOfficeIDUnitID"
                ))).clear()

            WebDriverWait(driver, 28).until(
                EC.presence_of_element_located((
                    By.ID, "CPC_TextboxAddSanimOfficeIDUnitID"
                ))).send_keys(val['edare'])

            WebDriverWait(driver, 28).until(
                EC.presence_of_element_located((
                    By.ID, "CPC_TextboxAddSanimOfficeIDClass"
                ))).clear()

            WebDriverWait(driver, 28).until(
                EC.presence_of_element_located((
                    By.ID, "CPC_TextboxAddSanimOfficeIDClass"
                ))).send_keys('1')

            WebDriverWait(driver, 28).until(
                EC.presence_of_element_located((
                    By.ID, "CPC_ButtonAddSanimOfficeID"
                ))).click()

        except Exception as e:
            if re.search(r'error', driver.current_url, re.IGNORECASE):
                driver.get(
                    'https://management.tax.gov.ir/Public/Process/Homepage')
            print(e)
            done = 'Failed'

        finally:
            connect_to_sql(
                sql_query=f"UPDATE tblSetHoze SET done = '{done}' WHERE rahgiri_code = '{val['rahgiri_code']}'",
                sql_con=get_sql_con(database='TestDb'),
                read_from_sql=False, return_df=False)

        time.sleep(1)

    driver.close()


def select_all(driver):

    for i in range(2):

        WebDriverWait(driver, 1).until(
            EC.presence_of_element_located((
                By.XPATH,
                "/html/body/kendo-popup/div/div/div[2]/i"
            ))).click()


def wait_and_click(driver, xpath, timeout=60, click=True):
    """Wait for an element and click it if required."""
    try:
        element = WebDriverWait(driver, timeout).until(
            EC.presence_of_element_located((By.XPATH, xpath))
        )
        if click:
            element.click()
        return element
    except Exception as e:
        print(f"Error waiting and clicking: {e}")
        return None


def select_all(driver, num_clicks=2):
    """Helper function to select all items in a dropdown or similar element."""
    for i in range(num_clicks):
        wait_and_click(driver, "//*[contains(text(), 'انتخاب همه')]")
    # Additional logic if needed


def handle_loading(driver):
    """Handle potential loading and wait for the page to load."""
    try:
        WebDriverWait(driver, 1).until(
            EC.presence_of_element_located((By.ID, "loading-content"))
        )
    except:
        pass  # Handle when loading is not present


def download_output(driver, path):
    """Handles the downloading of output."""
    wait_and_click(driver, "//*[contains(text(), ' خروجی ')]")
    try:
        t1 = threading.Thread(target=down, args=(driver, path))
        t2 = threading.Thread(target=watch_over, args=(path, 240, 2))
        t1.start()
        t2.start()
        t1.join()
        t2.join()
        wait_for_download_to_finish(path, ['xlsx'], sleep_time=15)
    except Exception as e:
        print(f"Error in downloading output: {e}")


# Function to construct dynamic URL
def construct_url(base_url, default_parameters, year, stage):
    # Update the parameters for the current year and stage
    updated_parameters = default_parameters.copy()

    # Update year and stage in the parameters
    updated_parameters["22ebbae9-c5f7-ab05-c09a-1136c4aadf23"] = [
        {"Value": [{"Id": str(year), "Value": str(year)}]}]
    updated_parameters["e2b4868a-9a38-a812-83ad-3dafb36ae8c4"] = [
        {"Value": [{"Id": stage, "Value": stage}]}]

    # Convert the parameters to a URL-encoded string
    param_str = urllib.parse.urlencode(updated_parameters, doseq=True)

    # Construct the full URL with the updated parameters
    full_url = f"{base_url}&{param_str}"
    return full_url

    # Perform string replacement separately before using f-string
    param_values_str = str(parameters).replace('\'', '\"')

    # Now use the result in the f-string
    return f"{base_url}&paramvalues={param_values_str}"

# Function to interact with dropdowns


def select_from_drpdown(driver, xpath, num_clicks=1):
    wait_and_click(driver, xpath)
    select_all(driver, num_clicks=num_clicks)

# Function to handle clicks with retries


def click_with_retry(driver, xpath):
    try:
        wait_and_click(driver, xpath)
    except Exception as e:
        print(f"Error: {e}. Retrying by reloading the page...")
        driver.get(driver.current_url)  # Reload the page
        time.sleep(3)  # Wait a bit for the page to reload
        wait_and_click(driver, xpath)


@retry_selenium(retries=3)
def open_dashboard(driver):
    driver.get(BASE_URL)
    WebDriverWait(driver, 30).until(
        EC.presence_of_element_located((By.XPATH, XPATHS["residegi"]))
    )


@retry_selenium(retries=3)
def navigate_to_declaration_dashboard(driver):
    wait_and_click(driver, XPATHS["residegi"])
    wait_and_click(driver, XPATHS["cycle_dashboard"])
    wait_and_click(driver, XPATHS["by_province"])


def force_same_tab_navigation(driver):
    driver.execute_script("""
        window.open = function(url) {
            window.location.href = url;
        };
    """)


def scroll_and_click(driver, xpath, timeout=15):
    element = WebDriverWait(driver, timeout).until(
        EC.element_to_be_clickable((By.XPATH, xpath))
    )
    driver.execute_script(
        "arguments[0].scrollIntoView({block:'center'});", element
    )
    element.click()


def select_khozestan_province(driver):
    scroll_and_click(driver, XPATHS["khozestan"])
    wait_and_click(driver, XPATHS["by_office"])


@retry_selenium(retries=2, refresh_on_fail=False)
def handle_window_switch(driver, timeout=10):
    original = driver.current_window_handle

    cell = WebDriverWait(driver, timeout).until(
        EC.element_to_be_clickable((By.XPATH, XPATHS["grid_cell"]))
    )
    cell.click()

    WebDriverWait(driver, timeout).until(
        lambda d: len(d.window_handles) > 1
    )

    new_window = next(
        w for w in driver.window_handles if w != original
    )

    driver.switch_to.window(original)
    driver.close()

    driver.switch_to.window(new_window)


def open_final_details(driver):
    wait_and_click(driver, XPATHS["final_details"])


@retry_selenium(retries=3)
def open_tax_office_details(driver):
    wait_and_click(
        driver,
        XPATHS["by_office"]
    )


def go_to_base_url(driver, path):
    open_dashboard(driver)
    navigate_to_declaration_dashboard(driver)
    force_same_tab_navigation(driver)
    select_khozestan_province(driver)
    open_tax_office_details(driver)
    handle_window_switch(driver)
    open_final_details(driver)

    return driver.current_url


def select_year(driver, year: str, dropdown):
    select_from_drpdown(driver, dropdown, 1)

    wait_and_click(driver, f"//*[contains(text(), '{year}')]")


# =========================
# PARAMETER TRANSFORMATION
# =========================
PID_RULES = {
    # اداره کل → استان خوزستان
    "68836f48-1934-fcb6-9538-84692c861b9b": lambda block: {
        **block,
        "Value": [{
            "Id": "استان خوزستان",
            "Value": "استان خوزستان",
            "DisplayValue": None
        }]
    },

    # دوره → 1 تا 4
    "5c90d3d0-3c49-9b4d-7927-51d3068d063d": lambda block: {
        **block,
        "Value": [
            {"Id": str(i), "Value": str(i), "DisplayValue": None}
            for i in range(1, 5)
        ]
    },

    # سال عملکرد → ALL
    "22ebbae9-c5f7-ab05-c09a-1136c4aadf23": lambda block: {
        **block,
        "Value": []
    },

    # منبع مالیاتی → ALL
    "e2b4868a-9a38-a812-83ad-3dafb36ae8c4": lambda block: {
        **block,
        "Value": []
    },
}


def transform_paramvalues(decoded):
    result = {}

    for pid, blocks in decoded.items():
        original = blocks[0]
        block = original.copy()

        transformer = PID_RULES.get(pid)
        if transformer:
            block = transformer(block)
        else:
            block["Value"] = original["Value"]  # keep original

        result[pid] = [block]

    return result


def build_dashboard_url(current_url, new_paramvalues):
    parsed = urllib.parse.urlparse(current_url)
    query = urllib.parse.parse_qs(parsed.query)

    dashboard_id = query["dashboardid"][0]
    encoded = urllib.parse.quote(
        json.dumps(new_paramvalues, ensure_ascii=False)
    )

    return (
        "https://star.tax.gov.ir/dashboard/index/preview"
        f"?dashboardid={dashboard_id}"
        f"&paramvalues={encoded}"
    )


YEARS = map(str, range(1396, 1405))

BASE_XPATH = (
    "/html/body/app-root/app-ui-shell/div/div/div/div/"
    "app-root/app-dashboard-design/div/app-sidebar-folder-view/"
    "kendo-splitter/kendo-splitter-pane[2]/div/div/div/div/div[2]/div[3]/"
    "app-parameter-value-assignment/div/div[1]"
)

DROPDOWNS = {
    "first": (
        f"{BASE_XPATH}/div[2]/div/div[2]/app-data-type-ui-generator/"
        "app-custom-dropdown/kendo-dropdownlist/span/span/div"
    ),
    "year": "//div[contains(@class,'parameter')]"
            "[.//div[contains(@class,'condition-name') and normalize-space()='سال عملکرد']]"
            "//kendo-dropdownlist",
    "last": (
        f"{BASE_XPATH}/div[6]/div/div[2]/app-data-type-ui-generator/"
        "app-custom-dropdown/kendo-dropdownlist/span/span/div"
    ),
}

STATUS_LABELS = (
    "در انتظار ابلاغ تشخیص",
    "تایید",
)


def ravand_residegi(driver, path=None):

    current_url = go_to_base_url(driver, path)

    final_url = _build_final_url(current_url)

    driver.get(final_url)

    _open_details(driver)

    _fill_filters(driver)


def _build_final_url(current_url):
    parsed = urlparse(current_url)
    query = parse_qs(parsed.query)

    paramvalues = json.loads(unquote(query["paramvalues"][0]))
    new_paramvalues = transform_paramvalues(paramvalues)

    return build_dashboard_url(current_url, new_paramvalues)


def _open_details(driver):
    wait_and_click(
        driver,
        "//*[contains(text(), 'جزئیات قطعی سازی اظهارنامه مودی')]"
    )


def select_all_sal_amalkard_years(driver, timeout=20):
    """
    Opens the 'سال عملکرد' Kendo dropdown and selects
    all year items one by one (excluding 'انتخاب همه').
    """
    wait = WebDriverWait(driver, timeout)

    # ---------------------------------------------------
    # 1. Open 'سال عملکرد' dropdown (data-index = 2)
    # ---------------------------------------------------
    dropdown = wait.until(
        EC.element_to_be_clickable((
            By.XPATH,
            "//div[@data-index='2']//kendo-dropdownlist"
            "//span[contains(@class,'k-input-inner')]"
        ))
    )

    driver.execute_script(
        "arguments[0].scrollIntoView({block:'center'});", dropdown
    )
    driver.execute_script("arguments[0].click();", dropdown)

    time.sleep(1)  # allow dropdown animation/rendering

    # ---------------------------------------------------
    # 2. Select each year one by one
    # ---------------------------------------------------
    while True:
        options = driver.find_elements(
            By.XPATH,
            "//li[@role='option' and .//input[@type='checkbox']"
            " and not(.//text()[contains(.,'انتخاب همه')])]"
        )

        clicked_any = False

        for option in options:
            checkbox = option.find_element(
                By.XPATH, ".//input[@type='checkbox']")

            if not checkbox.is_selected():
                driver.execute_script(
                    "arguments[0].scrollIntoView({block:'center'});", checkbox
                )
                driver.execute_script("arguments[0].click();", checkbox)
                clicked_any = True
                time.sleep(0.3)  # let Angular update state

        if not clicked_any:
            break

    print("✅ All 'سال عملکرد' years selected")


def select_all_manbae_maliyati(driver, timeout=20):
    """
    Opens the 'منبع مالیاتی' Kendo dropdown and selects
    all checkbox items one by one (excluding 'انتخاب همه').
    """
    wait = WebDriverWait(driver, timeout)

    # ---------------------------------------------------
    # 1. Open 'منبع مالیاتی' dropdown (data-index = 3)
    # ---------------------------------------------------
    dropdown = wait.until(
        EC.element_to_be_clickable((
            By.XPATH,
            "//div[@data-index='3']//kendo-dropdownlist"
            "//span[contains(@class,'k-input-inner')]"
        ))
    )

    driver.execute_script(
        "arguments[0].scrollIntoView({block:'center'});", dropdown
    )
    driver.execute_script("arguments[0].click();", dropdown)

    time.sleep(1)  # allow dropdown to render

    # ---------------------------------------------------
    # 2. Select all checkbox items (skip 'انتخاب همه')
    # ---------------------------------------------------
    while True:
        options = driver.find_elements(
            By.XPATH,
            "//li[@role='option' and .//input[@type='checkbox']"
            " and not(.//text()[contains(.,'انتخاب همه')])]"
        )

        clicked_any = False

        for option in options:
            checkbox = option.find_element(
                By.XPATH, ".//input[@type='checkbox']")

            if not checkbox.is_selected():
                driver.execute_script(
                    "arguments[0].scrollIntoView({block:'center'});", checkbox
                )
                driver.execute_script("arguments[0].click();", checkbox)
                clicked_any = True
                time.sleep(0.3)  # Angular stability delay

        if not clicked_any:
            break

    print("✅ All 'منبع مالیاتی' items selected")


def iterate_marhaleh_and_download(driver, timeout=20):
    wait = WebDriverWait(driver, timeout)

    # ---------------------------------------------------
    # 1. Open "مرحله‌ای که مودی در آن است" dropdown
    # ---------------------------------------------------
    dropdown = wait.until(
        EC.element_to_be_clickable((
            By.XPATH,
            "//div[@data-index='4']//kendo-dropdownlist"
            "//span[contains(@class,'k-input-inner')]"
        ))
    )

    driver.execute_script("arguments[0].click();", dropdown)
    time.sleep(1)

    # ---------------------------------------------------
    # 2. Get ACTIVE popup only
    # ---------------------------------------------------
    popup = wait.until(
        EC.presence_of_element_located((
            By.XPATH,
            "//kendo-list[ancestor::*[@aria-expanded='true']]"
        ))
    )

    options = popup.find_elements(
        By.XPATH,
        ".//li[@role='option' and .//input[@type='checkbox']"
        " and not(.//text()[contains(.,'انتخاب همه')])]"
    )

    total = len(options)
    print(f"Found {total} مراحل")

    # ---------------------------------------------------
    # 3. Iterate one by one
    # ---------------------------------------------------
    for index in range(total):
        # Re-fetch popup + options (Angular re-renders!)
        popup = wait.until(
            EC.presence_of_element_located((
                By.XPATH,
                "//kendo-list[ancestor::*[@aria-expanded='true']]"
            ))
        )

        options = popup.find_elements(
            By.XPATH,
            ".//li[@role='option' and .//input[@type='checkbox']"
            " and not(.//text()[contains(.,'انتخاب همه')])]"
        )

        option = options[index]
        checkbox = option.find_element(By.XPATH, ".//input[@type='checkbox']")
        label = option.text.strip()

        # Select current مرحله
        driver.execute_script(
            "arguments[0].scrollIntoView({block:'center'});", checkbox
        )
        driver.execute_script("arguments[0].click();", checkbox)
        print(f"✅ Selected: {label}")

        # Apply filter
        click_taeed_and_wait(driver)

        # Download data
        download_data_for_current_filter(driver)

        # ---------------------------------------------------
        # Re-open dropdown to deselect
        # ---------------------------------------------------
        driver.execute_script("arguments[0].click();", dropdown)
        time.sleep(1)

        popup = wait.until(
            EC.presence_of_element_located((
                By.XPATH,
                "//kendo-list[ancestor::*[@aria-expanded='true']]"
            ))
        )

        option = popup.find_elements(
            By.XPATH,
            ".//li[@role='option' and .//input[@type='checkbox']"
            " and not(.//text()[contains(.,'انتخاب همه')])]"
        )[index]

        checkbox = option.find_element(By.XPATH, ".//input[@type='checkbox']")
        driver.execute_script("arguments[0].click();", checkbox)
        print(f"↩ Deselected: {label}")

        time.sleep(0.5)

    print("🎉 Finished processing all مراحل")


def click_taeed_and_wait(driver, timeout=30):
    wait = WebDriverWait(driver, timeout)

    taeed_btn = wait.until(
        EC.element_to_be_clickable((
            By.ID, "sharedfilter-condition-approve"
        ))
    )

    driver.execute_script("arguments[0].click();", taeed_btn)

    # 🔽 IMPORTANT: replace this with a REAL wait if you can
    # Example: wait for table rows, spinner disappear, etc.
    time.sleep(5)


def download_data_for_current_filter(driver):
    """
    Put your real download logic here.
    Button click, API wait, filesystem check, etc.
    """
    time.sleep(3)  # placeholder


def _fill_filters(driver):

    select_from_drpdown(driver, DROPDOWNS["first"], num_clicks=2)

    select_all_manbae_maliyati(driver)

    select_all_sal_amalkard_years(driver)

    iterate_marhaleh_and_download(driver)

    print("...")


def get_heiat_data(driver, path):

    driver.get(
        'https://star.tax.gov.ir/dashboard/preview?dashboardid=cf69b644-656c-4e06-8b72-3cd2478303f0')

    WebDriverWait(driver, 300).until(
        EC.presence_of_element_located((
            By.XPATH,
            "//div[@class='ng-star-inserted' and text()=' شکایات در جریان دادرسی ']"
        ))).click()

    WebDriverWait(driver, 5).until(
        EC.element_to_be_clickable((
            By.XPATH,
            ('//*[@title="بیشتر"]')
        ))).click()

    WebDriverWait(driver, 1).until(
        EC.presence_of_element_located((
            By.XPATH,
            "/html/body/div/div/div/div/ul/li[2]/span/div/span"
        ))).click()

    time.sleep(1)

    def down(driver, path):

        WebDriverWait(driver, 5).until(
            EC.element_to_be_clickable((
                By.XPATH,
                ('/html/body/div/div[2]/div/nz-modal-container/div/div/div[3]/div/button[2]/span')
            ))).click()

    try:
        t1 = threading.Thread(
            target=down, args=(driver, path))
        t2 = threading.Thread(
            target=watch_over, args=(path, 240, 2))
        t1.start()
        t2.start()
        t1.join()
        t2.join()
        wait_for_download_to_finish(self.path, ['xlsx'], sleep_time=15)

    except Exception as e:

        print(e)


def find_hoghogh_info(driver, df, get_date=True, from_scratch=False):

    if from_scratch:

        WebDriverWait(driver, 32).until(
            EC.presence_of_element_located((
                By.XPATH, '//a[.//div[text()="management.tax.gov.ir"]]'
            ))).click()

        WebDriverWait(driver, 5).until(
            EC.presence_of_element_located((
                By.XPATH, "//div[@class='menubutton' and text()='گزارشات آماری سامانه ها']"
            ))).click()

        table_element = WebDriverWait(driver, 5).until(
            EC.presence_of_element_located((
                By.XPATH, "//table[tbody/tr/td/div[text()='سامانه مالیات بر درآمد حقوق']]"
            )))

        # Find the "گزارشات کلی" button inside the located table
        table_element.find_element(
            By.XPATH, ".//span[@class='openslider' and text()='گزارشات کلی']").click()

        WebDriverWait(driver, 5).until(
            EC.presence_of_element_located((
                By.XPATH, ".//a[contains(text(), '[PN1140] گزارش مالیات ابرازی به تفکیک سال عملکرد')]"
            ))
        ).click()

        start_year = 1404
        end_year = 1404
        start_date = f"{start_year}0101"
        end_date = f"{end_year}1229"
        url = f"https://management.tax.gov.ir/StatsProject/Show/TechnicalStats/Report/177/{start_date}-{end_date}"

        driver.get(url)

        WebDriverWait(driver, 50).until(
            EC.presence_of_element_located((
                By.XPATH, "//span[text()='نمایش جزییات']"
            ))).click()

        rows = driver.find_elements(By.TAG_NAME, "tr")

        # Get all rows HTML once
        html = driver.page_source
        soup = BeautifulSoup(html, 'html.parser')

        hrefs = []
        for row in tqdm(soup.select('tr')):
            tds = row.find_all('td')
            if len(tds) >= 2:
                last_td = tds[-2]
                href_td = tds[-1]

                text = last_td.get_text(strip=True).replace(",", "")
                if text.isdigit():
                    a_tag = href_td.find('a')
                    if a_tag and a_tag.has_attr('href'):
                        hrefs.append(a_tag['href'])

        df = pd.DataFrame({'Links': hrefs, 'Done': None})

        drop_into_db('tblhoghoghUrls',
                     df.columns.tolist(),
                     df.values.tolist(),
                     append_to_prev=False)

    else:
        dfs = []
        # Visit each stored URL using driver.get()
        for index, link in tqdm(df.iterrows(), total=len(df), desc="Visiting Links", unit="link"):
            href = link['Links']
            base_url = "https://management.tax.gov.ir"

            if not href.startswith("http"):
                href = base_url + href
                try:
                    # href = item.get_attribute('href')
                    driver.get(href)  # Navigate to the link

                    # Locate the table body
                    content_area = WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located(
                            (By.ID, "CPC_LabelCenter"))
                    )

                    table_body = WebDriverWait(content_area, 1).until(
                        EC.presence_of_element_located(
                            (By.TAG_NAME, "tbody"))
                    )
                    # Extract headers (from the first row's <td> elements)
                    header_row = table_body.find_elements(
                        By.TAG_NAME, "tr")[0]  # First row
                    headers = [td.text.strip()
                               for td in header_row.find_elements(By.TAG_NAME, "th")]

                    # Extract table rows (excluding the first row)
                    data = []
                    rows = table_body.find_elements(By.TAG_NAME, "tr")[
                        1:]  # Skip the header row

                    if len(rows) < 3_000:
                        for row in tqdm(rows):
                            cells = row.find_elements(By.TAG_NAME, "td")
                            data.append([cell.text.strip() for cell in cells])

                        shenase = href.split("/")[-1].split('-')[-1]
                        df_hoghogh = pd.DataFrame(data, columns=headers)
                        df_hoghogh['شناسه'] = shenase
                        df_hoghogh['آخرین بروزرسانی'] = get_update_date()
                        # Convert to DataFrame

                        sql_query = "select * from tblhoghoghUrls where Done is not NULL"
                        append_to_prev = connect_to_sql(
                            sql_query=sql_query,
                            sql_con=get_sql_con(database='TestDb'),
                            read_from_sql=True, return_df=True).any()['Links'].astype('bool')

                        drop_into_db('tblhoghoghdata',
                                     df_hoghogh.columns.tolist(),
                                     df_hoghogh.values.tolist(),
                                     append_to_prev=append_to_prev)

                        sql_query = f"UPDATE tblhoghoghUrls SET Done = 'Yes' WHERE Links = '{link['Links']}'"
                    else:
                        raise Exception
                except Exception as e:
                    sql_query = f"UPDATE tblhoghoghUrls SET Done = 'No' WHERE Links = '{href}'"
                    print(f"Error visiting link {index + 1}: {e}")

                finally:

                    connect_to_sql(
                        sql_query=sql_query,
                        sql_con=get_sql_con(database='TestDb'),
                        read_from_sql=False, return_df=False)


def set_arzesh(driver, df, get_date=True):

    mashmool_dict = {
        'عدم مشمولیت': 1,
        'مشمول مرحله چهارم': 5,
        'مشمول مرحله دهم': 11,
        'مشمول مرحله ششم': 7,
        'مشمول مرحله نهم': 10,
        'مشمول مرحله هشتم': 9,
        'مشمول مرحله دوم': 3,
        'مشمول مرحله سوم': 4,
        'مشمول مرحله اول': 2,
    }
    # df_6.status = ""

    WebDriverWait(driver, 32).until(
        EC.presence_of_element_located((
            By.XPATH, '//a[.//div[text()="management.tax.gov.ir"]]'
        ))).click()

    time.sleep(1)
    for index, val in df.iterrows():
        is_active = 'no'
        done = 'Success'
        date = 'None'
        try:

            try:
                search_box = WebDriverWait(driver, 28).until(
                    EC.presence_of_element_located((
                        By.ID,
                        "TextboxPublicSearch"
                    )))

                search_box.clear()

                time.sleep(0.1)

                search_box.send_keys(str(val["rahgiri_code"]))

                search_box.send_keys(Keys.ENTER)
            except:
                print('...')

            time.sleep(0.5)

            if driver.find_element(
                    By.XPATH,
                    "/html/body/form/table/tbody/tr[2]/td[2]/span/table/tbody/tr[2]/td[7]/span"
            ).text == "غيرفعال":

                is_Active = 'yes'

            time.sleep(1)

            try:
                tables = driver.find_elements(
                    By.CSS_SELECTOR, "table[id^='Grid_']")

                # The value you're looking for in the "کدرهگیری" column
                target_value = str(val["rahgiri_code"])

                found = False  # To track if the value is found
                office_name = None

                for table in tables:
                    headers = table.find_elements(By.TAG_NAME, "th")

                    # Check if the table has the "کدرهگیری" column
                    code_column_index = None
                    office_column_index = None
                    for index, header in enumerate(headers):
                        if header.text == "کدرهگیری":
                            code_column_index = index
                        if header.text == "اداره مالیاتی":
                            office_column_index = index

                    # Continue only if both "کدرهگیری" and "اداره مالیاتی" columns are found
                    if code_column_index is not None and office_column_index is not None:
                        # Get all rows in the table (excluding the header)
                        rows = table.find_elements(By.TAG_NAME, "tr")[1:]

                        for row in rows:
                            columns = row.find_elements(By.TAG_NAME, "td")
                            if columns[code_column_index].text == target_value:
                                # Found the row with the target value in "کدرهگیری"
                                office_name = columns[office_column_index].text
                                found = True
                                try:
                                    # Find and click the 'نمایش شناسنامه' span in the same row
                                    show_profile_button = row.find_element(
                                        By.XPATH, ".//span[contains(text(),'نمایش شناسنامه')]")
                                    time.sleep(0.2)
                                    show_profile_button.click()
                                    break
                                except NoSuchElementException:
                                    print(
                                        "Could not find 'نمایش شناسنامه' button in the row.")
                                    break

                    if found:
                        break

            except:
                done = city
                raise Exception

            if get_date:

                date = WebDriverWait(driver, 28).until(
                    EC.presence_of_element_located((
                        By.XPATH,
                        "/html/body/form/table/tbody/tr[2]/td[2]/span[3]/table[2]/tbody/tr/td/div/div[1]/table[1]/tbody/tr[1]/td[1]/table/tbody/tr[11]/td[2]"
                    ))).text

            else:

                try:

                    WebDriverWait(driver, 4).until(
                        EC.presence_of_element_located((
                            By.XPATH,
                            "/html/body/form/table/tbody/tr[2]/td[2]/span[3]/table[1]/tbody/tr/td[5]/a/div"
                        ))).click()

                    WebDriverWait(driver, 4).until(
                        EC.presence_of_element_located((
                            By.XPATH, "//a//div[text()='تعیین مشمولیت ارزش افزوده']"
                        ))).click()

                except exception as e:

                    print(e)
                    raise Exception

                sel = Select(
                    driver.find_element(
                        By.ID, 'CPC_DDLVatEligibilityType'))
                sel.select_by_index(mashmool_dict[val['mashmool']])

                if mashmool_dict[val['mashmool']] != 1:

                    WebDriverWait(driver, 28).until(
                        EC.presence_of_element_located((
                            By.ID, "CPC_TextBoxVatEligibilityDate"
                        ))).send_keys(val['تاريخ'])

                WebDriverWait(driver, 28).until(
                    EC.presence_of_element_located((
                        By.ID, "CPC_ButtonVatEligibility"
                    ))).click()

        except Exception as e:
            if re.search(r'error', driver.current_url, re.IGNORECASE):
                driver.get(
                    'https://management.tax.gov.ir/Public/Process/Homepage')
            print(e)
            done = 'Failed'

        finally:
            if get_date:
                sql_query = f"UPDATE tblSetArzesh SET done = '{done}', تاريخ='{date}' WHERE rahgiri_code = '{val['rahgiri_code']}'"
            else:
                sql_query = f"UPDATE tblSetArzesh SET done = '{done}' WHERE rahgiri_code = '{val['rahgiri_code']}'"
            connect_to_sql(
                sql_query=sql_query,
                sql_con=get_sql_con(database='TestDb'),
                read_from_sql=False, return_df=False)

        time.sleep(1)

    driver.close()


@wrap_it_with_params(10, 10, True, False, False, False)
def scrape_it(path, driver_type, headless=False, data_gathering=False, pred_captcha=False, driver=None, info={}):
    with init_driver(
            pathsave=path, driver_type=driver_type, headless=headless, driver=driver, info=info) as driver:
        path = path
        driver, info = login_codeghtesadi(
            driver=driver, data_gathering=data_gathering, pred_captcha=pred_captcha, info=info)

    return driver, info


def save_in_db(path, year):
    with open('saved_model_ngram1_2', 'rb') as f:
        model = pickle.load(f)
    dirs = []
    address_dict = get_soratmoamelat_mapping_address()
    dirs.extend([it.path for it in os.scandir(path) if it.is_dir()])
    final_dirs = dirs
    for item in dirs:
        final_dirs.extend([it.path for it in os.scandir(item) if it.is_dir()])
    for item in final_dirs:
        tbl_name = item.split('\\')[-1]
        files = list_files(item, 'xlsx')
        notfirst = False
        if len(files) > 0:
            for file in files:
                df = pd.read_excel(file)
                cols = df.loc[0].values.tolist()
                df = df.iloc[1:, :]
                df = df.astype(str)
                df.columns = cols
                df['آخرین بروزرسانی'] = get_update_date()
                df['اداره'] = model.predict(
                    df[address_dict[tbl_name]].to_numpy())
                drop_into_db(table_name=tbl_name + '%s' % year,
                             columns=df.columns.tolist(),
                             values=df.values.tolist(), append_to_prev=notfirst)
                notfirst = True


@wrap_it_with_paramsv1(15, 10, True, False, False, True)
def close_modal_dadrasi(driver, info):

    if (WebDriverWait(driver, 1).until(
            EC.presence_of_element_located((
                By.XPATH,
                '/html/body/div[9]/div/div/div[3]/button'
            )))):

        WebDriverWait(driver, 1).until(
            EC.presence_of_element_located((
                By.XPATH,
                '/html/body/div[9]/div/div/div[3]/button'
            ))).click()

    return driver, info


@wrap_it_with_paramsv1(5, 10, True, False, False, True)
def click_on_search_dadrasi(driver, info, path='/html/body/div[1]/nav/div[2]/ul/li[5]/a/span'):
    WebDriverWait(driver, 8).until(
        EC.presence_of_element_located((
            By.XPATH,
            path
        ))).click()
    return driver, info


@wrap_it_with_paramsv1(15, 10, True, False, False, True)
def click_on_btnsearch_dadrasi(driver, info):
    WebDriverWait(driver, 8).until(
        EC.presence_of_element_located((
            By.XPATH,
            '/html/body/div[1]/div/div/form/div[11]/div/button'
        ))).click()
    return driver, info


@wrap_it_with_paramsv1(15, 10, True, False, False, True)
def get_dadrasi_table(driver, info):
    info['table'] = WebDriverWait(driver, 4).until(
        EC.presence_of_element_located((
            By.XPATH,
            '/html/body/div[1]/div/div/div[2]/table'
        )))
    return driver, info


@wrap_it_with_paramsv1(10, 10, True, False, False, True)
def wait_for_next_page_dadrasi(driver, info):
    text_changed = WebDriverWait(driver, 8).until(
        EC.presence_of_element_located((
            By.XPATH,
            '/html/body/div[1]/div/div/div[2]/div[2]/span[2]'
        ))).text

    driver.find_element(By.XPATH, '/html/body/div[1]/div/div/div[2]/div[2]/ul').\
        find_elements(By.TAG_NAME, 'li')[-1].click()
    time_out = 60
    while text_changed == WebDriverWait(driver, 8).until(
            EC.presence_of_element_located((
                By.XPATH,
                '/html/body/div[1]/div/div/div[2]/div[2]/span[2]'
            ))).text:
        print('waiting')
        time.sleep(2)
        time_out -= 2
        if time_out == 0:
            print('driver needs to be refreshed...')
            driver.refresh()
            time_out == 60
    return driver, info


@wrap_it_with_paramsv1(15, 10, True, False, False, True)
def return_dadrasi_pages(driver, info):

    info['pages'] = int(extract_nums(WebDriverWait(driver, 16).until(
        EC.presence_of_element_located((
            By.XPATH,
            '/html/body/div[1]/div/div/div[2]/div[2]/ul/li[13]/a'
        ))).text)[0])

    return driver, info


@wrap_it_with_paramsv1(15, 10, True, False, False, True)
def click_on_filter_dadrasi_pages(driver, info):

    WebDriverWait(driver, 16).until(
        EC.presence_of_element_located((
            By.XPATH,
            '/html/body/div[1]/div/div/div[2]/table/thead/tr/th[18]/span/i[2]'
        ))).click()

    return driver, info


def check_if_dadrasi_updated(pages, num_pages_updated=0, records_perpage=50):
    sql_query = """
    IF OBJECT_ID('tbldadrasi', 'U') IS NOT NULL
        BEGIN
            -- Table exists, so perform the SELECT query
            SELECT MAX([تاریخ بروزرسانی]) FROM tbldadrasi;
        END
        ELSE
        BEGIN
            -- Table does not exist, handle accordingly
            SELECT 0;
        END
    """
    df = connect_to_sql(sql_query, read_from_sql=True, return_df=True)

    if df.loc[0].item() == get_update_date():
        sql_query = "SELECT COUNT(*) FROM tbldadrasi"
        num_pages_updated = int(math.ceil(connect_to_sql(
            sql_query, read_from_sql=True, return_df=True).loc[0].item() / records_perpage))

        if num_pages_updated == pages:
            return (True, 0)

    return (False, num_pages_updated)


@wrap_it_with_paramsv1(15, 10, True, False, False, True)
def navigate_to_next_dadrasi_page(driver=None, info={}, new_url='', timeout=30):
    driver.get(new_url)

    try:
        while (WebDriverWait(driver, 2).until(
                EC.presence_of_element_located((
                    By.CLASS_NAME,
                    'busy-indicator'
                )))):
            print('waiting for navigation')
            time.sleep(1)
            timeout -= 1
            if timeout == 0:
                raise Exception
            else:
                continue
    except:
        if timeout == 0:
            raise Exception

    return driver, info


@wrap_it_with_paramsv1(5, 10, True, False, False, True)
def wait_fordadrasi(driver=None, info={}):
    try:
        if (WebDriverWait(driver, 1).until(
                EC.presence_of_element_located((
                    By.XPATH,
                    '/html/body/div[1]/nav/div[2]/ul/li[5]/a/span'
                ))).is_displayed()):
            return driver, info
    except Exception as e:
        print('Waiting for the dadrasi application')
        raise Exception

    return driver, info


@wrap_it_with_paramsv1(15, 10, True, False, False, True)
def click_on_link_dadrasi(driver=None, info={}):
    WebDriverWait(driver, 32).until(
        EC.presence_of_element_located((
            By.XPATH, '//a[.//div[text()="dadresi.tax.gov.ir"]]'
        ))).click()
    return driver, info


@wrap_it_with_paramsv1(15, 10, True, False, False, True)
def click_on_reports_link(driver=None, info={}):
    WebDriverWait(driver, 32).until(
        EC.presence_of_element_located((
            By.XPATH,
            '/html/body/div[1]/div[3]/div[2]/div[2]/div/div[13]/div/div[2]/div/a/i'
        ))).click()
    return driver, info


@wrap_it_with_paramsv1(15, 10, True, False, False, True)
def click_on_reports_menu(driver=None, info={}, down_url=None):

    driver.get(down_url)

    # WebDriverWait(driver, 32).until(
    #     EC.presence_of_element_located((
    #         By.XPATH,
    #         '/html/body/div[2]/nav/div[2]/div[1]/div/ul/li[3]/a/i'
    #     ))).click()

    # WebDriverWait(driver, 32).until(
    #     EC.presence_of_element_located((
    #         By.XPATH,
    #         '/html/body/div[2]/nav/div[2]/div[1]/div/ul/li[3]/a/span'
    #     ))).click()
    # WebDriverWait(driver, 32).until(
    #     EC.presence_of_element_located((
    #         By.XPATH,
    #         '/html/body/div[2]/nav/div[2]/div[1]/div/ul/li[3]/div/ul/li[1]/a'
    #     ))).click()
    return driver, info


@wrap_it_with_paramsv1(15, 10, True, False, False, True)
def click_on_sabtenamcodeeghtesadi_downlink(driver=None, info={}):
    WebDriverWait(driver, 8).until(
        EC.presence_of_element_located((
            By.XPATH,
            '/html/body/div[2]/div[1]/div[2]/div[1]/div[2]/div/div/div/div[1]/div[2]/div[2]/table/tbody/tr[14]/td[7]/a/span'
        ))).click()

    WebDriverWait(driver, 8).until(
        EC.presence_of_element_located((
            By.XPATH,
            '/html/body/div[3]/div/div[3]/button[1]'
        ))).click()

    return driver, info


def click_on_sabtenamcodeeghtesadi_downlinkconfirm(driver=None, info={}):
    WebDriverWait(driver, 8).until(
        EC.presence_of_element_located((
            By.XPATH,
            '/html/body/div[3]/div/div[3]/button[1]'
        ))).click()
    return driver, info


@wrap_it_with_paramsv1(1, 10, True, False, False, True)
def wait_till_dadrasi_visible(driver=None, info={}):
    while True:
        driver, info = click_on_link_dadrasi(driver=driver, info=info)
        driver, info = wait_fordadrasi(driver=driver, info=info)
        if not info['success']:
            driver.back()
        else:
            return driver, info


@wrap_it_with_paramsv1(15, 10, True, False, False, True)
def get_sabtenamCodeEghtesadiData(driver=None,
                                  path=None,
                                  down_url="https://reports.tax.gov.ir/Reports/RegistrationReports/AllTaxpayerDataWithVatInfo",
                                  del_prev_files=True, info=None, *args, **kwargs):

    if del_prev_files:
        remove_excel_files(file_path=path, postfix=['.xls', '.html', '.xlsx'])

    driver, info = click_on_reports_link(driver=driver, info=info)

    driver, info = click_on_reports_menu(
        driver=driver, info=info, down_url=down_url)

    download_excel(func=lambda: click_on_sabtenamcodeeghtesadi_downlink(driver=driver, info=info),
                   table_name="sabtenamcodeeghtesadi",
                   path=path)
    open_and_save_excel_files(path=path, only_save=False,
                              save_as_csv=False, save_into_sql=True,
                              table_name='tblCodeeghtesadiArzesh', multi_process=True, db_name='TestDb')
    return driver, info


@wrap_it_with_paramsv1(15, 10, True, False, False, True)
def set_enseraf_heiat(driver=None, path=None, info=None, heiat_shenases=None, *args, **kwargs):

    driver, info = wait_till_dadrasi_visible(driver=driver, info=info)

    for shenase in tqdm(heiat_shenases['shenase_enseraf']):

        try:
            WebDriverWait(driver, 50).until(
                EC.presence_of_element_located((
                    By.CLASS_NAME,
                    'search-box'
                ))).clear()

            WebDriverWait(driver, 50).until(
                EC.presence_of_element_located((
                    By.CLASS_NAME,
                    'search-box'
                ))).send_keys(shenase)

            time.sleep(3)

            WebDriverWait(driver, 60).until(
                EC.presence_of_element_located((
                    By.XPATH, f"//span[text()='0{shenase}']/parent::a"
                ))).click()

            time.sleep(2)

            condition = WebDriverWait(driver, 30).until(
                EC.presence_of_element_located((
                    By.XPATH, '//td[@name="LifeCycle"]'
                ))).text

            if condition == 'اعلام انصراف مودی توسط کاربر':
                df_data = connect_to_sql(f"UPDATE tblenseraf_heait SET [done] = 'Yes' WHERE [shenase_enseraf]='{shenase}'",
                                         read_from_sql=False,
                                         return_df=False,)
                continue

            WebDriverWait(driver, 60).until(
                EC.presence_of_element_located((
                    By.NAME,
                    'CancelComplaintByEmployee'
                ))).click()

        except:
            df_data = connect_to_sql(f"UPDATE tblenseraf_heait SET [done] = 'Error' WHERE [shenase_enseraf]='{shenase}'",
                                     read_from_sql=False,
                                     return_df=False,)
            continue

        sel = Select(
            driver.find_element(
                By.ID, 'CancelComplaintByEmployee_Reason'))
        sel.select_by_index(2)

        WebDriverWait(driver, 35).until(EC.visibility_of_element_located(
            (By.XPATH,
             "//span[@class='other-frequent-text name' and contains(., 'دستور مدیر دادرسی')]")
        )).click()

        time.sleep(2)

        WebDriverWait(driver, 40).until(
            EC.presence_of_element_located((
                By.XPATH, "//button[@class='submit-button btn btn-danger' and contains(., 'اعلام رد یا انصراف')]"
            ))).click()

        try:
            while True:
                try:
                    WebDriverWait(driver, 1).until(
                        EC.presence_of_element_located((
                            By.XPATH, "//button[@class='submit-button btn btn-danger' and contains(., 'اعلام رد یا انصراف')]"
                        ))
                    )

                    break
                except:
                    continue

        finally:
            df_data = connect_to_sql(f"UPDATE tblenseraf_heait SET [done] = 'Yes' WHERE [shenase_enseraf]='{shenase}'",
                                     read_from_sql=False,
                                     return_df=False,)


@wrap_it_with_paramsv1(15, 10, True, False, False, True)
def get_dadrasidata(driver=None, path=None, del_prev_files=True, urls=None, init=True, info=None, *args, **kwargs):

    if init:

        driver, info = wait_till_dadrasi_visible(driver=driver, info=info)

        driver, info = click_on_search_dadrasi(driver=driver, info=info)

        if del_prev_files:
            remove_excel_files(file_path=path, postfix=['.xls', '.html'])

        # driver, info = click_on_btnsearch_dadrasi(driver=driver, info=info)

        driver, info = get_dadrasi_table(driver=driver, info=info)

        driver, info = return_dadrasi_pages(driver=driver, info=info)

        # driver, info = click_on_filter_dadrasi_pages(driver=driver, info=info)

        is_updated, num_pages_updated = check_if_dadrasi_updated(
            info['pages'], records_perpage=50)

        if is_updated:
            return

        append_to_prev = True if num_pages_updated > 0 else False
        # append_to_prev = True
        ind = 0

        WebDriverWait(driver, 8).until(
            EC.presence_of_element_located((
                By.XPATH,
                '/html/body/div[1]/div/div/div[2]/div[2]/ul/li[14]/a/span'
            ))).click()
        driver, info = wait_for_next_page_dadrasi(driver=driver, info=info)

        urls = []

        for index, value in enumerate(range(num_pages_updated + ind, info['pages']+1)):

            new_url = re.sub(
                r"(%22\d+%22)", f"%22{value}%22", driver.current_url)
            urls.append(new_url)
            continue
        df_pages = pd.DataFrame(urls, columns=['urls'])
        df_pages['تاریخ بروزرسانی'] = None
        drop_into_db('tbldadrasiUrls',
                     df_pages.columns.tolist(),
                     df_pages.values.tolist(),
                     append_to_prev=False)
        return driver, info

    else:

        # driver, info = navigate_to_next_dadrasi_page(
        #     driver=driver, info=info, new_url=new_url)

        is_updated, num_pages_updated = check_if_dadrasi_updated(
            0, records_perpage=50)

        append_to_prev = True if num_pages_updated > 0 else False

        driver, info = wait_till_dadrasi_visible(driver=driver, info=info)

        driver, info = click_on_search_dadrasi(driver=driver, info=info)

        for index, row in urls.iterrows():

            try:

                driver.get(row['urls'])

                table = WebDriverWait(driver, 32).until(
                    EC.presence_of_element_located((
                        By.XPATH,
                        '/html/body/div[1]/div/div/div[2]/table'
                    )))
                columns = table.find_elements(By.TAG_NAME, 'thead')[0].\
                    find_elements(By.TAG_NAME, 'tr')[0].\
                    find_elements(By.TAG_NAME, 'label')
                columns_names = list(
                    filter(None, [name.text for name in columns]))
                # columns_names = [name.text for name in columns]

                table_rows = table.find_elements(By.TAG_NAME, 'tbody')[0].\
                    find_elements(By.TAG_NAME, 'tr')
                table_tds = [item.find_elements(
                    By.XPATH, 'td') for item in table_rows]

                lst = []
                for items in table_tds:
                    try:
                        lst_tmp = []
                        for index, item in enumerate(items):
                            # if index not in [2]:
                            if ('...' in item.text and index != 10):
                                try:
                                    text = unquote(item.
                                                   find_element(By.CLASS_NAME, 'text-ellipsise').
                                                   get_attribute('data-text'))
                                except:
                                    text = item.text
                            else:
                                text = item.text
                            lst_tmp.append(text)

                        lst.append(
                            pd.Series(data=lst_tmp, index=columns_names))
                    except Exception as e:
                        print(e)
                        continue
                df = pd.concat(lst, axis=1).T
                df['تاریخ بروزرسانی'] = get_update_date()
                # Check if the column exists
                # if 'دوره' in df.columns:
                # Drop the column
                # df = df.drop('دوره', axis=1)
                drop_into_db('tbldadrasi',
                             df.columns.tolist(),
                             df.values.tolist(),
                             append_to_prev=append_to_prev)
                append_to_prev = True

                df_data = connect_to_sql(f"UPDATE tbldadrasiUrls SET [تاریخ بروزرسانی] = '{get_update_date()}' WHERE urls='{row['urls']}'",
                                         read_from_sql=False,
                                         return_df=False,)
            except:
                continue
        # driver, info = wait_for_next_page_dadrasi(driver=driver, info=info)

        print('...')

        return driver, info


@wrap_it_with_paramsv1(15, 10, True, False, False, True)
def get_amlak_row(driver=None, info={}, download_excel=False, edit=False, get_files=True):

    city = info['row'].find_elements(By.TAG_NAME, 'td')[2].text
    # Click on City link
    info['row'].find_elements(By.TAG_NAME, 'td')[
        13].find_element(By.TAG_NAME, 'a').click()
    # Number of rows per city
    tbl = WebDriverWait(driver, 32).until(
        EC.element_to_be_clickable((
            By.XPATH,
            '/html/body/form/table/tbody/tr[2]/td[2]/span[2]/table'
        )))
    rw = tbl.find_elements(By.TAG_NAME, 'tr')[1]
    # Iterate over rows per city
    # for ind, rw in enumerate(rws):
    rw.find_elements(By.TAG_NAME, 'td')[
        8].find_element(By.TAG_NAME, 'a').click()
    # Download Excel
    if get_files:
        WebDriverWait(driver, 32).until(
            EC.element_to_be_clickable((
                By.XPATH,
                '/html/body/form/table/tbody/tr[2]/td[2]/span[2]/a[2]/div'
            ))).click()
        time.sleep(2)

    if edit:
        df = pd.read_excel(
            r'D:\backup\New folder\excel_1402_فروردین_162600.xlsx')
        # calculate the new value
        df['new_ayan'] = (df['ارزش روز اعیان (ریال)'].str.replace(
            ',', '').astype(int) * 1.1).astype(int)
        df['new_arse'] = (df['ارزش روز عرصه (ریال)'].str.replace(
            ',', '').astype(int) * 1.1).astype(int)
        tbl_edit = WebDriverWait(driver, 8).until(
            EC.element_to_be_clickable((
                By.XPATH,
                '/html/body/form/table/tbody/tr[2]/td[2]/span[2]/table'
            )))
        rws_edit = tbl_edit.find_elements(By.TAG_NAME, 'tr')[1:-1]
        for ind, rw in enumerate(rws_edit):
            print(f'editing index: {ind}')
            try:
                print(f'Total = {len(rws_edit[:-1])}')
                while (True):
                    try:
                        rws_edit[ind].find_elements(By.TAG_NAME, 'td')[
                            9].find_element(By.TAG_NAME, 'a').click()
                        break
                    except Exception as e:
                        print(e)
                        continue
                val_arse = int(df.loc[ind]['new_arse'])
                val_ayan = int(df.loc[ind]['new_ayan'])
                time.sleep(1)
                # Clear the input
                driver.find_element(By.XPATH,
                                    '//*[@id="CPC_TextboxModificationLocalRealLandValue"]').clear()
                driver.find_element(By.XPATH,
                                    '//*[@id="CPC_TextboxModificationLocalRealLandValue"]').click()
                driver.find_element(By.XPATH,
                                    '//*[@id="CPC_TextboxModificationLocalRealLandValue"]').send_keys(str(val_arse))
                driver.find_element(By.XPATH,
                                    '//*[@id="CPC_TextboxModificationLocalRealBuldingValue"]').clear()
                driver.find_element(By.XPATH,
                                    '//*[@id="CPC_TextboxModificationLocalRealBuldingValue"]').click()
                driver.find_element(By.XPATH,
                                    '//*[@id="CPC_TextboxModificationLocalRealBuldingValue"]').send_keys(str(val_ayan))
                time.sleep(1)
                driver.find_element(By.ID,
                                    'CPC_ButtonModificationLocalBlockRowSave').click()
                try:
                    while (driver.find_element(By.ID,
                                               'CPC_ButtonModificationLocalBlockRowSave').is_displayed()):
                        time.sleep(1)
                except:
                    time.sleep(3)
                    tbl_edit = WebDriverWait(driver, 8).until(
                        EC.element_to_be_clickable((
                            By.XPATH,
                            '/html/body/form/table/tbody/tr[2]/td[2]/span[2]/table'
                        )))
                    rws_edit = tbl_edit.find_elements(
                        By.TAG_NAME, 'tr')[1:-1]
            except Exception as e:
                print(f'row number: {rws_edit[ind]}')
                print(e)

    # Go back
    WebDriverWait(driver, 16).until(
        EC.element_to_be_clickable((
            By.XPATH,
            '/html/body/form/table/tbody/tr[2]/td[2]/span[2]/a[1]/div'
        ))).click()
    time.sleep(3)
    WebDriverWait(driver, 16).until(
        EC.element_to_be_clickable((
            By.XPATH,
            '/html/body/form/table/tbody/tr[2]/td[2]/span[2]/a[1]/div'
        ))).click()

    tbl = WebDriverWait(driver, 32).until(
        EC.element_to_be_clickable((
            By.XPATH,
            '/html/body/form/table/tbody/tr[2]/td[2]/span[2]/table'
        )))
    rws = tbl.find_elements(By.TAG_NAME, 'tr')[1:]
    time.sleep(0.5)

    # inner_table = WebDriverWait(driver, 32).until(
    #     EC.presence_of_element_located((
    #         By.XPATH,
    #         '/html/body/form/table/tbody/tr[2]/td[2]/span[2]/table'
    #     )))
    # inner_rows = table.find_elements(By.TAG_NAME, 'tr')[1:]

    return driver, info


@wrap_it_with_paramsv1(15, 10, True, False, False, True)
def get_amlak(driver=None, path=None, del_prev_files=True, info=None, *args, **kwargs):
    try:
        while (WebDriverWait(driver, 4).until(
                EC.element_to_be_clickable((
                    By.XPATH,
                    '/html/body/div[1]/div[3]/div[2]/div[2]/div/div[5]/div/div[2]/div/a'
                ))).is_displayed()):

            WebDriverWait(driver, 4).until(
                EC.element_to_be_clickable((
                    By.XPATH,
                    '/html/body/div[1]/div[3]/div[2]/div[2]/div/div[5]/div/div[2]/div/a'
                ))).click()
            time.sleep(2)
    except:

        WebDriverWait(driver, 32).until(
            EC.element_to_be_clickable((
                By.XPATH,
                '/html/body/form/table/tbody/tr[2]/td[1]/div/div[2]'
            ))).click()
        WebDriverWait(driver, 32).until(
            EC.element_to_be_clickable((
                By.XPATH,
                '/html/body/form/table/tbody/tr[2]/td[1]/div/div[3]/a[4]/div'
            ))).click()
        table = WebDriverWait(driver, 32).until(
            EC.element_to_be_clickable((
                By.XPATH,
                '/html/body/form/table/tbody/tr[2]/td[2]/span[2]/table'
            )))

        start_index = 1
        rows = table.find_elements(By.TAG_NAME, 'tr')[1:25]

        for index, row in enumerate(rows):
            info['row'] = rows[start_index]
            driver, info = get_amlak_row(driver=driver, info=info)

            table = WebDriverWait(driver, 32).until(
                EC.element_to_be_clickable((
                    By.XPATH,
                    '/html/body/form/table/tbody/tr[2]/td[2]/span[2]/table'
                )))

            rows = table.find_elements(By.TAG_NAME, 'tr')[1:25]
            start_index += 1

        print('f')
    return driver, info


def get_eghtesadidata(driver=None, path=None, del_prev_files=True, *args, **kwargs):

    if del_prev_files:
        remove_excel_files(file_path=path, postfix=['.xls', '.html'])

    WebDriverWait(driver, 8).until(
        EC.presence_of_element_located((
            By.XPATH,
            '/html/body/div/div[3]/div[2]/div/div/div[1]/div/div[2]/div/a'
        )))
    driver.find_element(
        By.XPATH,
        '/html/body/div/div[3]/div[2]/div/div/div[1]/div/div[2]/div/a'
    ).click()

    WebDriverWait(driver, 8).until(
        EC.presence_of_element_located((
            By.XPATH,
            '/html/body/form/table/tbody/tr[2]/td[1]/a[3]/div'
        )))
    driver.find_element(
        By.XPATH,
        '/html/body/form/table/tbody/tr[2]/td[1]/a[3]/div'
    ).click()
    WebDriverWait(driver, 8).until(
        EC.presence_of_element_located((
            By.XPATH,
            '/html/body/form/table/tbody/tr[2]/td[2]/span[2]/div[2]/table/tbody/tr/td[2]/div/span'
        )))
    driver.find_element(
        By.XPATH,
        '/html/body/form/table/tbody/tr[2]/td[2]/span[2]/div[2]/table/tbody/tr/td[2]/div/span'
    ).click()

    WebDriverWait(driver, 8).until(
        EC.presence_of_element_located((
            By.XPATH,
            '/html/body/form/table/tbody/tr[2]/td[2]/span[2]/div[2]/table/tbody/tr/td[2]/div/div/a[1]'
        )))
    driver.find_element(
        By.XPATH,
        '/html/body/form/table/tbody/tr[2]/td[2]/span[2]/div[2]/table/tbody/tr/td[2]/div/div/a[1]'
    ).click()

    def if_downloaded():
        nonlocal is_done
        file_list = glob.glob(path + "/*" + '.xls')

        if len(file_list) == 0:
            WebDriverWait(driver, 8).until(
                EC.presence_of_element_located((
                    By.XPATH,
                    '/html/body/form/table/tbody/tr[2]/td[2]/span[2]/div/a'
                )))
            driver.find_element(
                By.XPATH,
                '/html/body/form/table/tbody/tr[2]/td[2]/span[2]/div/a'
            ).click()

            file_list = glob.glob(path + "/*" + '.xls.part')

            while len(file_list) != 0:

                time.sleep(5)
                print('waiting')
                file_list = glob.glob(path + "/*" + '.xls.part')

            time.sleep(5)

            file_list = glob.glob(path + "/*" + '.xls')
            if len(file_list) != 0:
                is_done = True

    is_done = False

    while not is_done:
        if_downloaded()


@log_the_func('none')
def scrape_arzeshafzoodeh_helper(path, del_prev_files=True,
                                 headless=False, driver_type='firefox',
                                 *args, **kwargs):
    if del_prev_files:
        remove_excel_files(file_path=path, postfix=[
                           '.xls', '.html'], field=kwargs['field'])
    driver = init_driver(pathsave=path,
                         driver_type=driver_type,
                         headless=headless,
                         field=kwargs['field'])

    driver = login_arzeshafzoodeh(driver, field=kwargs['field'])
    WebDriverWait(driver, 8).until(
        EC.presence_of_element_located((
            By.XPATH,
            '/html/body/form/div[3]/table/tbody/tr[2]/td/div/table/tbody/tr[11]/td/div/ul/li[10]/a/span'
        )))
    driver.find_element(
        By.XPATH,
        '/html/body/form/div[3]/table/tbody/tr[2]/td/div/table/tbody/tr[11]/td/div/ul/li[10]/a/span'
    ).click()
    WebDriverWait(driver, 8).until(
        EC.presence_of_element_located((
            By.XPATH,
            '/html/body/form/div[3]/table/tbody/tr[2]/td/div/table/tbody/tr[11]/td/div/ul/li[10]/div/ul/li[16]/a/span'
        )))
    driver.find_element(
        By.XPATH,
        '/html/body/form/div[3]/table/tbody/tr[2]/td/div/table/tbody/tr[11]/td/div/ul/li[10]/div/ul/li[16]/a/span'
    ).click()

    WebDriverWait(driver, 8).until(
        EC.presence_of_element_located(
            (By.ID, 'ctl00_ContentPlaceHolder1_chkAuditStatus_0')))
    driver.find_element(
        By.ID, 'ctl00_ContentPlaceHolder1_chkAuditStatus_0').click()

    WebDriverWait(driver, 8).until(
        EC.presence_of_element_located(
            (By.ID, 'ctl00_ContentPlaceHolder1_chkAuditStatus_2')))
    driver.find_element(
        By.ID, 'ctl00_ContentPlaceHolder1_chkAuditStatus_2').click()
    WebDriverWait(driver, 8).until(
        EC.presence_of_element_located(
            (By.ID, 'ctl00_ContentPlaceHolder1_chkAuditStatus_3')))
    driver.find_element(
        By.ID, 'ctl00_ContentPlaceHolder1_chkAuditStatus_3').click()

    def arzesh(i):
        WebDriverWait(driver, 8).until(
            EC.presence_of_element_located(
                (By.ID, 'ctl00_ContentPlaceHolder1_frm_year')))
        sel = Select(
            driver.find_element(
                By.ID, 'ctl00_ContentPlaceHolder1_frm_year'))
        sel.select_by_index(i)

        WebDriverWait(driver, 8).until(
            EC.presence_of_element_located(
                (By.ID, 'ctl00_ContentPlaceHolder1_frm_period')))
        sel = Select(
            driver.find_element(
                By.ID, 'ctl00_ContentPlaceHolder1_frm_period'))
        sel.select_by_index(0)

        WebDriverWait(driver, 8).until(
            EC.presence_of_element_located(
                (By.ID, 'ctl00_ContentPlaceHolder1_To_year')))
        sel = Select(
            driver.find_element(
                By.ID, 'ctl00_ContentPlaceHolder1_To_year'))
        sel.select_by_index(i)

        WebDriverWait(driver, 8).until(
            EC.presence_of_element_located(
                (By.ID, 'ctl00_ContentPlaceHolder1_To_period')))
        sel = Select(
            driver.find_element(
                By.ID, 'ctl00_ContentPlaceHolder1_To_period'))
        sel.select_by_index(3)

        WebDriverWait(driver, 8).until(
            EC.presence_of_element_located(
                (By.ID, 'ctl00_ContentPlaceHolder1_Button3')))
        time.sleep(10)
        driver.find_element(
            By.ID, 'ctl00_ContentPlaceHolder1_Button3').click()

    for i in range(0, 15):

        t1 = threading.Thread(target=arzesh, args=(i, ))
        t2 = threading.Thread(target=watch_over)
        t1.start()
        t2.start()
        t1.join()
        t2.join()

    file_list = glob.glob(path + "/*" + '.xls.part')
    # While the files are not completely downloaded just wait
    while len(file_list) != 0:

        time.sleep(1)
        file_list = glob.glob(path + "/*" + '.xls.part')

    time.sleep(3)
    driver.close()


def get_mergeddf_arzesh(path, return_df=True):
    dest = os.path.join(path, 'temp')
    file_list = glob.glob(path + "/*" + '.xls')
    if file_list:
        # dest = path
        rename_files(path,
                     dest=dest,
                     file_list=file_list,
                     years=lst_years_arzeshafzoodeSonati)
    df_arzesh = merge_multiple_html_files(path=dest,
                                          drop_into_sql=False,
                                          drop_to_excel=True,
                                          add_extraInfoToDf=True,
                                          return_df=True)

    if return_df:
        return df_arzesh


def insert_arzeshafzoodelSonati(df_arzesh=None):
    if df_arzesh is None:
        df_arzesh = pd.read_excel(
            r'E:\automating_reports_V2\saved_dir\arzeshafzoodeh_sonati\temp\final_df.xlsx'
        )
    df_arzesh['شماره پرونده'] = df_arzesh['شماره پرونده'].astype(
        'int64')
    df_arzesh = df_arzesh.astype('str')
    drop_into_db('tblArzeshAfzoodeSonati',
                 df_arzesh.columns.tolist(),
                 df_arzesh.values.tolist(),
                 append_to_prev=False)


# Drop sabtenamArzeshAfzoode into sql
def insert_sabtenamArzeshAfzoodeh():
    df_sabtarzesh = pd.read_excel(
        r'E:\automating_reports_V2\saved_dir\arzeshafzoodeh_sonati\temp\ثبت نام ارزش افزوده.xlsx'
    )

    df_sabtarzesh = df_sabtarzesh[
        df_sabtarzesh['کدرهگیری'].notna()]
    df_sabtarzesh['کدرهگیری'] = df_sabtarzesh['کدرهگیری'].astype(
        'int64')
    df_sabtarzesh['شناسه'] = df_sabtarzesh['شناسه'].astype('int64')
    df_sabtarzesh = df_sabtarzesh.astype('str')
    drop_into_db('tblSabtenamArzeshAfzoode',
                 df_sabtarzesh.columns.tolist(),
                 df_sabtarzesh.values.tolist(),
                 append_to_prev=False)


def insert_codeEghtesadi():
    df_codeeghtesadi = pd.read_excel(
        r'E:\automating_reports_V2\saved_dir\codeghtesadi\codeeghtesadi.xlsx'
    )
    df_codeeghtesadi['کد رهگیری'] = df_codeeghtesadi[
        'کد رهگیری'].astype('int64')
    df_codeeghtesadi = df_codeeghtesadi.astype('str')
    drop_into_db('tblSabtenamCodeEghtesadi',
                 df_codeeghtesadi.columns.tolist(),
                 df_codeeghtesadi.values.tolist(),
                 append_to_prev=False)


def insert_gashtPosti():

    df_gasht = pd.read_excel(
        r'E:\automating_reports_V2\saved_dir\arzeshafzoodeh_sonati\temp\گشت پستی استان.xlsx'
    )

    is_non_numeric = pd.to_numeric(df_gasht['گشت پستی'],
                                   errors='coerce').isnull()
    df_gasht = df_gasht[~is_non_numeric]
    index_gasht = df_gasht[df_gasht['گشت پستی']]
    df_gasht['گشت پستی'] = df_gasht['گشت پستی'].astype('int64')
    df_gasht['کد اداره امور مالیاتی'] = df_gasht[
        'کد اداره امور مالیاتی'].astype('int64')
    df_gasht = df_gasht.astype('str')
    drop_into_db('tblGashtPosti',
                 df_gasht.columns.tolist(),
                 df_gasht.values.tolist(),
                 append_to_prev=False)


def save_process(driver, path):
    global DOWNLOADED_FILES

    save = driver.find_element(By.ID, 'StiWebViewer1_SaveLabel')

    if (save.is_displayed()):
        actions = ActionChains(driver)
        actions.move_to_element(save).perform()
        hidden_submenu = driver.find_element(
            By.XPATH, '/html/body/form/div[3]/span/div[1]/div/table/tbody/tr/td[2]/table/tbody/tr/td[3]/div/div[2]/div/table/tbody/tr/td/table[12]/tbody/tr/td[5]')
        actions.move_to_element(hidden_submenu).perform()
        hidden_submenu.click()

        element = WebDriverWait(driver, 60).until(EC.presence_of_element_located(
            (By.ID, "StiWebViewer1_StiWebViewer1ExportDataOnly")))
        element.click()

        element = WebDriverWait(driver, 60).until(EC.presence_of_element_located(
            (By.ID, "StiWebViewer1_StiWebViewer1ExportObjectFormatting")))
        element.click()
        time.sleep(3)
        element = WebDriverWait(driver, 60).until(EC.presence_of_element_located(
            (By.XPATH, "/html/body/form/div[3]/span/table[1]/tbody/tr/td/table/tbody/tr[2]/td[2]/table/tbody/tr[6]/td/table/tbody/tr/td[1]/table/tbody/tr/td[2]")))
        time.sleep(1)
        element.click()


def scrape_1000_helper(driver):
    driver.find_element(
        By.XPATH,
        '/html/body/form/header/div[2]/div/ul/li[2]/span/span').click()
    time.sleep(1)
    driver.find_element(By.XPATH, '//*[@id="t_MenuNav_1_5i"]').click()
    time.sleep(1)
    driver.find_element(
        By.XPATH, '//*[@id="t_MenuNav_1_5_2i"]').click()

    WebDriverWait(driver, 8).until(
        EC.presence_of_element_located((By.ID, 'select2-P377_NUMBER-container')))
    driver.find_element(
        By.ID, 'select2-P377_NUMBER-container').click()

    WebDriverWait(driver, 8).until(
        EC.presence_of_element_located((By.XPATH, '/html/body/span/span/span[1]/input')))
    driver.find_element(
        By.XPATH, '/html/body/span/span/span[1]/input').send_keys('1000')
    time.sleep(1)
    driver.find_element(
        By.XPATH, '/html/body/span/span/span[1]/input').send_keys(Keys.RETURN)

    time.sleep(1)
    for year in ['1396', '1397', '1398', '1399']:
        WebDriverWait(driver, 8).until(
            EC.presence_of_element_located((By.ID, 'select2-P377_TAX_YEAR-container')))
        driver.find_element(
            By.ID, 'select2-P377_TAX_YEAR-container').click()
        WebDriverWait(driver, 8).until(
            EC.presence_of_element_located((By.XPATH, '/html/body/span/span/span[1]/input')))
        driver.find_element(
            By.XPATH, '/html/body/span/span/span[1]/input').send_keys(year)
        driver.find_element(
            By.XPATH, '/html/body/span/span/span[1]/input').send_keys(Keys.RETURN)

        for item in ['مالیات بر درآمد شرکت ها', 'مالیات بر درآمد مشاغل']:
            WebDriverWait(driver, 8).until(
                EC.presence_of_element_located((By.ID, 'select2-P377_TAX_TYPE-container')))
            driver.find_element(
                By.ID, 'select2-P377_TAX_TYPE-container').click()
            WebDriverWait(driver, 8).until(
                EC.presence_of_element_located((By.XPATH, '/html/body/span/span/span[1]/input')))
            driver.find_element(
                By.XPATH, '/html/body/span/span/span[1]/input').send_keys(item)
            driver.find_element(
                By.XPATH, '/html/body/span/span/span[1]/input').send_keys(Keys.RETURN)
            driver.find_element(
                By.ID, 'B151566902969448513').click()
            time.sleep(0.4)
            try:
                element = driver.find_element(
                    By.CLASS_NAME, 'u-Processing-spinner')
                while (element.is_displayed() == True):
                    print("waiting for the login to be completed")

            except Exception as e:
                time.sleep(0.5)
                driver.find_element(
                    By.ID, 'B480700311704515012').click()
                time.sleep(0.4)
                print(e)
                try:
                    element = driver.find_element(
                        By.CLASS_NAME, 'u-Processing-spinner')
                    while (element.is_displayed() == True):
                        print("waiting for the download to complete")

                except Exception as e:

                    time.sleep(3)

    time.sleep(10)
    driver.close()


def create_1000parvande_report(path, file_name, df, save_dir, *args, **kwargs):

    file_name = os.path.join(path, file_name)

    def is_ghatee(num):
        if len(num) > 6:
            return 'yes'
        else:
            return 'no'

    df['ghatee'] = df['شماره برگ قطعی'].apply(
        lambda x: is_ghatee(x))

    # df.to_excel(save_dir)
    cols = ['سال', 'منبع', 'اداره', 'درصد قطعی شده',
            'تعداد', 'تعداد قطعی شده']
    df_g = df.groupby(
        ['سال عملکرد', 'منبع مالیاتی', 'نام اداره فعلی'])
    lst = []

    df_to_excelsheet(file_name, df_g, index='', names=[
        'اداره ی امور مالیاتی', 'مالیات بر درآمد'])

    for key, item in df_g:
        ls = []
        count = item['ghatee'].count()
        yes_count = item['ghatee'][item['ghatee'] == 'yes'].count()
        ls.append(key[0])
        ls.append(key[1])
        ls.append(key[2])
        ls.append(yes_count/count)
        ls.append(count)
        ls.append(yes_count)
        lst.append(ls)

    new_df = pd.DataFrame(lst, columns=cols)

    p_df_1 = pd.pivot_table(new_df, values=['درصد قطعی شده', 'تعداد قطعی شده', 'تعداد'], aggfunc=sum,
                            index='اداره', columns=['سال', 'منبع'],
                            fill_value=0, margins=True, margins_name='جمع کلی')

    p_df_1.to_excel(
        os.path.join(path, 'final_agg_pv.xlsx'))


def get_dadrasi_new(driver=None, path=None, df=None, saving_dir=None, *args, **kwargs):

    if not os.path.exists(saving_dir):
        os.makedirs(saving_dir)

    WebDriverWait(driver, 20).until(
        EC.presence_of_element_located((
            By.XPATH, "//a[contains(@href, 'edadrasi.tax.gov.ir')]"
        ))).click()

    import time
    time.sleep(6)

    WebDriverWait(driver, 20).until(
        EC.presence_of_element_located((
            By.XPATH,
            "//p[text()='مشاهده وضعیت اعتراضات']"
        ))).click()

    time.sleep(4)

    WebDriverWait(driver, 20).until(
        EC.presence_of_element_located((
            By.XPATH, "/html/body/div[1]/div/main/div[2]/div/div[2]/div[1]/div/div/div[5]/div/div/div/div[1]/button/span[1]"
        ))).click()
    headers = []
    time.sleep(30)

    WebDriverWait(driver, 20).until(
        EC.presence_of_element_located((
            By.XPATH, "//div[@role='combobox' and text()='10']"
        ))).click()

    time.sleep(3)

    WebDriverWait(driver, 20).until(
        EC.presence_of_element_located((
            By.XPATH, "//li[@data-value='50']"
        ))).click()
    time.sleep(3)

    append_to_prev = False

    time.sleep(3)

    while (True):

        # Get page source after table is loaded
        html = driver.page_source
        soup = BeautifulSoup(html, "html.parser")

        # Locate the table
        table = soup.select_one("table.MuiTable-root")

        # Extract headers
        headers = [th.get_text(strip=True) for th in table.select(
            "thead th") if th.get_text(strip=True)]

        # Extract rows
        rows_data = []
        for row in table.select("tbody tr"):
            cells = row.select("td")
            row_data = []
            for cell in cells[1:]:  # skip first column
                div = cell.find("div")
                time = cell.find("time")
                link = cell.find("a")

                if div and div.get("aria-label"):
                    text = div["aria-label"]
                elif div:
                    text = div.get_text(strip=True)
                elif time:
                    text = time.get_text(strip=True)
                elif link:
                    text = link.get("href", "").strip()
                else:
                    text = cell.get_text(strip=True)

                row_data.append(text)
            rows_data.append(row_data)

        df = pd.DataFrame(rows_data, columns=headers)
        df['تاریخ بروزرسانی'] = get_update_date()
        drop_into_db('tbldadrasi', df.columns.tolist(),
                     df.values.tolist(), append_to_prev=append_to_prev)
        append_to_prev = True

        try:
            next_btn = driver.find_element(
                By.CSS_SELECTOR, 'div.MuiTablePagination-actions button[aria-label="رفتن به صفحه‌ی بعدی"]')
            if "Mui-disabled" in next_btn.get_attribute("class"):
                break
            next_btn.click()
            import time
            time.sleep(3)
        except Exception as e:
            print(e)
            break


def get_modi_info(driver=None, path=None, df=None, saving_dir=None, *args, **kwargs):

    if not os.path.exists(saving_dir):
        os.makedirs(saving_dir)

    WebDriverWait(driver, 8).until(
        EC.presence_of_element_located((
            By.XPATH,
            '/html/body/div/div[3]/div[2]/div/div/div[1]/div/div[2]/div/a'
        )))
    driver.find_element(
        By.XPATH,
        '/html/body/div/div[3]/div[2]/div/div/div[1]/div/div[2]/div/a'
    ).click()

    lst_names = []

    for index, row in df.iterrows():
        try:
            print(f'Iteration no {index}')
            print(
                '***************************************************************************************')
            row = str(row.values[0])
            if len(row) == 8:
                row = '00' + row
            elif len(row) == 9:
                row = '0' + row
            modi = Modi()
            WebDriverWait(driver, 8).until(
                EC.presence_of_element_located((
                    By.ID,
                    'TextboxPublicSearch'
                )))
            driver.find_element(
                By.ID,
                'TextboxPublicSearch'
            ).send_keys(row)

            WebDriverWait(driver, 8).until(
                EC.presence_of_element_located((
                    By.XPATH,
                    '/html/body/form/table/tbody/tr[1]/td[2]/div/div/table/tbody/tr[2]/td[2]/a/span'
                )))
            driver.find_element(
                By.XPATH,
                '/html/body/form/table/tbody/tr[1]/td[2]/div/div/table/tbody/tr[2]/td[2]/a/span'
            ).click()

            # WebDriverWait(driver, 8).until(
            #     EC.presence_of_element_located((
            #         By.XPATH,
            #         '/html/body/form/table/tbody/tr[2]/td[2]/span/table[1]/tbody/tr[2]/td[8]/a/span'
            #     ))).click()

            # WebDriverWait(driver, 8).until(
            #     EC.presence_of_element_located((
            #         By.XPATH,
            #         '/html/body/form/table/tbody/tr[2]/td[2]/span[3]/table[2]/tbody/tr/td/div/div[1]/table[1]/tbody/tr/td[3]/table/tbody/tr[11]/td/a/div'
            #     ))).click()

            # try:
            #     modi_pazirande = Modi_pazerande()
            #     modi_pazirande.melli = row
            #     time.sleep(1)
            #     if (WebDriverWait(driver, 8).until(
            #             EC.presence_of_element_located((
            #                 By.XPATH,
            #                 '/html/body/form/table/tbody/tr[2]/td[2]/span[3]/table/tbody'
            #             )))):

            #         elm = driver.find_element(
            #             By.XPATH,
            #             '/html/body/form/table/tbody/tr[2]/td[2]/span[3]/table/tbody'
            #         )

            #         rws = elm.find_elements(By.TAG_NAME, 'tr')[1:-1]

            #         for item in rws:
            #             modi_pazirande = Modi_pazerande()
            #             tds = item.find_elements(By.TAG_NAME, 'td')
            #             modi_pazirande.melli = row
            #             modi_pazirande.hoviyati = tds[0].text
            #             modi_pazirande.code = tds[3].text
            #             modi_pazirande.shomarepayane = tds[4].text
            #             modi_pazirande.vaziat = tds[5].text
            #             modi_pazirande.vaziatelsagh = tds[6].text
            #             modi_pazirande.money1400 = tds[9].text
            #             modi_pazirande.money1401 = tds[10].text
            #             lst_names.append([modi_pazirande.melli,
            #                               modi_pazirande.hoviyati,
            #                               modi_pazirande.code,
            #                               modi_pazirande.shomarepayane,
            #                               modi_pazirande.vaziat,
            #                               modi_pazirande.vaziatelsagh,
            #                               modi_pazirande.money1400,
            #                               modi_pazirande.money1401])

            #             if (len(lst_names) % 10 == 0):

            #                 columns = ['شناسه هویتی پذیرنده', 'کد پذیرنده فروشگاهی', 'شماره پایانه',
            #                            'وضعیت پذیرنده', 'وضعیت پذیرنده', 'وضعیت الصاق',
            #                            'مجموع گردش حسابهای حقیقی و حقوقی عملکرد سال 1400',
            #                            'مجموع گردش حسابهای حقیقی و حقوقی عملکرد سال 1401'
            #                            ]

            #                 final_df = pd.DataFrame(
            #                     lst_names[-10:], columns=columns)

            #                 file_name = os.path.join(
            #                     saving_dir, 'last10of%s.xlsx' % len(lst_names))

            #                 final_df.to_excel(file_name)

            # except:
            #     lst_names.append([row, 'None',
            #                       'None', 'None', 'None', 'None', 'None', 'None'])

            #     if (len(lst_names) % 10 == 0):

            #         columns = ['شناسه هویتی پذیرنده', 'کد پذیرنده فروشگاهی', 'شماره پایانه',
            #                    'وضعیت پذیرنده', 'وضعیت پذیرنده', 'وضعیت الصاق',
            #                    'مجموع گردش حسابهای حقیقی و حقوقی عملکرد سال 1400',
            #                    'مجموع گردش حسابهای حقیقی و حقوقی عملکرد سال 1401'
            #                    ]

            #         final_df = pd.DataFrame(lst_names[-10:], columns=columns)

            #         file_name = os.path.join(
            #             saving_dir, 'last10of%s.xlsx' % len(lst_names))

            #         final_df.to_excel(file_name)
        except Exception as e:
            print(e)
            continue
        # modi_pazirande.hoviyati = driver.find_element(
        #     By.XPATH,
        #     '/html/body/form/table/tbody/tr[2]/td[2]/span/table/tbody/tr[2]/td[1]'
        # ).text

        time.sleep(0.5)
        try:

            modi.melli = driver.find_element(
                By.XPATH,
                '/html/body/form/table/tbody/tr[2]/td[2]/span/table/tbody/tr[2]/td[1]'
            ).text

            modi.sex = driver.find_element(
                By.XPATH,
                '/html/body/form/table/tbody/tr[2]/td[2]/span/table/tbody/tr[2]/td[2]'
            ).text

            modi.name = driver.find_element(
                By.XPATH,
                '/html/body/form/table/tbody/tr[2]/td[2]/span/table/tbody/tr[2]/td[3]'
            ).text

            modi.father_name = driver.find_element(
                By.XPATH,
                '/html/body/form/table/tbody/tr[2]/td[2]/span/table/tbody/tr[2]/td[4]'
            ).text

            modi.dob = driver.find_element(
                By.XPATH,
                '/html/body/form/table/tbody/tr[2]/td[2]/span/table/tbody/tr[2]/td[5]'
            ).text

            modi.id_num = driver.find_element(
                By.XPATH,
                '/html/body/form/table/tbody/tr[2]/td[2]/span/table/tbody/tr[2]/td[6]'
            ).text

            lst_names.append([modi.melli, modi.sex, modi.name,
                              modi.father_name, modi.dob, modi.id_num])

        except Exception as e:
            lst_names.append([row, 'None',
                             'None', 'None', 'None', 'None'])
            continue

        if (len(lst_names) % 10 == 0):

            columns = ['کد ملی', 'جنسیت', 'نام',
                       'نام پدر', 'تاریخ تولد', 'شماره شناسنامه']

            final_df = pd.DataFrame(lst_names[-10:], columns=columns)

            file_name = os.path.join(
                saving_dir, 'last10of%s.xlsx' % len(lst_names))

            final_df.to_excel(file_name)


# @wrap_a_wrapper
@wrap_it_with_paramsv1(2, 10, True, False, False, True)
def check_if_shenase_exists(driver, info):
    WebDriverWait(driver, 4).until(
        EC.element_to_be_clickable(
            (By.XPATH, '/html/body/table/tbody/tr/td/div[1]/div[2]/table/tbody/tr[4]/\
                td/table/tbody/tr[2]/td/div/div[2]/div/div[2]/table/tbody/tr/td[2]/div')))
    return driver, info


def check_if_value_inserted(driver, item, path='/html/body/table/tbody/tr/td/div[1]/\
    div[2]/table/tbody/tr[3]/td/table/tbody/tr[2]/td[2]/div/input'):
    if (WebDriverWait(driver, 2).until(
        EC.element_to_be_clickable(
            (By.XPATH,
             path))).
            get_attribute('value') == str(int(item))):
        return driver
    else:
        raise Exception


@wrap_it_with_paramsv1(2, 10, True, False, False, True)
def table_dadrasi_exists(driver, info):
    table_rows = WebDriverWait(driver, 8).until(
        EC.element_to_be_clickable(
            (By.XPATH,
             "/html/body/table/tbody/tr/td/div[1]/div[2]/table/tbody/tr[4]/td/\
                 table/tbody/tr[2]/td/div/div[2]/div/div[2]/table/tbody/tr")))
    return driver, info

# @wrap_a_wrapper


@wrap_it_with_paramsv1(10, 10, True, False, False, True)
def insert_value(driver, item, info, path='/html/body/table/tbody/tr/td/div[1]/div[2]/table/tbody/tr[3]/\
                 td/table/tbody/tr[2]/td[2]/div/input'):
    item = item['shomare']
    # item = "14003242040"
    WebDriverWait(driver, 2).until(
        EC.element_to_be_clickable(
            (By.XPATH,
             path))).clear()
    time.sleep(2)
    WebDriverWait(driver, 2).until(
        EC.element_to_be_clickable(
            (By.XPATH,
             path))).send_keys(str(item))

    driver = check_if_value_inserted(driver, item, path)

    return driver, info


@wrap_it_with_paramsv1(4, 5, True, False, False, False)
def get_info_shenase_dadrasi(driver, info):
    info['shenase_dadrasi_num'] = WebDriverWait(driver, 3).until(
        EC.presence_of_element_located(
            (By.XPATH, '/html/body/table/tbody/tr/td/div[1]/div[2]/table/tbody/tr[4]/\
                td/table/tbody/tr[2]/td/div/div[2]/div/div[2]/table/tbody/tr/td[22]/div'))).text
    info['vaziat'] = WebDriverWait(driver, 3).until(
        EC.presence_of_element_located(
            (By.XPATH, '/html/body/table/tbody/tr/td/div[1]/div[2]/table/tbody/\
                tr[4]/td/table/tbody/tr[2]/td/div/div[2]/div/div[2]/table/tbody/tr/td[6]/div'))).text
    info['assigned'] = driver.find_element(
        By.XPATH, '/html/body/table/tbody/tr/td/div[1]/div[2]/table/tbody/tr[4]/td/table/tbody/tr[2]\
            /td/div/div[2]/div/div[2]/table/tbody/tr/td[8]/div').text

    return driver, info


@wrap_it_with_paramsv1(8, 10, True, False, False, True)
def click_on_search_btn(driver, info, path='/html/body/table/tbody/tr/td/div[1]/div[1]/div/div[1]/\
                ul/li[4]/div/div[1]/div[2]/input'):
    WebDriverWait(driver, 2).until(
        EC.element_to_be_clickable(
            (By.XPATH, path))).click()

    while (True):
        try:
            if (WebDriverWait(driver, 1).until(
                EC.presence_of_element_located(
                    (By.XPATH, "/html/body/div[13]")))):
                time.sleep(2)
                print("waiting")
                continue
        except:
            time.sleep(1)
            break

    return driver, info


def check_success(info):
    for k, v in info.items():
        if isinstance(v, bool):
            if not v:
                info['success'] = False
                return info
    return info

# @wrap_a_wrapper


# @wrap_a_wrapper
@wrap_it_with_paramsv1(2, 10, True, False, False, True)
def clear_and_send_keys(driver, item, info, role='manager_phase1'):
    if not info['success']:
        raise Exception

    driver, info = get_info_shenase_dadrasi(driver=driver, info=info)

    if not info['success']:
        raise Exception

    driver, info = check_if_shenase_exists(driver=driver, info=info)

    if not info['success']:
        raise Exception

    # info['success'] = False if len(shenase_dadrasi_num) == 1 else True

    return driver, info


# @wrap_a_wrapper
@wrap_it_with_paramsv1(5, 10, True, False, False, True)
def select_row(driver, info):
    WebDriverWait(driver, 7).until(
        EC.element_to_be_clickable(
            (By.XPATH, '/html/body/table/tbody/tr/td/div[1]/div[2]/table/tbody/\
                tr[4]/td/table/tbody/tr[2]/td/div/div[2]/div/div[2]/table/tbody/tr/td[8]/div'))).click()

    time.sleep(2)
    return driver, info


# @wrap_a_wrapper
@wrap_it_with_paramsv1(12, 12, True, False, False, False)
def assign_btn(driver, role='manager_phase2', shenase_dadrasi='no',
               info={'success': True, 'keep_alive': True}):

    if (role == 'manager_phase2'):
        try:
            element = WebDriverWait(driver, 1).until(
                EC.element_to_be_clickable(
                    (By.XPATH, '/html/body/table/tbody/tr/td/div[1]/\
                        div[1]/div/div[1]/ul/li[10]/div/div[1]/div[2]/input')))
            if element.get_attribute("title") == 'پردازش':
                element.click()
            try:
                driver.switch_to.default_content()
                WebDriverWait(driver, 1).until(
                    EC.element_to_be_clickable(
                        (By.XPATH, '//*[@id="flexPopupOKMsgBtn"]'))).click()
                frame = WebDriverWait(driver, 8).until(
                    EC.element_to_be_clickable(
                        (By.XPATH,
                         '/html/body/div[1]/div[1]/div/div/div/div/div/iframe[2]')))
                driver.switch_to.frame(frame)
                element.click()
            except:
                ...
        except:
            ...

    if (role == 'employee' and info['vaziat'] == 'باز'):
        try:
            element = WebDriverWait(driver, 2).until(
                EC.element_to_be_clickable(
                    (By.XPATH, '/html/body/table/tbody/tr/td/div[1]/\
                            div[1]/div/div[1]/ul/li[11]/div/div[1]/div[2]/input')))
            if element.get_attribute("title") == 'دریافت':
                element.click()
        except:
            ...

    elif (role == 'employee' and info['vaziat'] == 'ثبت شده توسط خدمات مودیان'):
        WebDriverWait(driver, 1).until(
            EC.element_to_be_clickable(
                (By.XPATH, '/html/body/table/tbody/tr/td/div[1]/\
                    div[1]/div/div[1]/ul/li[10]/div/div[1]/div[2]/input'))).click()

    elif (role == 'manager_phase1'):
        if (info['vaziat'] in ['ثبت شده توسط خدمات مودیان', 'باز']):
            try:
                element = WebDriverWait(driver, 1).until(
                    EC.element_to_be_clickable(
                        (By.XPATH, '/html/body/table/tbody/tr/td/div[1]/\
                            div[1]/div/div[1]/ul/li[6]/div/div[1]/div[2]/input')))
                if element.get_attribute("title") == 'تخصیص/تخصیص مجدد':
                    element.click()
            except:
                info['success'] = False
                return driver, info
        elif (info['vaziat'] == 'ثبت شده'):
            try:
                element = WebDriverWait(driver, 1).until(
                    EC.element_to_be_clickable(
                        (By.XPATH, '/html/body/table/tbody/tr/td/div[1]/div[1]/div/div[1]/ul/\
                            li[10]/div/div[1]/div[2]/input')))
                if element.get_attribute("title") == 'پردازش':
                    element.click()
            except:
                driver.switch_to.default_content()
                WebDriverWait(driver, 8).until(
                    EC.element_to_be_clickable(
                        (By.XPATH,
                            '//*[@id="flexPopupOKMsgBtn"]'))).click()
                frame = WebDriverWait(driver, 8).until(
                    EC.element_to_be_clickable(
                        (By.XPATH,
                            '/html/body/div[1]/div[1]/div/div/div/div/div/iframe[2]')))
                driver.switch_to.frame(frame)
                element.click()
                info['success'] = True
                return driver, info

    elif (role == 'employee' and shenase_dadrasi == 'no'):
        WebDriverWait(driver, 1).until(
            EC.element_to_be_clickable(
                (By.XPATH, '/html/body/table/tbody/tr/td/div[1]/\
                    div[1]/div/div[1]/ul/li[11]/div/div[1]/div[2]/input'))).click()

    elif (role == 'employee' or shenase_dadrasi == 'yes'):
        WebDriverWait(driver, 1).until(
            EC.element_to_be_clickable(
                (By.XPATH, '//html/body/table/tbody/tr/td/div[1]/div[1]/div/div[1]/\
                    ul/li[10]/div/div[1]/div[2]/input'))).click()

    time.sleep(1)
    return driver, info


# @wrap_a_wrapper
@wrap_it_with_paramsv1(12, 30, True, False, False, True)
def get_info(driver, role, info):

    info['data'] = driver.find_element(
        By.XPATH, '/html/body/table/tbody/tr/td/div[1]/div[2]/table/tbody/tr[4]/td/table/tbody/tr[2]\
            /td/div/div[2]/div/div[2]/table/tbody/tr/td[8]/div').text
    info['vaziat'] = driver.find_element(
        By.XPATH, '/html/body/table/tbody/tr/td/div[1]/div[2]/table/tbody/tr[4]/td/table/tbody\
            /tr[2]/td/div/div[2]/div/div[2]/table/tbody/tr/td[6]/div').text
    return driver, info


# @wrap_a_wrapper
@wrap_it_with_paramsv1(1, 60, False, False, False)
def save_excel(index, df, done='yes', success='yes',
               driver=None, second_phase='no', role='manager_phase2', shenase_dadrasi='no',
               shenase_dadrasi_num=None, vaziat='', info={}):

    df[0].loc[index, 'is_done'] = done
    df[0].loc[index, 'success'] = success
    df[0].loc[index, 'assigned_to'] = str(
        df[1]['employee'].head(1).item())
    df[0].loc[index, 'shenase_dadrasi'] = shenase_dadrasi
    df[0].loc[index, 'shenase_dadrasi_no'] = shenase_dadrasi_num
    df[0].loc[index, 'second_phase'] = second_phase
    df[0].loc[index, 'vaziat'] = vaziat
    remove_excel_files(files=[file_name],
                       postfix='xlsx')
    df[0].to_excel(
        file_name, index=False)

    return driver, info


# @wrap_a_wrapper
@wrap_it_with_paramsv1(11, 3, True, False, False, False)
def go_to_next_frame(driver, role='manager_phase2', shenase_dadrasi='no', info={}):
    if (role == 'manager_phase2'):
        ...
        # WebDriverWait(driver, 1).until(
        #     EC.element_to_be_clickable(
        #         (By.XPATH, '/html/body/table/tbody/tr/td/div[1]/div[1]/div/div[1]/ul/li[6]/div/div[1]/div[2]/input'))).click()
    time.sleep(4)
    driver.switch_to.default_content()
    frame = driver.find_element(
        By.XPATH, '/html/body/div[1]/div[1]/div/div/div/div/div/iframe[3]')
    driver.switch_to.frame(frame)

    return driver, info


# @wrap_a_wrapper
@wrap_it_with_paramsv1(1, 20, True, False, False, True)
def handle_error(driver, info={}):
    while True:
        info['fraud'] = 'nofraud'
        try:
            driver.switch_to.default_content()

            if (WebDriverWait(driver, 2).until(
                    EC.element_to_be_clickable(
                    (By.XPATH, '//*[@id="flexPopupErrMsgBtn"]')))):
                info['msg'] = driver.find_element(
                    By.XPATH, '/html/body/div[8]/p/a').text
                driver.find_element(
                    By.XPATH, '//*[@id="flexPopupErrMsgBtn"]').click()
                if info['msg'] == 'مبلغ اعتراض نمیتواند از مبلغ جاري کل فراتر رود':
                    try:
                        WebDriverWait(driver, 2).until(
                            EC.element_to_be_clickable(
                                (By.XPATH, '/html/body/table/tbody/tr/td/div[1]/div[2]/table/\
                            tbody/tr[4]/td/table/tbody/tr[2]/td/table/\
                            tbody/tr[3]/td/table/tbody/tr[2]/td/div/div[2]\
                                /div/div[2]/table/tbody/tr/td[10]/div'))).text
                    except:
                        ...
                time.sleep(2)
                driver.find_element(
                    By.XPATH, '/html/body/div[2]/div[3]/table/tbody/tr/td/ul/li[3]/a[2]').click()

                time.sleep(1)

                info['fraud'] = 'isfraud'

                frame = driver.find_element(
                    By.XPATH, '/html/body/div[1]/div[1]/div/div/div/div/div/iframe[2]')
                driver.switch_to.frame(frame)
                return driver, info
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
                info['fraud'] = 'isfraud'
                return driver, info
            except:
                return driver, info
        except:

            return driver, info


# @wrap_a_wrapper
@wrap_it_with_paramsv1(1, 20, True, False, False, False)
def if_apply_new_assignment(driver, role='manager_phase2', info={}):
    if role == 'manager_phase2':
        try:
            try:
                WebDriverWait(driver, 2).until(
                    EC.presence_of_element_located(
                        (By.XPATH,
                            '/html/body/table/tbody/tr/td/div[1]/div[2]/table/tbody/tr[8]/td/table/tbody/\
                                tr[2]/td[4]/div/input[2]')))
            except:
                return driver, info
            raise Exception
        except:
            raise Exception

    else:
        try:
            try:
                j = 0
                while (WebDriverWait(driver, 2).until(
                    EC.presence_of_element_located(
                        (By.XPATH,
                         '/html/body/table/tbody/tr/td/div[1]/div[2]/table/tbody/tr[6]/td/table/tbody/tr[2]/td/table/\
                             tbody/tr[10]/td[2]/div/input')))):
                    time.sleep(1)
                    j += 1
                    if j < 10:
                        continue
                    break
            except:
                return driver, info
            raise Exception
        except Exception:
            raise Exception

    # if text == 'مورد اعتراض/شکایت':
    #     raise Exception


# @wrap_a_wrapper
@wrap_it_with_paramsv1(1, 20, True, False, True, False)
def apply_new_assignment(driver, role='manager_phase2', info={}):
    if role == 'manager_phase1':
        xpath = '/html/body/table/tbody/tr/td/div[1]/div[1]/div/div[1]/ul/li[2]/div/div[1]/div[2]/input'
    elif role == 'manager_phase2':
        xpath = '/html/body/table/tbody/tr/td/div[1]/div[1]/div/div[1]/ul/li[2]/div/div[1]/div[2]/input'
    else:
        xpath = '/html/body/table/tbody/tr/td/div[1]/div[1]/div/div[1]/ul/li[4]/div/input'
    # WebDriverWait(driver, 1).until(
    #     EC.element_to_be_clickable(
    #         (By.XPATH,
    #          xpath))).click()
    try:
        if role == 'employee':
            path_jari = '/html/body/table/tbody/tr/td/div[1]/div[2]/table/tbody/tr[4]/td/table/tbody\
                /tr[2]/td/table/tbody/tr[3]/td/table/tbody/tr[2]\
                /td/div/div[2]/div/div[2]/table/tbody/tr/td[10]/div'
            path_eterazi = '/html/body/table/tbody/tr/td/div[1]/div[2]/table/\
                tbody/tr[4]/td/table/tbody/tr[2]/td/table/tbody/tr[3]/td/table/\
                tbody/tr[2]/td/div/div[2]/div/div[2]/table/tbody/tr/td[11]/div'
            mab_eterazi = float(WebDriverWait(driver, 16).until(
                EC.element_to_be_clickable(
                    (By.XPATH, path_eterazi
                     ))).text.replace(',', ''))
            mab_jari = float(WebDriverWait(driver, 16).until(
                EC.element_to_be_clickable(
                    (By.XPATH,
                     path_jari))).text.replace(',', ''))
            if mab_eterazi > mab_jari:
                # Locate the element on which you want to perform the double click
                element = driver.find_element(By.XPATH, path_eterazi)
                element.click()
                element = driver.find_element(By.XPATH, '/html/body/table/tbody/tr/td/div[1]/div[2]/table/\
                    tbody/tr[4]/td/table/tbody/tr[2]/td/table/tbody/tr[3]/td/table/tbody/tr[2]\
                    /td/div/div[2]/div/div[1]/table/tbody/tr[1]/td[11]/div/div/div/input').send_keys(mab_jari)
                # Create an ActionChains object and perform a double click on the element

        WebDriverWait(driver, 1).until(
            EC.element_to_be_clickable(
                (By.CLASS_NAME,
                 'submit_icon'))).click()
    except:
        ...

    try:

        element = WebDriverWait(driver, 2).until(
            EC.element_to_be_clickable(
                (By.XPATH,
                 '/html/body/table/tbody/tr/td/div[1]/div[1]/\
                    div/div[2]/ul/li[4]/div/div[1]/div[2]/input'))).click()
        if element.get_attribute("title") == 'پذیرش':
            element.click()
    except:
        ...

    time.sleep(2)
    driver, info = handle_error(driver=driver, info=info)
    if info['fraud'] == 'isfraud':
        # return driver, fraud
        raise Exception
    driver, info = if_apply_new_assignment(driver=driver, role=role, info=info)
    if not info['success']:
        raise Exception
    return driver, info


# @wrap_a_wrapper
@wrap_it_with_paramsv1(2, 20, True, False, False, False)
def click_submit_btn(driver, info):

    WebDriverWait(driver, 8).until(
        EC.element_to_be_clickable(
            (By.XPATH,
                '/html/body/table/tbody/tr/td/div[1]/div[1]/div/div[2]/ul/li[4]/div/div[1]/div[2]/input'))).click()
    # while (True):

    #     i = 0
    #     while (len(driver.window_handles) == 1):
    #         time.sleep(1)
    #         i += 1
    #         print('waiting')
    #         if i > 120:
    #             raise Exception
    #         continue
    #     time.sleep(4)
    #     # Close the child window
    #     driver.switch_to.window(driver.window_handles[1])
    #     driver.close()
    #     # Switch to the main window
    #     driver.switch_to.window(driver.window_handles[0])
    #     break
    return driver, info


@wrap_it_with_paramsv1(8, 20, True, False, False, False)
def insert_assignment(driver, info, item):
    elm = WebDriverWait(driver, 4).until(
        EC.element_to_be_clickable(
            (By.XPATH,
             '/html/body/table/tbody/tr/td/div[1]/div[2]\
                                 /table/tbody/tr[9]/td/table/tbody/\
                                     tr[2]/td/table/tbody/tr[2]/td[2]/div/input[1]')))
    elm.click()
    time.sleep(3)
    elm.clear()
    time.sleep(2)
    elm.send_keys(item['assigned_to'])
    time.sleep(2)
    try:
        while (WebDriverWait(driver, 8).until(
            EC.element_to_be_clickable(
                (By.XPATH,
                 ('//*[@title="ارسال"]')))).is_displayed()):
            WebDriverWait(driver, 8).until(
                EC.element_to_be_clickable(
                    (By.XPATH,
                     ('//*[@title="ارسال"]')))).click()
            time.sleep(3)
    except:
        ...

    return driver, info


# @wrap_a_wrapper
@wrap_it_with_paramsv1(8, 20, True, False, False, False)
def insert_new_assignment(driver,
                          df,
                          index,
                          role='manager_phase2',
                          item=None,
                          shenase_dadrasi='no',
                          file_name=None,
                          backup_file=None,
                          info={}):

    if ((role == 'manager_phase2') or ((role == 'manager_phase1') and info['vaziat'] == 'ثبت شده')):
        try:
            i = 0
            while (i < 1):
                driver, info = click_submit_btn(driver=driver, info=info)
                try:
                    while (WebDriverWait(driver, 4).until(
                        EC.element_to_be_clickable(
                            (By.XPATH,
                             '/html/body/div[14]'))).is_displayed()):
                        time.sleep(1)
                        print('waiting')
                except:
                    ...
                driver.switch_to.default_content()
                if (WebDriverWait(driver, 8).until(
                    EC.element_to_be_clickable(
                        (By.XPATH, '//*[@id="flexPopupErrMsgBtn"]')))):
                    info['msg'] = WebDriverWait(driver, 2).until(
                        EC.element_to_be_clickable(
                            (By.XPATH,
                                '/html/body/div[8]/p'))).text
                    driver.find_element(
                        By.XPATH, '//*[@id="flexPopupErrMsgBtn"]').click()
                    driver.switch_to.default_content()
                    frame = driver.find_element(
                        By.XPATH, '/html/body/div[1]/div[1]/div/div/div/div/div/iframe[3]')
                    driver.switch_to.frame(frame)
                i += 1
                if i == 1:
                    raise Exception
        except:
            driver.switch_to.default_content()
            if info['msg'] == "خطا در ارتباط با سامانه دادرسی":
                WebDriverWait(driver, 2).until(
                    EC.element_to_be_clickable(
                        (By.XPATH,
                            '/html/body/div[2]/div[3]/table/tbody/tr/td/ul/li[3]/a[2]'))).click()
            frame = WebDriverWait(driver, 2).until(
                EC.element_to_be_clickable(
                    (By.XPATH,
                        '/html/body/div[1]/div[1]/div/div/div/div/div/iframe[2]')))
            driver.switch_to.frame(frame)
            driver, info = get_info_shenase_dadrasi(driver=driver, info=info)
            # Close
            # # info['success'] = False
            # info['message'] = message
            # frame = driver.find_element(
            #     By.XPATH, '/html/body/div[1]/div[1]/div/div/div/div/div/iframe[2]')
            return driver, info

        return driver, info

        # driver, info = get_info_shenase_dadrasi(driver=driver, info=info)

    elif (role == 'manager_phase1'):
        xpath = '/html/body/table/tbody/tr/td/div[1]/div[2]/table/tbody/tr[8]/td/table/tbody/tr[2]/td[4]/div/input[2]'
        input_str = str(df[1]['employee'].head(1).item())
        WebDriverWait(driver, 2).until(
            EC.element_to_be_clickable(
                (By.XPATH,
                    xpath))).click()
        time.sleep(3)
        WebDriverWait(driver, 2).until(
            EC.element_to_be_clickable(
                (By.XPATH,
                    xpath))).send_keys(input_str)

    elif (role == 'manager_phase2' and shenase_dadrasi == 'no'):
        xpath = '/html/body/table/tbody/tr/td/div[1]/div[2]/table/tbody/tr[8]/td/table/tbody/tr[2]/td[4]/div/input[2]'
        input_str = str(df[1]['employee'].head(1).item())

        if not info['success']:
            raise Exception

        return driver, info

    elif (role == 'employee' and info['vaziat'] in ['باز', 'در انتظار تکمیل']):
        try:
            i = 0
            while (i < 4):
                WebDriverWait(driver, 2).until(
                    EC.element_to_be_clickable(
                        (By.XPATH,
                         '/html/body/table/tbody/tr/td/div[1]/div[1]/div/\
                        div[1]/ul/li[2]/div/div[1]/div[2]/input'))).click()
                try:
                    while (WebDriverWait(driver, 4).until(
                        EC.element_to_be_clickable(
                            (By.XPATH,
                             '/html/body/div[14]'))).is_displayed()):
                        print('waiting')
                except:
                    ...
                driver.switch_to.default_content()
                if (WebDriverWait(driver, 12).until(
                    EC.element_to_be_clickable(
                        (By.XPATH, '//*[@id="flexPopupErrMsgBtn"]')))):
                    info['msg'] = WebDriverWait(driver, 2).until(
                        EC.element_to_be_clickable(
                            (By.XPATH,
                                '/html/body/div[8]/p'))).text
                    driver.find_element(
                        By.XPATH, '//*[@id="flexPopupErrMsgBtn"]').click()
                    driver.switch_to.default_content()
                    frame = driver.find_element(
                        By.XPATH, '/html/body/div[1]/div[1]/div/div/div/div/div/iframe[3]')
                    driver.switch_to.frame(frame)
                i += 1
                if i == 1:
                    driver.switch_to.default_content()
                    WebDriverWait(driver, 2).until(
                        EC.element_to_be_clickable(
                            (By.XPATH,
                                '/html/body/div[2]/div[3]/table/tbody/tr/td/ul/li[3]/a[2]'))).click()
                    frame = WebDriverWait(driver, 2).until(
                        EC.element_to_be_clickable(
                            (By.XPATH,
                                '/html/body/div[1]/div[1]/div/div/div/div/div/iframe[2]')))
                    driver.switch_to.frame(frame)
                    driver, info = get_info_shenase_dadrasi(
                        driver=driver, info=info)
                    raise Exception
        except:
            return driver, info

            # Close
            # # info['success'] = False
            # info['message'] = message
            # frame = driver.find_element(
            #     By.XPATH, '/html/body/div[1]/div[1]/div/div/div/div/div/iframe[2]')
        return driver, info

    elif (role == 'employee' and info['vaziat'] == 'ثبت شده توسط خدمات مودیان'):
        xpath = '/html/body/table/tbody/tr/td/div[1]/div[2]/table/tbody/tr[6]/td/\
            table/tbody/tr[2]/td/table/tbody/tr[10]/td[2]/div/input'
        # input_str = str(item['daramad_tashkhis'])
        input_str = str(0)
        WebDriverWait(driver, 2).until(
            EC.element_to_be_clickable(
                (By.XPATH,
                    xpath))).click()
        while WebDriverWait(driver, 2).until(
            EC.element_to_be_clickable(
                (By.XPATH,
                    xpath))).get_attribute('value') == '':
            WebDriverWait(driver, 2).until(
                EC.element_to_be_clickable(
                    (By.XPATH,
                        xpath))).send_keys(input_str)
            time.sleep(1)
    elif (role != 'employee'):
        WebDriverWait(driver, 2).until(
            EC.element_to_be_clickable(
                (By.XPATH,
                    xpath))).click()
        time.sleep(3)
        WebDriverWait(driver, 2).until(
            EC.element_to_be_clickable(
                (By.XPATH,
                    xpath))).send_keys(input_str)

    time.sleep(2)
    if role != "manager_phase2":
        driver, info = apply_new_assignment(driver, role=role, info=info)

        if info['fraud'] == 'isfraud':
            raise Exception

    # if isinstance(driver, tuple):
    #     driver, success = driver
    #     if isinstance(success, str):
    #         if success == 'isfraud':
    #             raise Exception
    #         return driver, info
    #     raise Exception
    time.sleep(1)

    return driver, info


def check_health(driver,
                 index=None,
                 df=None,
                 item=None,
                 serious=True,
                 role='manager_phase2',
                 init=False,
                 done='no',
                 success='no',
                 second_phase='yes',
                 shenase_dadrasi='yes',
                 table_name='tbltempheiat',
                 info={}):

    if init:
        return driver, info

    if serious:

        driver = login_iris(driver, creds={'username': df[1][role].head(1).item(),
                                           'pass': df[1]['pass'].head(1).item()})
        driver = find_obj_and_click(driver)

        return driver, info
    if (('shenase_dadrasi_num' not in info) and ('msg' not in info) and ('assigned' not in info)):
        info['msg'] = 'یافت نشد'

    if 'shenase_dadrasi_num' not in info:
        info['shenase_dadrasi_num'] = ''
    if 'msg' not in info:
        info['msg'] = ''
    if 'vaziat' not in info:
        info['vaziat'] = ''
    if 'assigned' not in info:
        info['assigned'] = ''
    if len(info['shenase_dadrasi_num']) > 3:
        second_phase = 'yes'
        done = 'yes'
        success = 'yes'
        connect_to_sql(sql_query=f"""UPDATE {table_name} SET is_done='{done}',
                       success='{success}', 
                       second_phase='{second_phase}', 
                       shenase_dadrasi='{shenase_dadrasi}', 
                       shenase_dadrasi_no='{info['shenase_dadrasi_num']}',
                       msg=N'{info['msg']}'
                       WHERE shomare = '{item['shomare']}'""")
        info['success'] = True
        return driver, info

    if (role == 'manager_phase1'):
        second_phase = 'no'
        done = 'yes'
        success = 'yes'
        if info['msg'] == 'یافت نشد':
            second_phase = 'no'
            done = 'no'
            success = 'no'
            connect_to_sql(sql_query=f"""UPDATE {table_name} SET is_done='{done}',
                    success='{success}', 
                    second_phase='{second_phase}', 
                    shenase_dadrasi='{shenase_dadrasi}', 
                    shenase_dadrasi_no='{info['shenase_dadrasi_num']}',
                    msg=N'{info['msg']}'
                    WHERE shomare = '{item['shomare']}'""")
        if info['vaziat'] == '':
            success = 'no'
        if info['assigned'] != df[1]['employee'].head(1).item():
            return driver, info
        if (info['vaziat'] == 'ثبت شده توسط خدمات مودیان' and
                info['assigned'] == df[1]['employee'].head(1).item()):
            second_phase = 'no'
        if (info['assigned'] == '-' and info['vaziat'] == 'ثبت شده توسط خدمات مودیان'):
            return driver, info
        connect_to_sql(sql_query=f"""UPDATE {table_name} SET is_done='{done}',
                    success='{success}', 
                    second_phase='{second_phase}', 
                    shenase_dadrasi='{shenase_dadrasi}', 
                    shenase_dadrasi_no='{info['shenase_dadrasi_num']}',
                    msg=N'{info['msg']}'
                    WHERE shomare = '{item['shomare']}'""")
        info['success'] = True
        return driver, info

    if role == 'employee':
        second_phase = 'yes'
        done = 'yes'
        if info['vaziat'] == 'باز':
            second_phase = 'yes'
        if ((not info['success']) or (info['vaziat'] in ['باز', 'ثبت شده'])):
            connect_to_sql(sql_query=f"""UPDATE {table_name} SET is_done='{done}',
                       success='{success}', 
                       second_phase='{second_phase}', 
                       shenase_dadrasi='{shenase_dadrasi}', 
                       shenase_dadrasi_no='{info['shenase_dadrasi_num']}',
                       msg=N'{info['msg']}'
                       WHERE shomare = '{item['shomare']}'""")
            info['success'] = True

        return driver, info

    if ((info['assigned'] == df[1]['employee'].head(1).item() and role == 'manager_phase1') or
            (info['vaziat'] != 'ثبت شده توسط خدمات مودیان' and role == 'employee') or
            (info['vaziat'] != 'ثبت شده' and role == 'manager_phase2')):
        success = 'yes'
        done = 'yes'
        second_phase = 'yes'
        if len(info['shenase_dadrasi_num']) > 3:
            second_phase = 'yes'
        connect_to_sql(sql_query=f"""UPDATE {table_name} SET is_done='{done}',
                       success='{success}', 
                       second_phase='{second_phase}', 
                       shenase_dadrasi='{shenase_dadrasi}', 
                       shenase_dadrasi_no='{info['shenase_dadrasi_num']}',
                       msg=N'{info['msg']}'
                       WHERE shomare = '{item['shomare']}'""")

    if (info['msg'] == 'خطا در ارتباط با سامانه دادرسی'):
        connect_to_sql(sql_query=f"""UPDATE {table_name} SET is_done='{done}',
                       success='{success}', 
                       second_phase='{second_phase}', 
                       shenase_dadrasi='{shenase_dadrasi}', 
                       shenase_dadrasi_no='{info['shenase_dadrasi_num']}',
                       msg=N'{info['msg']}'
                       WHERE shomare = '{item['shomare']}'""")

    return driver, info


# @wrap_a_wrapper
@wrap_it_with_paramsv1(28, 10, True, False, True, False)
def find_obj_and_click(driver, info, elm='OBJ', linktext='موارد اعتراض/ شکایت', textboxid="TinSearch-TIN"):
    WebDriverWait(driver, 8).until(
        EC.element_to_be_clickable(
            (By.ID,
                elm)))
    driver.find_element(
        By.ID,
        elm).click()

    time.sleep(3)

    WebDriverWait(driver, 8).until(
        EC.element_to_be_clickable(
            (By.LINK_TEXT,
                linktext))).click()

    time.sleep(3)

    frame = driver.find_element(
        By.XPATH, '/html/body/div[1]/div[1]/div/div/div/div/div/iframe[2]')
    driver.switch_to.frame(frame)

    # WebDriverWait(driver, 16).until(
    #     EC.element_to_be_clickable(
    #         (By.XPATH,
    #             f'//*[@id="{textboxid}"]'))).click()
    # time.sleep(1)

    return driver, info


# @wrap_a_wrapper
@wrap_it_with_paramsv1(2, 180, True, False, False, False)
def scrape_iris_helper(stop_threads,
                       index,
                       item,
                       driver,
                       df,
                       role,
                       shenase_dadrasi='no',
                       table_name='tbltempheiat',
                       info={},
                       serious=False,
                       *args,
                       **kwargs):

    time.sleep(1)

    driver, info = insert_value(driver=driver, item=item, info=info)
    time.sleep(1)
    driver, info = click_on_search_btn(
        driver=driver, info=info, path='//*[@id="tbSearch"]')

    timeout = 30
    try:
        while (WebDriverWait(driver, 2).until(
                EC.presence_of_element_located((
                    By.XPATH,
                    '/html/body/div[6]'
                )))):
            print('waiting')
            time.sleep(1)
            timeout -= 1
            if timeout == 0:
                raise Exception
            else:
                continue
    except:
        if timeout == 0:
            raise Exception

    # driver, info = table_dadrasi_exists(driver=driver, info=info)
    # if not info['success']:
    #     return driver, info

    # For heiat
    driver, info = get_info_shenase_dadrasi(driver=driver, info=info)
    driver, info = check_health(
        driver=driver,
        index=index,
        df=df,
        item=item,
        serious=False,
        role=role,
        init=False,
        table_name=table_name,
        info=info)
    if len(info['shenase_dadrasi_num']) > 4:
        return driver, info

    # table_rows = WebDriverWait(driver, 16).until(
    #     EC.element_to_be_clickable(
    #         (By.XPATH,
    #          "/html/body/table/tbody/tr/td/div[1]/\
    #              div[2]/table/tbody/tr[3]/td/table/tbody/tr[2]/td/div/div[3]")))

    # try:
    #     elm = WebDriverWait(driver, 16).until(
    #         EC.element_to_be_clickable((By.XPATH, '//*[@title="تخصیص داده شده" or @title="باز"]')))
    # except:
    #     elm = WebDriverWait(driver, 16).until(
    #         EC.element_to_be_clickable((By.XPATH, '//*[@id="row0cell5CaseStatusCol"]')))
    # elm.click()

    # driver, info = click_on_search_dadrasi(
    #     driver=driver, info=info, path="/html/body/table/tbody/tr/td/div[1]/div[1]/div/\
    #         div[1]/ul/li[4]/div/div[1]/div[2]/input")

    # try:

    #     driver.switch_to.default_content()

    #     frame = WebDriverWait(driver, 8).until(
    #         EC.element_to_be_clickable(
    #             (By.XPATH,
    #                 '/html/body/div[1]/div[1]/div/div/div/div/div/iframe[3]')))

    #     driver.switch_to.frame(frame)

    #     driver, info = insert_assignment(driver=driver, info=info, item=item)

    # except:
    #     print('f')

    # driver.switch_to.default_content()

    # frame = WebDriverWait(driver, 8).until(
    #     EC.element_to_be_clickable(
    #         (By.XPATH,
    #             '/html/body/div[1]/div[1]/div/div/div/div/div/iframe[2]')))

    # driver.switch_to.frame(frame)

    # return driver, info

    if (not info['success']):
        raise Exception

    if ((not info['success']) or
            (role == 'manager_phase1' and
             info['assigned'] == df[1]['employee'].head(1).item() and
             info['vaziat'] == 'ثبت شده توسط خدمات مودیان') or
            (role == 'manager_phase2' and info['vaziat'] == 'ثبت شده توسط خدمات مودیان')):
        return driver, info

    time.sleep(2)

    driver, info = select_row(driver=driver, info=info)
    if not info['success']:
        return driver, info

    if role == 'manager_phase1':
        # if condition true then continue with the next item

        if (((info['assigned'] == str(df[1]['employee'].head(1).item()) and
                info['vaziat'] in ['ثبت شده توسط خدمات مودیان', 'در انتظار تکمیل', 'باز']) or
                len(info['shenase_dadrasi_num']) > 3)):
            return driver, info
        if (info['vaziat'] == 'ثبت شده' and role != 'manager_phase1'):
            shenase_dadrasi = 'yes'
    if (role in ['employee']):

        if info['vaziat'] in ['ثبت شده', 'باز']:
            try:
                element = WebDriverWait(driver, 2).until(
                    EC.element_to_be_clickable(
                        (By.XPATH, '/html/body/table/tbody/tr/td/div[1]/\
                        div[1]/div/div[1]/ul/li[11]/div/div[1]/div[2]/input')))
            except:
                element = driver.find_element(By.XPATH, '/html/body/table/tbody/tr/td/div[1]/div[1]/div\
                    /div[1]/ul/li[7]/div/div[1]/div[2]/input')
                if element.get_attribute('title') == 'رای':
                    info['msg'] = 'رای ثبت نشده است'
                second_phase = 'yes'
                if info['vaziat'] == 'باز':
                    if len(info['shenase_dadrasi_num']) > 3:
                        driver, info = check_health(
                            driver=driver,
                            index=index,
                            df=df,
                            item=item,
                            serious=False,
                            role=role,
                            init=False,
                            table_name=table_name,
                            info=info)
                        return driver, info
                    second_phase = 'yes'
                    driver, info = check_health(
                        driver=driver,
                        index=index,
                        df=df,
                        item=item,
                        serious=False,
                        role=role,
                        init=False,
                        table_name=table_name,
                        info=info)
                return driver, info

        if (info['vaziat'] != 'ثبت شده توسط خدمات مودیان' and role == 'employee'):
            ...

    # if condition false, then assign taskto the new user
    driver, info = assign_btn(driver=driver, role=role,
                              shenase_dadrasi=shenase_dadrasi, info=info)

    driver, info = check_health(
        driver=driver,
        index=index,
        df=df,
        item=item,
        serious=serious,
        role=role,
        init=False,
        table_name=table_name,
        info=info)

    if not info['success']:
        return driver, info

    time.sleep(1)
    try:
        driver.switch_to.default_content()
        if (WebDriverWait(driver, 2).until(
                EC.element_to_be_clickable(
                (By.XPATH, '//*[@id="flexPopupCancelMsgBtn"]')))):

            try:

                if driver.find_element(
                        By.XPATH, '/html/body/div[7]/p').text == 'این وظیفه توسط کاربری دیگر رزرو شده است، آیا تمایل به انجام آن دارید؟':

                    driver.find_element(
                        By.ID, 'flexPopupOKMsgBtn').click()

                    frame = WebDriverWait(driver, 8).until(
                        EC.element_to_be_clickable(
                            (By.XPATH,
                             '/html/body/div[1]/div[1]/div/div/div/div/div/iframe[2]')))
                    driver.switch_to.frame(frame)

                    driver, info = assign_btn(driver=driver, role=role,
                                              shenase_dadrasi=shenase_dadrasi, info=info)

                # driver.find_element(
                #     By.XPATH, '//*[@id="flexPopupCancelMsgBtn"]').click()
                # frame = WebDriverWait(driver, 8).until(
                #     EC.element_to_be_clickable(
                #         (By.XPATH,
                #             '/html/body/div[1]/div[1]/div/div/div/div/div/iframe[2]')))

            except:
                ...

        else:

            driver.find_element(
                By.XPATH, '//*[@id="flexPopupCancelMsgBtn"]').click()
        frame = WebDriverWait(driver, 1).until(
            EC.element_to_be_clickable(
                (By.XPATH,
                 '/html/body/div[1]/div[1]/div/div/div/div/div/iframe[2]')))

        driver.switch_to.frame(frame)
        info['success'] = True
    except:
        ...
    driver, info = check_health(
        driver=driver,
        index=index,
        df=df,
        item=item,
        serious=serious,
        role=role,
        init=False,
        table_name=table_name,
        info=info)

    if not info['success']:
        return driver, info

    time.sleep(1)

    driver, info = go_to_next_frame(driver=driver, role=role,
                                    shenase_dadrasi=shenase_dadrasi, info=info)

    driver, success = check_health(
        driver=driver,
        index=index,
        df=df,
        item=item,
        serious=serious,
        role=role,
        init=False,
        table_name=table_name,
        info=info)

    if not info['success']:
        return driver, info

    time.sleep(2)

    driver, info = insert_new_assignment(
        driver=driver, df=df,
        index=index, role=role, item=item,
        shenase_dadrasi=shenase_dadrasi,
        info=info)

    driver, info = check_health(
        driver=driver,
        index=index,
        df=df,
        item=item,
        serious=False,
        role=role,
        init=False,
        table_name=table_name,
        info=info)

    if (not info['success']):
        return driver, info

    if role in ["manager_phase1", "employee"]:
        driver.switch_to.default_content()
        frame = WebDriverWait(driver, 8).until(
            EC.element_to_be_clickable(
                (By.XPATH,
                    '/html/body/div[1]/div[1]/div/div/div/div/div/iframe[2]')))
        driver.switch_to.frame(frame)
        timeout = 40
        try:
            while (WebDriverWait(driver, 2).until(
                    EC.presence_of_element_located((
                        By.XPATH,
                        '/html/body/div[6]'
                    )))):
                print('waiting')
                time.sleep(1)
                timeout -= 1
                if timeout == 0:
                    raise Exception
                else:
                    continue
        except:
            if timeout == 0:
                raise Exception

    driver, info = get_info_shenase_dadrasi(driver=driver, info=info)

    driver, info = check_health(
        driver=driver,
        index=index,
        df=df,
        item=item,
        serious=False,
        role=role,
        init=False,
        table_name=table_name,
        info=info)

    if (not info['success']):
        return driver, info

    if len(info['shenase_dadrasi_num']) > 3:
        ...
        return driver, info

    if (role in ['employee', 'manager_phase1']):
        driver.switch_to.default_content()
        frame = WebDriverWait(driver, 8).until(
            EC.element_to_be_clickable(
                (By.XPATH,
                    '/html/body/div[1]/div[1]/div/div/div/div/div/iframe[2]')))
        driver.switch_to.frame(frame)

    driver, info = get_info_shenase_dadrasi(driver=driver, info=info)

    driver, info = check_health(
        driver=driver,
        index=index,
        df=df,
        item=item,
        serious=serious,
        role=role,
        init=False,
        table_name=table_name,
        info=info)

    if (not info['success'] or
            (role == 'manager_phase1' and info['assigned'] == df[1]['employee'].head(1).item())):
        return driver, info

    time.sleep(1)
    shenase_dadrasi_num = None
    time.sleep(2)

    if (role == 'manager_phase2' and shenase_dadrasi == 'yes'):
        dadrasi_xpath = '/html/body/table/tbody/tr/td/div[1]/div[2]/table/tbody/tr[4]/td' + \
            '/table/tbody/tr[2]/td/div/div[2]/div/div[2]/table/tbody/tr/td[22]/div'

        shenase_dadrasi_num = WebDriverWait(driver, 2).until(
            EC.element_to_be_clickable(
                (By.XPATH,
                    dadrasi_xpath))).text

    if role == 'employee' and info['vaziat'] == 'باز':
        driver, info = click_on_search_btn(driver=driver, info=info)
        driver, info = get_info_shenase_dadrasi(driver=driver, info=info)
        driver, info = get_info_shenase_dadrasi(driver=driver, info=info)
        driver, info = check_health(
            driver=driver,
            index=index,
            df=df,
            item=item,
            serious=False,
            role=role,
            init=False,
            table_name=table_name,
            info=info)

    return driver, info


def old_sanim_way():
    with init_driver(pathsave=self.path, driver_type=self.driver_type, headless=self.headless) as driver:
        self.driver = driver
        global excel_file_names

        self.driver, self.info = login_sanim(
            driver=self.driver, info=self.info)
        if self.report_type == 'ezhar':
            download_button = download_button_ezhar
        else:
            download_button = download_button_rest

            # انتخاب منوی گزارشات اصلی
        self.driver, self.info = get_main_menu(
            driver=self.driver, info=self.info)

        td_number = get_td_number(report_type=self.report_type)

        if (self.report_type == '1000_parvande'):

            self.driver, self.info = download_1000_parvandeh(self.driver, self.report_type,
                                                             self.year, self.path, self.info)

        else:
            self.driver, self.info = select_year(
                driver=self.driver, info=self.info, year=self.year)

            self.driver, self.info = select_column(
                driver=self.driver, info=self.info, td_number=td_number)
            # دریافت اظهارنامه ها و تشخیص های صادر شده
            exists_in_first_list = first_list.count(td_number)

            if (exists_in_first_list):

                if not (is_updated_to_download(
                        '%s\%s' % (self.path, excel_file_names[0]))):
                    print('updating for report_type=%s and year=%s' %
                          (self.report_type, self.year))
                    self.driver, self.info = list_details(driver=self.driver,
                                                          info=self.info,
                                                          report_type=self.report_type,
                                                          manba='hoghoghi')

                    self.driver, self.info = select_btn_type(
                        driver=self.driver, info=self.info, report_type=self.report_type)

                    print(
                        '*******************************************************************************************'
                    )
                    download_excel(
                        path=self.path,
                        report_type=self.report_type,
                        type_of_excel='Hoghoghi',
                        no_files_in_path=0,
                        excel_file=excel_file_names[0],
                        year=self.year,
                        table_name=self.table_name,
                        type_of=self.type_of)
                    self.driver.back()

                if not (is_updated_to_download(
                        '%s\%s' % (self.path, excel_file_names[1]))):
                    print('updating for report_type=%s and year=%s' %
                          (self.report_type, self.year))
                    if (self.report_type != 'ezhar'):

                        self.driver, self.info = list_details(driver=self.driver,
                                                              info=self.info,
                                                              report_type=self.report_type,
                                                              manba='haghighi')

                        self.driver, self.info = select_btn_type(
                            driver=self.driver, info=self.info, report_type=self.report_type)

                    else:
                        WebDriverWait(self.driver, timeout_fifteen).until(
                            EC.presence_of_element_located(
                                (By.XPATH, td_ezhar % 4))).click()

                    print(
                        '*******************************************************************************************'
                    )

                    download_excel(path=self.path,
                                   report_type=self.report_type,
                                   type_of_excel='Haghighi',
                                   no_files_in_path=0,
                                   excel_file=excel_file_names[1],
                                   year=self.year,
                                   table_name=self.table_name,
                                   type_of=self.type_of)
                    self.driver.back()

                if not (is_updated_to_download(
                        '%s\%s' % (self.path, excel_file_names[2]))):
                    print('updating for report_type=%s and year=%s' %
                          (self.report_type, self.year))
                    if (self.report_type != 'ezhar'):
                        WebDriverWait(self.driver, timeout_fifteen).until(
                            EC.presence_of_element_located(
                                (By.XPATH, '/html/body/form/div[2]/div/div[2]/main/\
                                            div[2]/div/div/div/div/font/div\
                                            /div/div[2]/div[2]/div[5]/div[1]/div/div[2]\
                                            /table/tbody/tr[2]/td[8]/a'))).click()
                    else:
                        WebDriverWait(self.driver, timeout_fifteen).until(
                            EC.presence_of_element_located(
                                (By.XPATH, td_ezhar % 8))).click()

                    time.sleep(4)
                    WebDriverWait(self.driver, time_out_1).until(
                        EC.presence_of_element_located(
                            (By.XPATH, download_button))).click()

                    print(
                        '*******************************************************************************************'
                    )

                    download_excel(path=self.path,
                                   report_type=self.report_type,
                                   type_of_excel='Arzesh Afzoode',
                                   no_files_in_path=0,
                                   excel_file=excel_file_names[2],
                                   year=self.year,
                                   table_name=self.table_name,
                                   type_of=self.type_of)
                    self.driver.back()

            # if there is only one report and no distinction between haghighi, hoghoghi and arzesh afzoode
            else:

                time.sleep(3)
                WebDriverWait(self.driver, time_out_2).until(
                    EC.presence_of_element_located(
                        (By.XPATH, year_button_6))).click()
                time.sleep(1)
                WebDriverWait(self.driver, time_out_2).until(
                    EC.presence_of_element_located(
                        (By.XPATH, year_button_4))).click()
                time.sleep(0.5)
                WebDriverWait(self.driver, time_out_2).until(
                    EC.presence_of_element_located(
                        (By.XPATH, switch_to_data))).click()
                time.sleep(0.5)
                WebDriverWait(self.driver, time_out_2).until(
                    EC.presence_of_element_located(
                        (By.XPATH, download_excel_btn_1))).click()
                time.sleep(0.5)
                WebDriverWait(self.driver, time_out_2).until(
                    EC.presence_of_element_located(
                        (By.XPATH, download_excel_btn_2))).click()
                self.type_of = 'download'
                download_excel(path=self.path,
                               report_type=self.report_type,
                               type_of_excel=self.report_type,
                               no_files_in_path=0,
                               excel_file=badvi_file_names[0],
                               year=self.year,
                               table_name=self.table_name,
                               type_of=self.type_of)

        # else:
        time.sleep(self.time_out)


def scrape_sanim(self, *args, **kwargs):
    with init_driver(pathsave=self.path, driver_type=self.driver_type, headless=self.headless) as self.driver:
        self.driver, self.info = login_sanim(
            driver=self.driver, info=self.info)
        self.driver, self.info = get_main_menu(
            driver=self.driver, info=self.info)
        self.driver, self.info = select_year(
            driver=self.driver, info=self.info, year=self.year)
        td_number = get_td_number(report_type=self.report_type)
        self.driver, self.info = select_column(
            driver=self.driver, info=self.info, td_number=td_number)
        if self.report_type not in ['badvi_darjarian_dadrasi',
                                    'badvi_takmil_shode',
                                    'tajdidnazer_darjarian_dadrasi',
                                    'tajdidnazar_takmil_shode']:
            links = get_report_links(report_type=self.report_type)
            for link in links:
                self.driver, self.info = click_on_down_btn_sanim(
                    driver=self.driver, info=self.info, link=link)

                download_excel(func=lambda: click_on_down_btn_excelsanim(driver=self.driver, info=self.info),
                               path=self.path,
                               report_type=self.report_type,
                               type_of_excel=self.report_type,
                               no_files_in_path=0,
                               excel_file=badvi_file_names[0],
                               year=self.year,
                               table_name=self.table_name,
                               type_of=self.type_of)
                self.driver.back()

        else:
            self.driver, self.info = click_on_down_btn_excelsanimforheiat(
                driver=self.driver, info=self.info)
            download_excel(func=lambda: click_on_down_btn_excelsanimforheiatend(driver=self.driver, info=self.info),
                           path=self.path,
                           report_type=self.report_type,
                           type_of_excel=self.report_type,
                           no_files_in_path=0,
                           excel_file=badvi_file_names[0],
                           year=self.year,
                           table_name=self.table_name,
                           type_of=self.type_of)
            self.driver.back()

    return self.driver, self.info


@wrap_it_with_paramsv1(15, 10, True, False, False, True)
def select_from_dropdown(driver, info, xpath, dropitem='تاریخ تشکیل هیات'):

    WebDriverWait(driver, 1).until(
        EC.presence_of_element_located(
            (By.XPATH, xpath))).click()
    WebDriverWait(driver, 1).until(
        EC.presence_of_element_located(
            (By.XPATH, xpath))).\
        send_keys(dropitem)
    WebDriverWait(driver, 1).until(
        EC.presence_of_element_located(
            (By.XPATH, xpath))).\
        send_keys(Keys.RETURN)

    return driver, info


@wrap_it_with_paramsv1(15, 10, True, False, False, True)
def set_start_date(driver, info, xpath, date):
    WebDriverWait(driver, 3).until(
        EC.presence_of_element_located(
            (By.ID, xpath))).clear()
    WebDriverWait(driver, 3).until(
        EC.presence_of_element_located(
            (By.ID, xpath))).send_keys(date)
    time.sleep(2)
    if (WebDriverWait(driver, 3).until(
        EC.presence_of_element_located(
            (By.ID, xpath))).get_attribute("value") == date):

        return driver, info
    else:
        raise Exception


@wrap_it_with_paramsv1(15, 10, True, False, False, True)
def get_sodor_gharar_karshenasi(driver, info):
    # وارد کردن تاریخ شروع
    try:
        sql_query = "SELECT MAX([آخرین بروزرسانی]) FROM tblamar_sodor_gharar_karshenasi"
        df = connect_to_sql(sql_query, read_from_sql=True,
                            return_df=True).iloc[0].values[0]
    except:
        df = None
    if df is None:
        date = '1390/01/01'
    else:
        start_date = f'{df[:4]}/{df[4:6]}/{df[6:]}'
        date = add_days_to_persian_date(start_date)
    driver, info = set_start_date(
        driver=driver, info=info, xpath='P380_START_DATE', date='1390/01/01')

    # انتخاب تاریخ تشکیل هیات
    driver, info = select_from_dropdown(driver=driver, info=info, xpath='/html/body/\
        form/div[1]/div/div[2]\
        /main/div[2]/div\
        /div[1]/div/div/div/div[1]/div[1]/div/div[2]\
            /div/span/span[1]/span', dropitem='تاریخ تشکیل هیات')
    # انتخاب قرار اجرا شده و نشده
    driver, info = select_from_dropdown(driver=driver, info=info, xpath='/html/body/form\
        /div[1]/div/div[2]/\
        main/div[2]/div/div[1]/div/div\
        /div/div[2]/div[1]/div/div[2]/\
            div/span/span[1]/span', dropitem='قرار اجرا شده و نشده')
    # کلیک کلید جستجو
    WebDriverWait(driver, 1).until(
        EC.presence_of_element_located(
            (By.ID, 'B200056751384075521'))).click()

    try:
        try:
            while (WebDriverWait(driver, 20).until(
                EC.presence_of_element_located(
                    (By.XPATH, '/html/body/form/div[1]/div/div[2]/main/div[2]/div/div[2]/div\
                        /div/div/div/div[2]/div[2]/div[5]/div/span'))).text == 'اطلاعاتی برای نمایش یافت نشد'):
                time.sleep(2)
                print('waiting')
        except:
            while (WebDriverWait(driver, 20).until(
                EC.presence_of_element_located(
                    (By.CLASS_NAME, 'u-Processing-spinner'))).is_displayed()):
                print('waiting')
    except:
        time.sleep(1)
        print('done')

    return driver, info


@wrap_it_with_paramsv1(15, 10, True, False, False, True)
def get_amar_sodor_ray(driver, info):
    # وارد کردن تاریخ شروع

    driver, info = set_start_date(
        driver=driver, info=info, xpath='P482_START_DATE_RAY', date='1390/01/01')

    driver, info = set_start_date(
        driver=driver, info=info, xpath='P482_END_DATE_RAY',
        date=f'{get_update_date()[:4]}/{get_update_date()[4:6]}/{get_update_date()[6:8]}')

    # کلیک کلید جستجو
    WebDriverWait(driver, 1).until(
        EC.presence_of_element_located(
            (By.ID, 'B1451081100278449861'))).click()

    try:
        try:
            while (WebDriverWait(driver, 20).until(
                EC.presence_of_element_located(
                    (By.CLASS_NAME, 'icon-irr-no-results'))).is_displayed()):
                time.sleep(2)
                print('waiting')
        except:
            while (WebDriverWait(driver, 20).until(
                EC.presence_of_element_located(
                    (By.CLASS_NAME, 'u-Processing-spinner'))).is_displayed()):
                print('waiting')
    except:
        time.sleep(1)
        print('done')

    return driver, info


@wrap_it_with_paramsv1(15, 10, True, False, False, True)
def get_imp_parvand(driver, info):
    # وارد کردن تاریخ شروع
    combos = WebDriverWait(driver, 20).until(
        EC.presence_of_element_located(
            (By.XPATH, '/html/body/form/div[1]/div/div[2]/main/\
                        div[2]/div/div/div/div/div[1]')))
    combos = combos.find_elements(By.CLASS_NAME, 'select2-selection')
    combos[0].click()
    time.sleep(1)
    combos[0].send_keys('100')
    combos[0].send_keys(Keys.RETURN)
    combos[1].click()
    time.sleep(1)
    combos[1].send_keys('خوزستان')
    combos[1].send_keys(Keys.RETURN)

    # کلیک کلید جستجو
    WebDriverWait(driver, 1).until(
        EC.presence_of_element_located(
            (By.ID, 'B2023811065487053636'))).click()

    try:
        try:
            while (WebDriverWait(driver, 20).until(
                EC.presence_of_element_located(
                    (By.CLASS_NAME, 'icon-irr-no-results'))).is_displayed()):
                time.sleep(2)
                print('waiting')
        except:
            while (WebDriverWait(driver, 10).until(
                EC.presence_of_element_located(
                    (By.CLASS_NAME, 'u-Processing-spinner'))).is_displayed()):
                print('waiting')
    except:
        time.sleep(1)
        print('done')

    return driver, info
