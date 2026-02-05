import time
from functools import wraps
from iris_helpers import init_driver, login_iris, connect_to_sql, retry_decorator
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException

# Optional import for progress bar
try:
    from tqdm import tqdm
except ImportError:
    # Fallback if tqdm is not available
    def tqdm(iterable, *args, **kwargs):
        return iterable

# Constants for repeated XPaths
COMMON_XPATHS = {
    'request_no_input': '/html/body/table/tbody/tr/td/div[1]/div[2]/table/tbody/tr[3]/td/table/tbody/tr[2]/td[2]/div/input',
    'main_iframe': '/html/body/div[1]/div[1]/div/div/div/div/div/iframe[2]',
    'search_button': '/html/body/table/tbody/tr/td/div[1]/div[1]/div/div[1]/ul/li[4]/div/div[1]/div[2]/input',
    'row_selection': '/html/body/table/tbody/tr/td/div[1]/div[2]/table/tbody/tr[4]/td/table/tbody/tr[2]/td/div/div[2]/div/div[2]/table/tbody/tr/td[8]/div',
    'popup_loading': '/html/body/div[13]',
    'popup_error': '/html/body/div[6]'
}

# Helper functions for common patterns


def wait_and_click(driver, xpath, timeout=5):
    """Helper for WebDriverWait + click pattern"""
    element = WebDriverWait(driver, timeout).until(
        EC.element_to_be_clickable((By.XPATH, xpath))
    )
    element.click()
    return driver


def wait_and_get_element(driver, xpath, timeout=5):
    """Helper for WebDriverWait + get element"""
    return WebDriverWait(driver, timeout).until(
        EC.presence_of_element_located((By.XPATH, xpath))
    )


def switch_to_frame_safely(driver, xpath, timeout=8):
    """Helper for frame switching"""
    frame = WebDriverWait(driver, timeout).until(
        EC.element_to_be_clickable((By.XPATH, xpath))
    )
    driver.switch_to.frame(frame)
    return driver


def switch_to_default_and_frame(driver, frame_xpath, timeout=8):
    """Helper for switching to default content then to frame"""
    driver.switch_to.default_content()
    return switch_to_frame_safely(driver, frame_xpath, timeout)


def time_it(threshold_seconds=180):  # default: 3 minutes
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            start = time.perf_counter()
            result = func(*args, **kwargs)
            end = time.perf_counter()
            elapsed = end - start

            print(
                f"⏱ Function '{func.__name__}' took {elapsed:.2f} seconds to complete.")

            if elapsed > threshold_seconds:
                print(
                    f"⚠️ WARNING: '{func.__name__}' exceeded {threshold_seconds/60:.0f} minutes!")
                # You can add a notification method here (e.g., email, sound, log, etc.)

            return result
        return wrapper
    return decorator


class IrisScraper:
    """
    Dedicated scraper class for IRIS functionality.
    Isolated from the main Scrape class to improve modularity.
    """

    def __init__(self, driver_type='firefox', headless=True, info=None):
        """
        Initialize the IRIS scraper.

        Args:
            driver_type (str): Type of web driver to use (default: 'firefox')
            headless (bool): Whether to run in headless mode (default: True)
            info (dict): Additional information dictionary (default: None)
        """
        self.driver_type = driver_type
        self.headless = headless
        self.info = info or {}
        self.driver = None
        self.path = None

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
                    info=None):

        self.info = info or {}
        self.path = path

        # Filter data based on role
        if role == 'manager_phase2':

            df_final = df[0].loc[
                (df[0]['shenase_dadrasi_no'].fillna('') != '0') &
                (df[0]['shenase_dadrasi_no'].fillna('').str.len() < 2) &
                (df[0]['msg'].fillna('').str.len() < 4) &
                (df[0]['assigned_to'] != df[1].iloc[0]["employee"])]

        elif role == 'manager_phase1':
            filtered = df[0].loc[df[0]['shenase_dadrasi_no'] != 'yes']
            df_final = df[0]
            # df_final = filtered if len(filtered) < 2 else df[0].iloc[0:0]  # empty DataFrame with same columns

        else:
            df_final = df[0].loc[
                (df[0]['assigned_to'] == df[1].iloc[0]["employee"])
                & (df[0]['shenase_dadrasi_no'].fillna('') != '0')
                & (df[0]['shenase_dadrasi_no'].fillna('').str.len() < 4)
                & (df[0]['msg'].fillna('').str.len() < 2)
            ]

        if not df_final.empty:
            init = True
            with init_driver(pathsave=path, driver_type=self.driver_type,
                             headless=headless, prefs={'maximize': True,
                                                       'zoom': '0.60'},
                             disable_popups=True, info=self.info) as self.driver:

                for index, item in tqdm(df_final.iterrows()):
                    if init:
                        # Login to IRIS
                        self.driver, self.info = login_iris(
                            self.driver,
                            creds={'username': str(df[1][role].iloc[0]),
                                   'pass': str(df[1]['pass'].iloc[0])},
                            info=self.info
                        )

                        self.driver, self.info = wait_and_click_request_no(
                            self.driver, self.info, max_attempts=15)

                        init = False

                    scrape_iris_helper(
                        lambda: False, index, item, self.driver, df,
                        role, shenase_dadrasi, table_name)

        return self.driver, self.info


# IRIS-specific helper functions moved from scrape_helpers.py

@retry_decorator(max_retries=10, delay=1, driver_based=True, close_driver=False,
                 custom_cleanup=None, use_global_retries=False,
                 tuple_result_raises=False, custom_error_msg=None)
def check_if_value_inserted(driver, item, path='/html/body/table/tbody/tr/td/div[1]/div[2]/table/tbody/tr[3]/td/table/tbody/tr[2]/td[2]/div/input'):
    if (WebDriverWait(driver, 12).until(
        EC.element_to_be_clickable(
            (By.XPATH,
             path))).
            get_attribute('value') == str(int(item))):
        return driver
    else:
        raise Exception


# def step_log(func):

#     def wrapper(*args, **kwargs):
#         res = func(*args, **kwargs)

#         return res
#     return wrapper


def insert_value(driver, item, info,
                 path='/html/body/table/tbody/tr/td/div[1]/div[2]/table/tbody/tr[3]/td/table/tbody/tr[2]/td[2]/div/input',
                 attempt=1, max_retries=10):
    """
    Insert value into the input field, retrying up to max_retries times.
    If all attempts fail, click the popup and call insert_value again.
    """
    item_value = item['shomare']

    try:
        click_popup(driver, "flexPopupErrMsgBtn")
        # Wait for the input element to be clickable
        element = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "#RequestNo input[name='requestNo']")))

        element.clear()
        element.send_keys(str(item_value))

        # Validate insertion
        driver = check_if_value_inserted(driver, item_value, path)
        return driver, info  # ✅ Success: stop retrying

    except Exception as e:
        if attempt < max_retries:
            # 🔄 Retry without clicking popup yet
            return insert_value(driver, item, info, path, attempt + 1, max_retries)
        else:
            # ❗ After max failures, click the popup and try again
            driver = click_popup(driver, "flexPopupFailMsgBtn", switched=False)
            # Restart attempts after popup click
            return insert_value(driver, item, info, path, attempt=1, max_retries=max_retries)


# Removed duplicate imports - already imported at top

def get_info_shenase_dadrasi(driver, info, retries=5, delay=4):
    attempt = 0
    while attempt <= retries:
        try:
            info['shenase_dadrasi_num'] = WebDriverWait(driver, 2).until(
                EC.presence_of_element_located(
                    (By.ID, "row0cell23BstdSerialCol"))
            ).text.strip()
            print("BstdSerial:", info['shenase_dadrasi_num'])

            info['vaziat'] = WebDriverWait(driver, 1).until(
                EC.presence_of_element_located(
                    (By.ID, "row0cell7RequestStatusCol"))
            ).text.strip()
            print("AssignTo:", info['vaziat'])

            info['assigned_to'] = WebDriverWait(driver, 1).until(
                EC.presence_of_element_located(
                    (By.ID, "row0cell9RequestAssignToCol"))
            ).text.strip()
            print("✅ Extracted text:", info['assigned_to'])

            # ✅ if value is not empty, break out of loop
            if info['shenase_dadrasi_num']:
                return driver, info

        except Exception as e:
            print(f"⚠️ Attempt {attempt+1} failed: {e}")

        attempt += 1
        if attempt <= retries:
            print(f"🔄 Retrying in {delay} seconds...")
            time.sleep(delay)

    print("❌ Failed to extract 'shenase_dadrasi_num' after retries.")
    return driver, info


def click_on_search_btn(driver, info,
                        path='/html/body/table/tbody/tr/td/div[1]/div[1]/div/div[1]/ul/li[4]/div/div[1]/div[2]/input',
                        attempt=1, max_retries=10):
    """
    Clicks the search button, retries up to max_retries.
    If all attempts fail, clicks the popup and tries again.
    """
    try:
        # Wait until the search button is clickable and click it
        search_btn = WebDriverWait(driver, 5).until(
            EC.element_to_be_clickable(
                (By.CSS_SELECTOR, "input.iconBtn.search_icon[title='جستجو']"))
        )
        search_btn.click()

        print("✅ Search button clicked")

        # Wait until the loading popup disappears
        while True:
            try:
                if WebDriverWait(driver, 1).until(
                    EC.presence_of_element_located(
                        (By.XPATH, "/html/body/div[13]"))
                ):
                    time.sleep(2)
                    print("waiting")
                    continue
            except:
                time.sleep(1)
                break

        return driver, info  # ✅ Success

    except Exception as e:
        if attempt < max_retries:
            # 🔄 Retry without clicking popup yet
            return click_on_search_btn(driver, info, path, attempt + 1, max_retries)
        else:
            # ❗ After max failures, click the popup and retry
            driver = click_popup(driver, "flexPopupFailMsgBtn", switched=False)
            return click_on_search_btn(driver, info, path, attempt=1, max_retries=max_retries)


def run_search_until_value(driver, info, max_retries=10, wait_between=5):
    """
    Runs click_on_search_btn repeatedly until the target <td> has a valid value.
    """
    for attempt in range(1, max_retries + 1):
        print(f"▶️ Attempt {attempt}")

        driver = click_popup(driver, "flexPopupErrMsgBtn", switched=False)
        # Call your function once
        click_on_search_btn(driver, info)

        try:
            # Wait briefly for the <td> to appear and check its value
            td = WebDriverWait(driver, 30).until(
                EC.presence_of_element_located(
                    (By.ID, "row0cell23BstdSerialCol")
                )
            )
            value = td.get_attribute("title") or td.text.strip()

            if value and value != "":  # ✅ Found valid value
                print(f"✅ Found valid value: {value}")
                return driver, info

            else:
                raise ValueError("Empty value")

        except Exception as e:
            print(f"❌ Value not found or error: {e}")
            if attempt < max_retries:
                print(f"🔁 Retrying in {wait_between}s...")
                time.sleep(wait_between)
            else:
                print("⏳ Max retries reached. Stopping.")
                break

    return driver, info


@retry_decorator(max_retries=5, delay=1, driver_based=True, close_driver=False,
                 custom_cleanup=None, use_global_retries=False,
                 tuple_result_raises=False, custom_error_msg=None)
def select_row(driver, info):
    WebDriverWait(driver, 2).until(
        EC.element_to_be_clickable(
            (By.XPATH, '/html/body/table/tbody/tr/td/div[1]/div[2]/table/tbody/tr[4]/td/table/tbody/tr[2]/td/div/div[2]/div/div[2]/table/tbody/tr/td[8]/div'))).click()

    time.sleep(2)
    return driver, info


@retry_decorator(max_retries=12, delay=1, driver_based=True, close_driver=False,
                 custom_cleanup=None, use_global_retries=False,
                 tuple_result_raises=False, custom_error_msg=None)
def assign_btn(driver, role='manager_phase2', shenase_dadrasi='no',
               info={'success': True, 'keep_alive': True}):

    if (role == 'manager_phase2'):
        try:
            element = WebDriverWait(driver, 1).until(
                EC.element_to_be_clickable(
                    (By.XPATH, '/html/body/table/tbody/tr/td/div[1]/div[1]/div/div[1]/ul/li[10]/div/div[1]/div[2]/input')))
            if element.get_attribute("title") == 'پردازش':
                element.click()
            try:
                driver.switch_to.default_content()
                WebDriverWait(driver, 1).until(
                    EC.element_to_be_clickable(
                        (By.XPATH, '//*[@id="flexPopupOKMsgBtn"]'))).click()
                frame = WebDriverWait(driver, 8).until(
                    EC.element_to_be_clickable(
                        (By.XPATH, COMMON_XPATHS['main_iframe'])))
                driver.switch_to.frame(frame)
                element.click()
            except:
                pass
        except:
            pass

    if (role == 'employee' and info['vaziat'] == 'باز'):
        try:
            element = WebDriverWait(driver, 2).until(
                EC.element_to_be_clickable(
                    (By.XPATH, '/html/body/table/tbody/tr/td/div[1]/div[1]/div/div[1]/ul/li[11]/div/div[1]/div[2]/input')))
            if element.get_attribute("title") == 'دریافت':
                element.click()
        except:
            ...

    elif (role == 'employee' and info['vaziat'] == 'ثبت شده توسط خدمات مودیان'):
        WebDriverWait(driver, 1).until(
            EC.element_to_be_clickable(
                (By.XPATH, '/html/body/table/tbody/tr/td/div[1]/div[1]/div/div[1]/ul/li[10]/div/div[1]/div[2]/input'))).click()

    elif (role == 'manager_phase1'):
        if (info['vaziat'] in ['ثبت شده توسط خدمات مودیان', 'باز']):
            try:
                element = WebDriverWait(driver, 1).until(
                    EC.element_to_be_clickable(
                        (By.XPATH, '/html/body/table/tbody/tr/td/div[1]/div[1]/div/div[1]/ul/li[6]/div/div[1]/div[2]/input')))
                if element.get_attribute("title") == 'تخصیص/تخصیص مجدد':
                    element.click()
            except:
                info['success'] = False
                return driver, info
        elif (info['vaziat'] == 'ثبت شده'):
            try:
                element = WebDriverWait(driver, 1).until(
                    EC.element_to_be_clickable(
                        (By.XPATH, '/html/body/table/tbody/tr/td/div[1]/div[1]/div/div[1]/ul/li[10]/div/div[1]/div[2]/input')))
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
                        (By.XPATH, COMMON_XPATHS['main_iframe'])))
                driver.switch_to.frame(frame)
                element.click()
                info['success'] = True
                return driver, info

    elif (role == 'employee' and shenase_dadrasi == 'no'):
        WebDriverWait(driver, 1).until(
            EC.element_to_be_clickable(
                (By.XPATH, '/html/body/table/tbody/tr/td/div[1]/div[1]/div/div[1]/ul/li[11]/div/div[1]/div[2]/input'))).click()

    elif (role == 'employee' or shenase_dadrasi == 'yes'):
        WebDriverWait(driver, 1).until(
            EC.element_to_be_clickable(
                (By.XPATH, '//html/body/table/tbody/tr/td/div[1]/div[1]/div/div[1]/ul/li[10]/div/div[1]/div[2]/input'))).click()

    time.sleep(1)
    return driver, info


@retry_decorator(max_retries=11, delay=1, driver_based=True, close_driver=False,
                 custom_cleanup=None, use_global_retries=False,
                 tuple_result_raises=False, custom_error_msg=None)
def go_to_next_frame(driver, role='manager_phase2', shenase_dadrasi='no', info={}):
    if (role == 'manager_phase2' or role == "manager_phase1"):
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


def wait_for_popup(driver, timeout=90, check_interval=1, progress_interval=10):
    """
    Wait until popup disappears, raise if timeout.
    Prints less frequently to avoid flooding the terminal.
    """
    print("⏳ Waiting for popup to disappear...")
    waited = 0

    while waited < timeout:
        try:
            WebDriverWait(driver, check_interval).until(
                EC.presence_of_element_located((By.XPATH, '/html/body/div[6]'))
            )
            waited += check_interval
            # Print progress every 'progress_interval' seconds
            if waited % progress_interval == 0:
                print(f"Still waiting... {waited}/{timeout}s elapsed")
        except:
            print("✅ Popup disappeared")
            return driver
        time.sleep(check_interval)

    raise TimeoutError(f"❌ Popup did not disappear after {timeout}s")


def safe_click(driver, xpath, timeout=5):
    """Try to click element safely with WebDriverWait."""
    return wait_and_click(driver, xpath, timeout)


def safe_switch_to_frame(driver, xpath, timeout=8):
    """Switch to iframe safely."""
    return switch_to_frame_safely(driver, xpath, timeout)


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
        driver, info = wait_and_click_request_no(driver, info, max_attempts=10)

        return driver, info
    if (('shenase_dadrasi_num' not in info) and ('msg' not in info) and ('assigned' not in info)):
        info['msg'] = 'یافت نشد'

    if 'shenase_dadrasi_num' not in info:
        info['shenase_dadrasi_num'] = ''
    if 'msg' not in info:
        info['msg'] = ''
    if 'vaziat' not in info:
        info['vaziat'] = ''
    if 'assigned_to' not in info:
        info['assigned_to'] = ''
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
        if info['assigned_to'] != df[1]['employee'].head(1).item():
            return driver, info
        if (info['vaziat'] == 'ثبت شده توسط خدمات مودیان' and
                info['assigned_to'] == df[1]['employee'].head(1).item()):
            second_phase = 'no'
        if (info['assigned_to'] == '-' and info['vaziat'] == 'ثبت شده توسط خدمات مودیان'):
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

    if ((info['assigned_to'] == df[1]['employee'].head(1).item() and role == 'manager_phase1') or
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


def find_obj_and_click(driver, info, elm='OBJ', linktext='موارد اعتراض/ شکایت', textboxid="TinSearch-TIN"):

    wait = WebDriverWait(driver, 60)

    # locate the اعتراض/شکایت link
    link = wait.until(EC.element_to_be_clickable(
        (By.CSS_SELECTOR, "a.menumain-link[title='اعتراض/شکایت']")))

    # keep trying until the desired span shows up (or timeout)
    timeout = time.time() + 20  # max 20 seconds loop
    while True:
        try:
            link.click()
            # wait briefly for the span to appear
            WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable(
                    (By.XPATH, "//span[@class='menuitem-text' and normalize-space(text())='موارد اعتراض/ شکایت']"))
            ).click()
            print("✅ Found 'موارد اعتراض/ شکایت'. Stopping.")
            break
        except Exception:
            if time.time() > timeout:
                print("⏳ Timeout reached, span not found.")
                break
            print("🔁 Retrying click...")
            time.sleep(1)

    # wait until the frame is available, then switch to it
    WebDriverWait(driver, 32).until(
        EC.frame_to_be_available_and_switch_to_it(
            (By.XPATH, COMMON_XPATHS['main_iframe'])
        )
    )

    print("✅ Switched to iframe successfully")

    return driver, info

# Removed duplicate imports - already imported at top


def wait_and_click_request_no(driver, info, max_attempts=10):
    """
    Repeats find_obj_and_click until the 'requestNo' input becomes clickable.
    Waits 10 seconds between attempts.

    :param driver: Selenium WebDriver instance
    :param info: Extra info (passed through to find_obj_and_click)
    :param max_attempts: Safety limit to avoid infinite loops
    """
    attempts = 0
    while attempts < max_attempts:
        try:
            # Wait up to 5 sec for clickability
            WebDriverWait(driver, 5).until(
                EC.element_to_be_clickable((By.NAME, "requestNo"))
            )
            print("✅ 'requestNo' input is clickable!")
            break
        except Exception:
            print(f"🔁 Attempt {attempts+1}: Element not clickable yet.")
            # Perform your function
            find_obj_and_click(driver, info)
            # Wait before retrying
            time.sleep(10)
            attempts += 1
    else:
        print("⏳ Reached max attempts without element becoming clickable.")

    return driver, info


def handle_health_check(driver, index, df, item, role, table_name, info, serious=False):
    """Wrapper for check_health with common params."""
    driver, info = check_health(
        driver=driver,
        index=index,
        df=df,
        item=item,
        serious=serious,
        role=role,
        init=False,
        table_name=table_name,
        info=info,
    )
    return driver, info


def process_manager_phase1(driver, df, info, role):
    if (info["assigned_to"] == str(df[1]["employee"].head(1).item())
        and info["vaziat"] in ["ثبت شده توسط خدمات مودیان", "در انتظار تکمیل", "باز"]) \
            or len(info["shenase_dadrasi_num"]) > 3:
        return driver, info
    if info["vaziat"] == "ثبت شده" and role != "manager_phase1":
        return driver, {**info, "shenase_dadrasi": "yes"}
    return driver, info


def process_manager_phase2(driver, df, info, role):
    if (info["assigned_to"] == str(df[1]["employee"].head(1).item())
        and info["vaziat"] in ["ثبت شده توسط خدمات مودیان", "در انتظار تکمیل", "باز"]) \
            or len(info["shenase_dadrasi_num"]) > 3:
        return driver, info
    if info["vaziat"] == "ثبت شده" and role != "manager_phase1":
        return driver, {**info, "shenase_dadrasi": "yes"}
    return driver, info


def process_employee(driver, index, df, item, role, table_name, info):
    if info["vaziat"] in ["ثبت شده", "باز"]:
        try:
            safe_click(
                driver, '/html/body/table/tbody/tr/td/div[1]/div[1]/div/div[1]/ul/li[11]/div/div[1]/div[2]/input', 2)
        except:
            element = driver.find_element(By.XPATH,
                                          '/html/body/table/tbody/tr/td/div[1]/div[1]/div/div[1]/ul/li[7]/div/div[1]/div[2]/input')
            if element.get_attribute("title") == "رای":
                info["msg"] = "رای ثبت نشده است"
            if info["vaziat"] == "باز":
                driver, info = handle_health_check(
                    driver, index, df, item, role, table_name, info)
            return driver, info
    if info["vaziat"] != "ثبت شده توسط خدمات مودیان":
        return driver, info
    return driver, info


def click_on_btnassignment(driver, info):
    # Click tbProcess button
    # Wait for the element to be clickable
    element = WebDriverWait(driver, 16).until(
        EC.element_to_be_clickable(
            (By.XPATH, "//input[@title='تخصیص/تخصیص مجدد']"))
    )

    # Click it
    element.click()

    try:
        # Switch to default content and click OK button
        driver.switch_to.default_content()
        ok_button = WebDriverWait(driver, 2).until(
            EC.element_to_be_clickable((By.ID, "flexPopupOKMsgBtn"))
        )
        ok_button.click()
        print("✅ OK button clicked")

        # Wait for iframe and switch to it
        WebDriverWait(driver, 32).until(
            EC.frame_to_be_available_and_switch_to_it(
                (By.XPATH, COMMON_XPATHS['main_iframe'])
            )
        )
        print("✅ Switched to iframe successfully")

        # If everything worked fine, repeat the function
        return click_on_btnassignment(driver, info)

    except Exception as e:
        print(f"⚠️ Stopping because of error: {e}")
        return driver, info


def click_submit_button(driver, info, assigned_to='', timeout=10, max_attempts=3):
    """
    Wait for the ارسال (submit) button to be clickable and click it.
    Retries if the click fails or the button is not found.
    """
    submit_locator = (
        By.XPATH, "//input[@class='iconBtn submit_icon' and @title='ارسال']")

    for attempt in range(max_attempts):
        try:
            # Wait until the button is clickable
            submit_button = WebDriverWait(driver, timeout).until(
                EC.element_to_be_clickable(submit_locator)
            )

            # Click the button
            submit_button.click()
            print(
                f"Submit button clicked successfully on attempt {attempt+1}.")
            info['assigned_to'] = assigned_to
            return driver, info

        except TimeoutException:
            print(
                f"Attempt {attempt+1}: Submit button not clickable, retrying...")

    # If all retries fail
    print("Submit button could not be clicked after max retries.")
    return driver, info


def click_on_btnprocess(driver, info):
    # Click tbProcess button
    WebDriverWait(driver, 16).until(
        EC.presence_of_element_located((By.XPATH, '//*[@id="tbProcess"]'))
    ).click()

    try:
        # Switch to default content and click OK button
        driver.switch_to.default_content()
        ok_button = WebDriverWait(driver, 2).until(
            EC.element_to_be_clickable((By.ID, "flexPopupOKMsgBtn"))
        )
        ok_button.click()
        print("✅ OK button clicked")

        # Wait for iframe and switch to it
        WebDriverWait(driver, 32).until(
            EC.frame_to_be_available_and_switch_to_it(
                (By.XPATH, COMMON_XPATHS['main_iframe'])
            )
        )
        print("✅ Switched to iframe successfully")

        # If everything worked fine, repeat the function
        return click_on_btnprocess(driver, info)

    except Exception as e:
        print(f"⚠️ Stopping because of error: {e}")
        return driver, info


def click_on_input_assgnment(driver, info, employee_shenase):
    # Locators
    input_locator = (
        By.CSS_SELECTOR, "#AssignUser input.ui_list_ListOfValue_text")
    popup_locator = (
        By.XPATH, "//div[@testinfo='AssignUser:POPUP' and contains(@class,'ui_list_ListOfValue-listDiv')]")

    # Retry loop
    max_attempts = 3
    for attempt in range(max_attempts):
        try:
            # Step 1: wait for visible input and click
            input_element = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable(input_locator)
            )
            input_element.click()

            # Step 2: wait for popup to appear
            popup_element = WebDriverWait(driver, 5).until(
                EC.visibility_of_element_located(popup_locator)
            )
            print("Popup appeared!")

            # Step 3: clear + send keys
            input_element.clear()
            input_element.send_keys(employee_shenase)

            # Step 4: verify value
            WebDriverWait(driver, 3).until(
                lambda d: input_element.get_attribute(
                    "value") == employee_shenase
            )
            time.sleep(1)
            print("send_keys successful!")
            break  # success → exit loop

        except TimeoutException:
            print(
                f"Attempt {attempt+1}: Popup did not appear or value not set, retrying...")

    else:
        raise Exception("Failed to send keys after max retries.")

    return driver, info


def click_on_accept(driver, info):
    WebDriverWait(driver, 16).until(
        EC.presence_of_element_located((
            By.XPATH,
            '//*[@id="tbAccept"]'
        ))).click()
    return driver, info


def click_on_close(driver, info):
    WebDriverWait(driver, 16).until(
        EC.element_to_be_clickable((
            By.XPATH,
            '//a[@class="win-tabs-item-close-btn"]'
        ))
    ).click()
    return driver, info


# --------------- Logging Helper --------------- #
def log_step(step_num, message, status="INFO"):
    """Logs each step with status indicators."""
    symbols = {
        "INFO": "ℹ️",
        "SUCCESS": "✅",
        "ERROR": "❌",
        "WARNING": "⚠️"
    }
    print(f"{symbols.get(status, 'ℹ️')} Step {step_num}: {message}")


def update_sql(table_name, info, item):
    connect_to_sql(
        sql_query=f"""
            UPDATE {table_name} SET 
                assigned_to ='{info.get('assigned_to', '')}',
                msg =N'{info.get('msg', '')}',
                shenase_dadrasi_no='{info.get('shenase_dadrasi_num', '')}'
            WHERE shomare='{item['shomare']}'
        """
    )


def click_popup(driver, popup_id, timeout=3, switched=True):
    try:
        if switched:
            WebDriverWait(driver, timeout).until(
                EC.element_to_be_clickable((By.ID, popup_id))
            ).click()
        else:
            driver.switch_to.default_content()
            WebDriverWait(driver, timeout).until(
                EC.element_to_be_clickable((By.ID, popup_id))
            ).click()
            frame = WebDriverWait(driver, 8).until(
                EC.element_to_be_clickable(
                    (By.XPATH, COMMON_XPATHS['main_iframe'])))
            driver.switch_to.frame(frame)
    except:
        if not switched:
            frame = WebDriverWait(driver, 8).until(
                EC.element_to_be_clickable(
                    (By.XPATH, COMMON_XPATHS['main_iframe'])))
            driver.switch_to.frame(frame)
        pass
    return driver


def step_log(step, message, level="INFO"):
    log_step(step, message, level)
    return step + 1


# --------------- Main Function --------------- #
@time_it(threshold_seconds=30)
def scrape_iris_helper(
    stop_threads,
    index,
    item,
    driver,
    df,
    role,
    shenase_dadrasi="no",
    table_name="tbltempheiat",
    info=None,
    serious=False,
    *args,
    **kwargs,
):
    info = info or {'assigned_to': '', 'msg': ''}
    time.sleep(1)
    step = 1

    try:
        # Close any lingering error popup
        driver = click_popup(driver, "flexPopupErrMsgBtn")

        step = step_log(step, "Inserting initial values...")
        driver, info = insert_value(driver=driver, item=item, info=info)

        step = step_log(step, "Clicking search button...")
        driver, info = run_search_until_value(
            driver, info, max_retries=15, wait_between=15)

        step = step_log(step, "Waiting for popup...")
        driver = wait_for_popup(driver)

        step = step_log(step, "Fetching shenase_dadrasi info...")
        driver, info = get_info_shenase_dadrasi(driver=driver, info=info)

        # Early skip conditions
        if (
            not info
            or info.get("shenase_dadrasi_num") == '0'
            or len(info.get("shenase_dadrasi_num", "")) > 4
            or (
                df[1]['employee'].iloc[0] == info['assigned_to']
                and role != 'employee'
            )
        ):

            return driver, info

        step = step_log(step, "Selecting row...")
        driver, info = select_row(driver=driver, info=info)

        step = step_log(step, f"Processing role: {role}")
        driver, info = process_by_role(
            driver, role, index, df, item, table_name, info)

        if role == 'employee':
            try:
                driver, info = go_to_next_frame(
                    driver, role, shenase_dadrasi, info)
                submit_locator = (
                    By.XPATH, "//input[@class='iconBtn submit_icon' and @title='ارسال']")
                # Wait until the button is clickable
                submit_button = WebDriverWait(driver, 2).until(
                    EC.element_to_be_clickable(submit_locator)
                )
                # Click the button
                submit_button.click()

                step = step_log(step, "Waiting for popup...")
                driver = wait_for_popup(driver)

                step = step_log(step, "Clicking search button...")
                driver, info = run_search_until_value(
                    driver, info, max_retries=15, wait_between=15)

                step = step_log(step, "Waiting for popup...")
                driver = wait_for_popup(driver)

                step = step_log(step, "Fetching shenase_dadrasi info...")
                driver, info = get_info_shenase_dadrasi(
                    driver=driver, info=info)
            except:
                driver, info = check_if_processed_by_else(
                    driver, info, check_type='employee')
            return driver, info

        if info.get("vaziat") == "باز":
            step = step_log(step, "Handling 'باز' case...")
            driver, info = handle_opened_case(
                driver, role, shenase_dadrasi, info, df[1]['employee'].iloc[0])
            return driver, info

        if info.get("vaziat") == "ثبت شده":
            step = step_log(step, "Handling 'ثبت شده' case...")
            driver, info = handle_registered_case(
                driver, role, shenase_dadrasi, info)
            return driver, info
        else:
            step = step_log(
                step, "Case is not 'ثبت شده', skipping further processing.", "WARNING")

        step = step_log(step, "Clicking assign button...")
        driver, info = assign_btn(
            driver=driver, role=role, shenase_dadrasi=shenase_dadrasi, info=info)

        step = step_log(step, "Scraping completed successfully!", "SUCCESS")
        return driver, info

    except Exception as e:
        log_step(step, f"Error occurred: {e}", "ERROR")
        if info['msg'] == "خطایی در سرویس سامانه دادرسی رخ داده است.":
            driver = click_popup(driver, "flexPopupFailMsgBtn")
        return driver, info

    finally:
        # Always update SQL at the end, regardless of the path
        update_sql(table_name, info, item)


def process_by_role(driver, role, index, df, item, table_name, info):
    """Handles role-specific processing logic."""
    if role == "manager_phase1":
        return process_manager_phase1(driver, df, info, role)
    if role == "manager_phase2":
        return process_manager_phase2(driver, df, info, role)
    elif role == "employee":
        return process_employee(driver, index, df, item, role, table_name, info)
    return driver, info


def check_if_processed_by_else(driver, info, check_type="button"):
    """
    Consolidated function to check if task was processed by another user.

    Args:
        check_type: "button" for flexPopupErrMsgBtn check, "span" for span.win-float-msg-content check
    """
    try:
        if check_type == "button":
            driver.switch_to.default_content()
            # wait for the button to be clickable, then click it
            button = WebDriverWait(driver, 20).until(
                EC.element_to_be_clickable((By.ID, "flexPopupErrMsgBtn"))
            )
            button.click()
        elif check_type == "span":
            element = WebDriverWait(driver, 2).until(
                EC.visibility_of_element_located(
                    (By.CSS_SELECTOR, "span.win-float-msg-content"))
            )
            text = element.text
            print("Captured text:", text)
            driver.switch_to.default_content()

        # Switch back to frame
        elif check_type == "employee":
            try:
                driver.switch_to.default_content()
                handle_error_popup(driver, info)
            except:
                button = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.XPATH, "//input[@title='ارسال']")))
                button.click()

        # If we reach here, element was found -> raise
        raise Exception("Task already processed by another user!")

    except TimeoutException:
        # If element not found -> continue normally
        return driver, info


# Removed wrapper functions - use direct function calls instead


def handle_post_processing_cleanup(driver, info):
    """Handles cleanup operations after processing, including error handling."""
    driver.switch_to.default_content()
    log_step("RC4", "Checking for error popup...")

    try:
        time.sleep(1)
        handle_error_popup(driver, info)
    except Exception:
        wait_and_switch_to_iframe(driver)

    return driver, info


# Legacy function name for backward compatibility
def handle_registered_case(driver, role, shenase_dadrasi, info):
    """Processes cases with status 'ثبت شده'. 

    This function calls the individual workflow functions directly.
    """
    try:
        log_step("RC1", "Clicking process button...")
        driver, info = click_on_btnprocess(driver, info)
        driver, info = check_if_processed_by_else(driver, info, "span")
        driver, info = check_if_processed_by_else(driver, info, "button")
        log_step("RC2", "Switching to next frame...")
        driver, info = go_to_next_frame(driver, role, shenase_dadrasi, info)
        log_step("RC3", "Clicking accept button...")
        driver, info = click_on_accept(driver, info)
        driver, info = handle_post_processing_cleanup(driver, info)
        return driver, info
    except:
        return driver, info


def handle_opened_case(driver, role, shenase_dadrasi, info, employee_shenase):
    """Processes cases with status 'ثبت شده'. 

    This function calls the individual workflow functions directly.
    """
    try:
        log_step("RC1", "Clicking process button...")
        driver, info = click_on_btnassignment(driver, info)
        # driver, info = check_if_processed_by_else(driver, info, "span")
        # driver, info = check_if_processed_by_else(driver, info, "button")
        log_step("RC2", "Switching to next frame...")
        driver, info = go_to_next_frame(driver, role, shenase_dadrasi, info)
        log_step("RC3", "Clicking accept button...")
        driver, info = click_on_input_assgnment(driver, info, employee_shenase)
        driver, info = click_submit_button(driver, info, employee_shenase)
        return driver, info
    except:
        return driver, info


def handle_error_popup(driver, info):
    """Checks for error popups, clicks, and closes them."""
    if WebDriverWait(driver, 12).until(EC.element_to_be_clickable((By.XPATH, '//*[@id="flexPopupErrMsgBtn"]'))):
        info['msg'] = WebDriverWait(driver, 36).until(
            EC.element_to_be_clickable((By.XPATH, '/html/body/div[8]/p'))
        ).text

        element = WebDriverWait(driver, 36).until(
            EC.element_to_be_clickable((By.XPATH, '//*[@id="flexPopupErrMsgBtn"]')))

        element.click()

        elements = driver.find_elements(
            By.XPATH, '/html/body/div[2]/div[3]/table/tbody/tr/td/ul/li[3]/a[2]')
        if elements:
            driver.execute_script("arguments[0].click();", elements[0])

        wait_and_switch_to_iframe(driver)
    return info


def wait_and_switch_to_iframe(driver):
    """Waits for the main iframe and switches to it."""
    WebDriverWait(driver, 32).until(
        EC.frame_to_be_available_and_switch_to_it(
            (By.XPATH, COMMON_XPATHS['main_iframe'])
        )
    )
    log_step("IFR", "Switched to iframe successfully!", "SUCCESS")
    return driver
