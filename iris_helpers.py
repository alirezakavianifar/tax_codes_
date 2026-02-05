import pyodbc
import pandas as pd
import time
import os
from datetime import datetime
from functools import wraps
from contextlib import contextmanager
from selenium import webdriver
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.firefox.options import Options as FirefoxOptions
import socket
from pathlib import Path

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


import time
from functools import wraps


def retry_decorator(
    max_retries=5,
    delay=1,
    driver_based=False,
    close_driver=False,
    custom_cleanup=None,
    use_global_retries=False,
    tuple_result_raises=False,
    custom_error_msg=None
):
    """
    Unified retry decorator with driver cleanup, custom cleanup, and logging support.
    """

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            nonlocal max_retries, delay
            retry_count = 0
            last_exception = None

            def cleanup_driver():
                """Helper to close/quit driver safely if needed."""
                driver = kwargs.get("driver", None)
                if not driver:
                    for arg in args:
                        if hasattr(arg, "close") and hasattr(arg, "quit"):
                            driver = arg
                            break
                if driver:
                    try:
                        driver.close()
                    except:
                        try:
                            driver.quit()
                        except:
                            pass

            while retry_count < max_retries:
                try:
                    result = func(*args, **kwargs)

                    # If tuple result should trigger retry (legacy behavior)
                    if tuple_result_raises and isinstance(result, tuple):
                        raise ValueError(
                            "Tuple result detected, triggering retry")

                    return result

                except Exception as e:
                    last_exception = e
                    retry_count += 1

                    # Optional global retry tracking
                    if use_global_retries:
                        global n_retries
                        n_retries = retry_count

                    # Print custom or default error message
                    error_message = custom_error_msg or f"Attempt {retry_count} failed: {e}"
                    print(error_message)

                    # Driver cleanup if required
                    if driver_based and close_driver:
                        cleanup_driver()

                    # Execute custom cleanup callback if provided
                    if custom_cleanup:
                        try:
                            custom_cleanup(*args, **kwargs)
                        except Exception as cleanup_error:
                            print(f"Custom cleanup failed: {cleanup_error}")

                    # Sleep before next retry
                    if retry_count < max_retries:
                        print(f"Retrying in {delay} seconds...")
                        time.sleep(delay)

            # If all retries failed, raise last exception with context
            raise RuntimeError(
                f"Function {func.__name__} failed after {max_retries} attempts"
            ) from last_exception

        return wrapper
    return decorator


def get_ip():
    host_name = socket.gethostname()
    ip_addr = socket.gethostbyname(host_name)

    return ip_addr


def geck_location(set_save_dir=False, driver_type='firefox'):
    """
    Returns appropriate path for geckodriver/chromedriver or saved_dir
    based on IP address and driver type.
    """
    ip_addr = get_ip()

    # Configurable paths for different environments
    paths = {
        "10.52.0.114": {
            "save_dir": r"E:\automating_reports_V2\saved_dir",
            "chrome": r"E:\automating_reports_V2\chromedriver.exe",
            "firefox": r"E:\AUTOMATING_REPORTS_V2\geckodriver.exe"
        },
        "default": {
            "save_dir": r"H:\پروژه اتوماسیون گزارشات\monthly_reports\saved_dir",
            "chrome": r"E:\automating_reports_V2\chromedriver.exe",
            "firefox": r"E:\AUTOMATING_REPORTS_V2\geckodriver.exe"
        }
    }

    env_paths = paths.get(ip_addr, paths["default"])

    # If only saved_dir requested
    if set_save_dir:
        return env_paths["save_dir"]

    # Return appropriate driver path based on type
    if driver_type == "chrome":
        return env_paths["chrome"]
    elif driver_type == "firefox":
        return env_paths["firefox"]

    # Default fallback
    return str(Path(os.getcwd()) / "geckodriver.exe")


def sql_delete(table):

    return """
        BEGIN TRANSACTION
                BEGIN TRY 
            
                    IF Object_ID('%s') IS NOT NULL DROP TABLE %s
            
                    COMMIT TRANSACTION;
                    
                END TRY
                BEGIN CATCH
                    ROLLBACK TRANSACTION;
                END CATCH
        """ % (table, table)


def replace_last(phrase, strToReplace=',', replacementStr=''):

    # Search for the last occurrence of substring in string
    pos = phrase.rfind(strToReplace)
    if pos > -1:
        # Replace last occurrences of substring 'is' in string with 'XX'
        phrase = phrase[:pos] + replacementStr + \
            phrase[pos + len(strToReplace):]

    return phrase


def insert_into(table, columns=None):

    temp = ''
    values = ''

    for c in columns:
        temp += '[%s],' % c

        values += '?,'

    values = replace_last(values)
    temp = replace_last(temp)
    # temp = temp.replace('[تاریخ بروزرسانی],', '[تاریخ بروزرسانی]', 1)

    sql_insert = """
    BEGIN TRANSACTION
    BEGIN TRY 

        INSERT INTO %s
        (
        %s         
            )
        
        VALUES
        (%s)

        COMMIT TRANSACTION;
        
    END TRY
    BEGIN CATCH
        ROLLBACK TRANSACTION;
    END CATCH
    """ % (table, temp, values)

    return sql_insert


def create_sql_table(table, columns):

    temp = ''

    for c in columns:
        temp += '[%s] NVARCHAR(MAX) NULL,\n' % c

    sql_query = """
    BEGIN TRANSACTION
    BEGIN TRY 

        IF Object_ID('%s') IS NULL
        
        CREATE TABLE %s
        (
         [ID] [int] IDENTITY(1,1) NOT NULL,
         PRIMARY KEY (ID),
         %s
                                  
       
         )

        COMMIT TRANSACTION;
        
    END TRY
    BEGIN CATCH
        ROLLBACK TRANSACTION;
    END CATCH
    """ % (table, table, temp)

    # time.sleep(400)
    return sql_query


def unified_logger(
    log_file_path=None,
    log_to_db=False,
    db_config=None,
    log_timing=True,
    log_special_params=False,
    custom_message=None,
    *log_args,
    **log_kwargs
):
    """
    Unified logging decorator for timing, file logging, DB logging, and custom messages.
    """

    def wrapper(func):
        @wraps(func)
        def try_it(*args, **kwargs):
            start_time = datetime.now()
            func_name = func.__name__

            def write_log(message, to_console=True):
                """Writes message to console and/or file."""
                timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                log_msg = f"[{timestamp}] {message}"
                if to_console:
                    print(log_msg)
                if log_file_path:
                    with open(log_file_path, "a", encoding="utf-8") as f:
                        f.write(log_msg + "\n")

            # Skip logging if explicitly disabled
            if kwargs.get("log") is False:
                return func(*args, **kwargs)

            # Starting log
            custom_msg = custom_message or f"Starting function {func_name}"
            write_log(custom_msg)

            # Call the actual function with error handling
            try:
                result = func(*args, **kwargs)
            except Exception as e:
                write_log(
                    f"❌ Error in function {func_name}: {e}", to_console=True)
                raise  # Re-raise after logging

            # Timing info
            if log_timing:
                duration = (datetime.now() - start_time).total_seconds()
                write_log(
                    f"⏱ Function {func_name} took {duration:.2f} seconds")

            # Special parameter logging (if enabled)
            if log_special_params and len(args) >= 2:
                rahgiri, success = args[:2]
                df = pd.DataFrame([[rahgiri, success]], columns=[
                                  "rahgiri", "success"])
                write_log(
                    f"Special params logged: {df.to_dict(orient='records')}")

            # Database logging placeholder
            if log_to_db and db_config:
                # Implement DB logic here if needed
                write_log(f"DB logging for {func_name} not yet implemented")

            # Completion log
            write_log(f"✅ Function {func_name} completed successfully")
            return result

        return try_it
    return wrapper


@contextmanager
def init_driver(
    pathsave,
    driver_type="firefox",
    headless=False,
    prefs=None,
    driver=None,
    info=None,
    use_proxy=False,
    disable_popups=False,
    proxy_address="10.52.3.128",
    proxy_port=8080,
    *args, **kwargs
):

    prefs = prefs or {"maximize": False, "zoom": "1.0"}
    info = info or {}

    driver = None
    try:
        if driver_type.lower() == "chrome":
            driver = _init_chrome_driver(pathsave, headless)
        else:
            driver = _init_firefox_driver(
                pathsave, headless, prefs, use_proxy, disable_popups, proxy_address, proxy_port)

        if prefs.get("maximize", False):
            driver.maximize_window()

        driver.switch_to.window(driver.window_handles[0])
        yield driver

    finally:
        if driver:
            driver.quit()


def _init_chrome_driver(pathsave, headless):
    options = ChromeOptions()
    options.add_argument("start-maximized")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)
    options.add_experimental_option(
        "prefs", {"download.default_directory": pathsave})

    if headless:
        options.add_argument("--headless=new")

    return webdriver.Chrome(executable_path=geck_location(driver_type="chrome"), options=options)


def _init_firefox_driver(pathsave, headless, prefs, use_proxy, disable_popups, proxy_address, proxy_port):
    profile = webdriver.FirefoxProfile()
    _set_firefox_prefs(profile, pathsave, prefs, use_proxy,
                       disable_popups, proxy_address, proxy_port)

    options = FirefoxOptions()
    if headless:
        options.headless = True

    return webdriver.Firefox(firefox_profile=profile, executable_path=geck_location(), options=options)


def _set_firefox_prefs(profile, pathsave, prefs, use_proxy, disable_popups, proxy_address, proxy_port):
    # Download settings
    profile.set_preference("browser.download.folderList", 2)
    profile.set_preference("browser.download.manager.showWhenStarting", False)
    profile.set_preference("browser.download.dir", pathsave)
    profile.set_preference("browser.helperApps.neverAsk.openFile",
                           "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet,application/octet-stream")
    profile.set_preference("browser.helperApps.neverAsk.saveToDisk",
                           "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet,application/octet-stream;application/excel")
    profile.set_preference("browser.helperApps.alwaysAsk.force", False)
    profile.set_preference("layout.css.devPixelsPerPx",
                           prefs.get("zoom", "1.0"))

    # Proxy
    if use_proxy:
        profile.set_preference("network.proxy.type", 1)
        profile.set_preference("network.proxy.http", proxy_address)
        profile.set_preference("network.proxy.http_port", proxy_port)
        profile.set_preference("network.proxy.ssl", proxy_address)
        profile.set_preference("network.proxy.ssl_port", proxy_port)

    # Popup blocking
    if disable_popups:
        profile.set_preference("dom.popup_maximum", 0)
        profile.set_preference("privacy.popups.disable_from_plugins", 3)
        profile.set_preference("dom.popup_allowed_events", "")


def create_directories(paths, exist_ok=True):
    # Normalize input to list
    if isinstance(paths, (str, bytes)):
        paths = [paths]
    elif not isinstance(paths, (list, tuple)):
        raise TypeError("paths must be a string, list, or tuple")

    # Create each directory
    for path in paths:
        if path:  # Skip empty strings
            os.makedirs(path, exist_ok=exist_ok)


def get_sql_con(server='10.52.0.116', database='testdb', username='sa', password='Y@zd116'):

    constr = 'DRIVER={SQL Server};SERVER=' + server + ';DATABASE=' + \
        database + ';UID=' + username + ';PWD=' + password

    return constr


def drop_into_db(
    table_name=None,
    columns=None,
    values=None,
    append_to_prev=False,
    db_name="TestDb",
    del_tbl=None,
    create_tbl=None,
    sql_query=None,
    drop=True
):
    sql_con = get_sql_con(database=db_name)

    def delete_table_if_needed():
        if not append_to_prev and (del_tbl is None or del_tbl is True):
            print(f"🗑 Dropping table {table_name}...")
            delete_query = sql_delete(table_name)
            connect_to_sql(delete_query, sql_con=sql_con,
                           connect_type="dropping sql table")

    def create_table_if_needed():
        if not append_to_prev and (create_tbl is None or create_tbl == "yes"):
            print(f"🆕 Creating table {table_name}...")
            create_query = create_sql_table(table_name, columns)
            connect_to_sql(create_query, sql_con=sql_con,
                           connect_type="creating sql table")

    def insert_data():
        if drop:
            final_query = sql_query or insert_into(table_name, columns)
            print(f"📥 Inserting data into {table_name}...")
            connect_to_sql(final_query, sql_con=sql_con, df_values=values,
                           connect_type="inserting into sql table")

    # --- Main Flow ---
    delete_table_if_needed()
    create_table_if_needed()
    insert_data()


@unified_logger(log_timing=True, log_to_db=False)
def connect_to_sql(
    sql_query,
    sql_con=None,
    df_values=None,
    read_from_sql=False,
    connect_type=None,
    return_df=False,
    chunk_size=None,
    return_df_as="dataframe",
    num_runs=12,
    *args,
    **kwargs
):
    """
    Executes SQL queries with retry logic, optional DataFrame return, and batch execution support.
    """

    if sql_con is None:
        sql_con = get_sql_con()  # Default connection string

    def execute_query():
        """Executes the SQL query or batch operations."""
        with pyodbc.connect(sql_con) as cnxn:
            cursor = cnxn.cursor()

            if read_from_sql:
                # Return a DataFrame (with or without chunks)
                return pd.read_sql(sql_query, cnxn, chunksize=chunk_size)

            if df_values is None:
                cursor.execute(sql_query)
            else:
                cursor.executemany(sql_query, df_values)

            cursor.execute("commit")

    # Retry loop
    for attempt in range(1, num_runs + 1):
        try:
            result = execute_query()

            # Return data if requested
            if return_df and read_from_sql:
                if return_df_as == "json":
                    return result.to_json(orient="records", force_ascii=False)
                elif return_df_as == "dict":
                    return result.to_dict(orient="records")
                else:
                    return result

            return result
        except Exception as e:
            print(f"❌ SQL execution failed on attempt {attempt}: {e}")
            if attempt < num_runs:
                print("⏳ Retrying in 4 seconds...")
                time.sleep(4)
            else:
                raise RuntimeError(
                    f"SQL query failed after {num_runs} attempts") from e


def check_if_login_iris_user_pass_inserted(driver, creds, timeout=2):
    username_field = WebDriverWait(driver, timeout).until(
        EC.element_to_be_clickable((By.NAME, "userName"))
    )
    password_field = WebDriverWait(driver, timeout).until(
        EC.element_to_be_clickable((By.NAME, "password"))
    )

    if (username_field.get_attribute("value") == creds["username"] and
            password_field.get_attribute("value") == str(creds["pass"])):
        return driver
    else:
        raise Exception(
            "❌ Username/password not correctly inserted into the form")


def insert_login_iris_user_pass(driver, creds, info):

    username_field = WebDriverWait(driver, 2).until(
        EC.element_to_be_clickable((By.NAME, "userName")))
    username_field.clear()
    username_field.send_keys(creds['username'])

    password_field = WebDriverWait(driver, 2).until(
        EC.element_to_be_clickable((By.NAME, "password")))
    password_field.clear()
    password_field.send_keys(creds['pass'])

    driver = check_if_login_iris_user_pass_inserted(driver, creds)

    return driver, info


def find_obj_login(driver, info):
    """Backward compatibility wrapper for login_website."""
    # This is a specific action for IRIS login - click the OBJ element
    WebDriverWait(driver, 15).until(
        EC.element_to_be_clickable((By.ID, 'OBJ'))
    )
    return driver, info


def _login_iris_custom(driver, creds, info, max_retries=40):
    """Handle IRIS login with multi-step process with retry."""
    driver.get("https://its.tax.gov.ir/flexform2/logineris/form2login.jsp")
    time.sleep(4)

    for attempt in range(max_retries):
        # Insert username and password
        driver, info = insert_login_iris_user_pass(
            driver=driver, creds=creds, info=info)
        time.sleep(2)

        # Click login button
        WebDriverWait(driver, 3).until(
            EC.element_to_be_clickable((By.ID, 'ok_but'))
        ).click()
        time.sleep(0.5)

        # Handle possible alert (login failure)
        try:
            alert_obj = driver.switch_to.alert
            alert_obj.accept()
            time.sleep(5)
            print(f"Login attempt {attempt+1} failed, retrying...")
        except:
            # No alert -> success
            driver, info = find_obj_login(driver=driver, info=info)
            return driver, info

    # If all retries fail, raise exception
    raise Exception("Login failed after maximum retries")


def login_iris(driver, creds=None, info={'success': True}):
    """Backward compatibility wrapper for IRIS login."""
    return _login_iris_custom(driver, creds, info)
