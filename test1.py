import traceback

try:
    # Your main code here
    # Example: your existing code
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    import pandas as pd
    from tqdm import tqdm
    import time

    from selenium import webdriver
    from selenium.webdriver.chrome.service import Service as ChromeService
    from selenium.webdriver.chrome.options import Options as ChromeOptions
    from selenium.webdriver.firefox.service import Service as FirefoxService
    from selenium.webdriver.firefox.options import Options as FirefoxOptions
    from selenium.webdriver.common.by import By
    import time
    from contextlib import contextmanager
    from functools import wraps

    def url_logger(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            start_time = time.time()  # Record the start time
            print(f"\nscraping {kwargs['url']}")
            result = func(*args, **kwargs)  # Execute the function
            end_time = time.time()  # Record the end time
            execution_time = end_time - start_time  # Calculate the execution time
            # print(f"It took {execution_time:.4f} seconds")
            return result
        return wrapper

    from contextlib import contextmanager

    @contextmanager
    def init_driver(pathsave, driver_type='firefox', headless=False, prefs={'maximize': False, 'zoom': '1.0'}, driver=None,
                    info={}, use_proxy=False, disable_popups=False, *args, **kwargs):
        """
        Context manager for initializing and managing a Selenium WebDriver.

        Args:
        - pathsave (str): The directory path where downloaded files will be saved.
        - driver_type (str): Type of the WebDriver ('chrome' or 'firefox'), default is 'firefox'.
        - headless (bool): Whether to run the browser in headless mode, default is False.
        - prefs (dict): Dictionary of preferences for the WebDriver.
        - driver: Existing WebDriver instance, if provided.
        - info (dict): Additional information, if needed.
        - *args, **kwargs: Additional arguments and keyword arguments.

        Yields:
        - driver: The initialized WebDriver instance.

        Usage:
        with init_driver(pathsave='/path/to/downloads', driver_type='chrome') as driver:
            # Your code using the WebDriver goes here
        """

        if driver_type == 'chrome':
            # Configuring options for Chrome WebDriver
            options = ChromeOptions()
            options.add_argument(
                "--disable-blink-features=AutomationControlled")
            options.add_argument(
                "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36")
            options.add_argument('--no-sandbox')
            options.add_argument('--disable-dev-shm-usage')
            options.add_argument('--remote-debugging-port=9222')
            if headless:
                options.add_argument('--headless')
            prefs = {'download.default_directory': pathsave}
            options.add_argument("start-maximized")
            options.add_experimental_option(
                "excludeSwitches", ["enable-automation"])
            options.add_experimental_option('useAutomationExtension', False)
            options.add_experimental_option(
                "prefs", {"download.default_directory": pathsave})

            # Initialize Chrome WebDriver
            service = ChromeService()
            driver = webdriver.Chrome(service=service, options=options)

        elif driver_type == 'firefox':
            # Configuring options for Firefox WebDriver
            options = FirefoxOptions()

            # Set Firefox preferences
            # Use custom download folder
            options.set_preference("browser.download.folderList", 2)
            options.set_preference(
                "browser.download.manager.showWhenStarting", False)
            options.set_preference(
                "browser.helperApps.neverAsk.saveToDisk", "application/pdf")  # Auto-download PDFs
            # Disable built-in PDF viewer
            options.set_preference("pdfjs.disabled", True)

            if headless:
                options.add_argument("--headless")

            if prefs.get('maximize'):
                options.add_argument("--start-maximized")

            # Initialize Firefox WebDriver
            service = FirefoxService(executable_path='geckodriver.exe')
            driver = webdriver.Firefox(service=service, options=options)

        else:
            raise ValueError(f"Unsupported driver type: {driver_type}")

        try:
            # Set the page load timeout to 35 seconds
            driver.set_page_load_timeout(35)
            yield driver  # Provide the initialized WebDriver to the user

        finally:
            driver.quit()  # Ensure WebDriver is properly closed after use

        URL = 'https://trace.ivo.ir/CheckBarcode.aspx'
        df = pd.read_excel(r'sample.xlsx')

        @url_logger
        def go_to_url(driver, info, url=URL):
            try:
                driver.get(url)
                time.sleep(3)
            except:
                ...

            return driver, info

    with init_driver(pathsave=None, driver_type='firefox', headless=False) as driver:
        driver, info = go_to_url(driver=driver, info={}, url=URL)
        wait = WebDriverWait(driver, 10)

        vahed_dami = []
        vahed_takhlie = []
        barnums = []

        for barnum in tqdm(df['barnum']):
            barnums.append(barnum)
            wait.until(EC.visibility_of_element_located(
                (By.NAME, "txtImpBarcode"))).send_keys(barnum)
            wait.until(EC.visibility_of_element_located(
                (By.NAME, "btnCheckBarcode"))).click()
            vahed_dami.append(wait.until(
                EC.visibility_of_element_located((By.ID, "lblFarmName"))).text)
            vahed_takhlie.append(wait.until(
                EC.visibility_of_element_located((By.ID, "lblDownloadFarm"))).text)
            new_df = pd.DataFrame({'بارکد': barnums,
                                   'واحد دامي/قرنطينه': vahed_dami,
                                   'واحد تخليه': vahed_takhlie})
            new_df = new_df.astype('str')
            new_df.to_excel('final.xlsx')
            wait.until(EC.visibility_of_element_located(
                (By.NAME, "txtImpBarcode"))).clear()

except Exception as e:
    print("An error occurred:", e)
    traceback.print_exc()  # Print the detailed error stack trace
    input("Press Enter to close the program...")  # Keep the window open
