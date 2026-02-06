from automation.helpers import init_driver, log_the_func, remove_excel_files
from automation.constants import TIMEOUT_15

class BaseScraper:
    def __init__(self, path=None, driver_type='firefox', headless=True, info={}, time_out=180):
        self.path = path
        self.driver_type = driver_type
        self.headless = headless
        self.info = info
        self.time_out = time_out
        self.driver = None

    def init_driver(self, path=None, headless=None):
        if path is None:
            path = self.path
        if headless is None:
            headless = self.headless
        
        return init_driver(pathsave=path, driver_type=self.driver_type, headless=headless)

    def close_driver(self):
        if self.driver:
            try:
                self.driver.quit()
            except:
                pass
            self.driver = None
