from selenium import webdriver
from selenium.webdriver.common.by import By
from constants import geck_location_remote, set_gecko_prefs


class Login:
    def __init__(self, pathsave):
        self.pathsave = pathsave
        fp = set_gecko_prefs(pathsave)
        self.driver = webdriver.Firefox(
            fp, executable_path=geck_location_remote())
        self.driver.window_handles
        self.driver.switch_to.window(self.driver.window_handles[0])

    def __call__(self):
        return self.driver

    def close(self):
        self.driver.close()


def login_sanim(driver):
    driver.get("https://mgmt.tax.gov.ir/ords/f?p=100:101:16540338045165:::::")
    driver.implicitly_wait(20)
    txtUserName = driver.find_element(By.ID,
        'P101_USERNAME').send_keys('1971385018')
    txtPassword = driver.find_element(By.ID, 
        'P101_PASSWORD').send_keys('123456')

    driver.find_element(By.ID, 'B1700889564218640').click()

    return driver


def login_tgju(driver):
    driver.get("https://www.tgju.org/")
    driver.implicitly_wait(20)

    return driver
