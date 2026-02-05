from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver import ActionChains
import time
import glob
import os.path
import xlwings as xw
import pyodbc
#from xlutils.copy import copy
import xlrd
import pandas as pd
import datetime
import pandas as pd
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException
from tqdm import tqdm
from logger import log_it


@log_it
def log(row, success):
    print('log is called')
#

# filelist = glob.glob(os.path.join('C:\Temp\EtebarSanji', "*.xls"))
# for f in filelist:
#     os.remove(f)

# filelist = glob.glob(os.path.join('C:\Temp\EtebarSanji', "*.xlsx"))
# for f in filelist:
#     os.remove(f)
# پاک کردن فایل های اکسل پوشه temp

# تنظیمات وب درایور فایرفاکس
fp = webdriver.FirefoxProfile()
fp.set_preference('browser.download.folderList', 2)
fp.set_preference('browser.download.manager.showWhenStarting', False)
fp.set_preference('browser.download.dir', r'C:\Temp\EtebarSanji\\')

fp.set_preference('browser.helperApps.neverAsk.openFile',
                  'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet,application/octet-stream')
fp.set_preference('browser.helperApps.neverAsk.saveToDisk',
                  'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet,application/octet-stream')
fp.set_preference('browser.helperApps.alwaysAsk.force', False)
fp.set_preference('browser.download.manager.alertOnEXEOpen', False)
fp.set_preference('browser.download.manager.focusWhenStarting', False)
fp.set_preference('browser.download.manager.useWindow', False)
fp.set_preference('browser.download.manager.showAlertOnComplete', False)
fp.set_preference('browser.download.manager.closeWhenDone', False)

driver = webdriver.Firefox(fp, executable_path="H:\driver\geckodriver.exe")
driver.window_handles
driver.switch_to.window(driver.window_handles[0])
#

# اجرای فرایند اتوماسیون
driver.get("http://management.tax.gov.ir/Public/Login")
driver.implicitly_wait(5)
txtUserName = driver.find_element(By.ID, 'TextBoxUsername').send_keys('1756914443')
txtPassword = driver.find_element(By.ID, 'TextboxPassword').send_keys('1756914443')
txtPassword = driver.find_element(By.NAME, 'ButtonLogin').click()
time.sleep(1)
driver.find_element(By.XPATH, "/html/body/form/center/div[1]/table/tbody/tr[2]/td[1]/a[1]/div").click()
time.sleep(1)

# خواندن فایل اکسل
df = pd.read_excel(r"D:\baghmalek.xlsx")

df_1 = df.iloc[:100,:]
df_2 = df.iloc[101:200,:]
df_3 = df.iloc[201:300,:]
df_4 = df.iloc[301:400,:]
df_5 = df.iloc[401:500,:]
df_6 = df.iloc[501:600,:]
df_7 = df.iloc[601:700,:]
df_8 = df.iloc[701:800,:]
df_9 = df.iloc[801:,:]

df_6.status=""

for index, row in tqdm(df.iterrows()):
    try:
        driver.find_element(By.XPATH, "/html/body/form/table/tbody/tr[1]/td[2]/div/div/table/tbody/tr[2]/td[1]/input").send_keys(str(row["rahgiri"]))
        driver.find_element(By.XPATH, "/html/body/form/table/tbody/tr[1]/td[2]/div/div/table/tbody/tr[2]/td[2]/a/span").click() 
        
        if driver.find_element(By.XPATH, "/html/body/form/table/tbody/tr[2]/td[2]/span/table/tbody/tr[2]/td[7]/span").text =="غيرفعال":
            log(row["rahgiri"], 'قبلا غیر فعال شده است')
            continue
        
        time.sleep(1)
        try:
            if (driver.find_element(By.XPATH,"//span[contains(text(),'نمایش شناسنامه')]"):
                
                driver.find_element(By.XPATH,"//span[contains(text(),'نمایش شناسنامه')]").click()
        except:
            log(row['rahgiri'], 'پرونده ای موجود نیست')
            continue
        driver.find_element(By.XPATH, "/html/body/form/table/tbody/tr[2]/td[2]/span[3]/table[1]/tbody/tr/td[7]/a/div").click()
        driver.find_element(By.XPATH, "/html/body/form/table/tbody/tr[2]/td[2]/span[3]/table[2]/tbody/tr/td/div/div[7]/table[1]/tbody/tr[3]/td[1]/a/div").click()
        driver.find_element(By.XPATH, '//*[@id="CPC_TextboxDisableTaxpayerDateFa"]').send_keys("1401/07/17")
        driver.find_element(By.XPATH, "/html/body/form/table/tbody/tr[2]/td[2]/div/table/tbody/tr[2]/td[2]/div/a/span").click()
        driver.find_element(By.XPATH, '/html/body/form/table/tbody/tr[2]/td[2]/div/table/tbody/tr[2]/td[2]/div/div/div/input').send_keys("سایر موارد")
        driver.find_element(By.XPATH, '/html/body/form/table/tbody/tr[2]/td[2]/div/table/tbody/tr[2]/td[2]/div/div/div/input').send_keys(Keys.RETURN)
        driver.find_element(By.XPATH, '/html/body/form/table/tbody/tr[2]/td[2]/div/table/tbody/tr[3]/td[2]/input').send_keys("با توجه به نامه شماره 22213 و با درخواست اداره مربوطه غیرفعال گردید")
        driver.find_element(By.XPATH, '//*[@id="CPC_CheckBoxDisableTaxpayer"]').click()
        driver.find_element(By.XPATH, '//*[@id="CPC_ButtonDisableTaxpayer"]').click()
        time.sleep(4)
        
        try:
            if (driver.find_element(By.XPATH, '/html/body/form/table/tbody/tr[2]/td[2]/span[2]/div"]')):
                log(row['rahgiri'], 'به علت اتصال دستگاه کارتخوان امکان غیر فعالسازی نیست')
        except:
            log(row['rahgiri'], 'success')
    except Exception as e:
        log(row["rahgiri"], 'failure')
        print(e)
        time.sleep(1)
        continue
        

    time.sleep(1)
    
driver.close()    

    

