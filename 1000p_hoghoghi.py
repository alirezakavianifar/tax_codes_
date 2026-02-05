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
from logger_h import log_it
from selenium.webdriver.common.alert import Alert
from constants import geck_location



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

driver = webdriver.Firefox(fp, executable_path=geck_location())
driver.window_handles
driver.switch_to.window(driver.window_handles[0])
#

# اجرای فرایند اتوماسیون
driver.get("http://management.tax.gov.ir/Public/Login")
driver.implicitly_wait(5)
txtUserName = driver.find_element(By.ID, 'username').send_keys('1756914443')
txtPassword = driver.find_element(By.ID, 'Password').send_keys('1756914443')
time.sleep(10)
driver.find_element(By.CLASS_NAME,'button').click()
time.sleep(1)
# driver.find_element(By.XPATH, "/html/body/form/center/div[1]/table/tbody/tr[2]/td[1]/a[1]/div").click()
# time.sleep(1)

# خواندن فایل اکسل
df = pd.read_excel(r"C:\ezhar-temp\1000p-hoghoghi.xlsx")

# df_1 = df.iloc[:100,:]
# df_2 = df.iloc[101:300,:]
# df_3 = df.iloc[1500:1800,:]
# df_4 = df.iloc[900:1200,:]
# df_5 = df.iloc[401:700,:]
# df_6 = df.iloc[600:800,:]
# df_7 = df.iloc[1800:2000,:]
# df_8 = df.iloc[701:800,:]
# df_9 = df.iloc[1998:,:]

# df_6.status=""
for index, row in tqdm(df.iterrows()):
    try:
        driver.find_element(By.ID, "TextboxPublicSearch").send_keys(str(row["rahgiri"]))
        time.sleep(0.2)
        driver.find_element(By.XPATH, "/html/body/form/table/tbody/tr[1]/td[2]/div/div/table/tbody/tr[2]/td[2]/a/span").click() 
        time.sleep(0.2)
        rahgiri = driver.find_element(By.XPATH, "/html/body/form/table/tbody/tr[2]/td[2]/span/table[1]/tbody/tr[2]/td[1]").text 
        
        driver.find_element(By.ID, "TextboxPublicSearch").send_keys(rahgiri)
        time.sleep(0.2)
        driver.find_element(By.XPATH, "/html/body/form/table/tbody/tr[1]/td[2]/div/div/table/tbody/tr[2]/td[2]/a/span").click() 
        
        if driver.find_element(By.XPATH, "/html/body/form/table/tbody/tr[2]/td[2]/span/table/tbody/tr[2]/td[7]/span").text =="غيرفعال":
            log(row["rahgiri"], 'قبلا غیر فعال شده است')
            continue
        
        time.sleep(0.2)
        try:
            if (driver.find_element(By.XPATH, "//span[contains(text(),'نمایش شناسنامه')]")):
                driver.find_element(By.XPATH, "//span[contains(text(),'نمایش شناسنامه')]").click()
        except:
            log(row['rahgiri'], 'پرونده ای موجود نیست')
            continue
        
        try:
            WebDriverWait(driver, 4).until(EC.presence_of_element_located((By.XPATH, '/html/body/form/table/tbody/tr[2]/td[2]/span[3]/table[2]/tbody/tr/td/div/div[1]/table[1]/tbody/tr[1]/td[2]/table[1]/tbody/tr[1]/td[4]/span')))
            if (driver.find_element(By.XPATH, '/html/body/form/table/tbody/tr[2]/td[2]/span[3]/table[2]/tbody/tr/td/div/div[1]/table[1]/tbody/tr[1]/td[2]/table[1]/tbody/tr[1]/td[4]/span')):
                if (driver.find_element(By.XPATH, '/html/body/form/table/tbody/tr[2]/td[2]/span[3]/table[2]/tbody/tr/td/div/div[1]/table[1]/tbody/tr[1]/td[2]/table[1]/tbody/tr[1]/td[4]/span').text == 'پرونده مهم'):
                    log(row['rahgiri'], 'success')
                    
        except:
        
            driver.find_element(By.XPATH, "/html/body/form/table/tbody/tr[2]/td[2]/span[3]/table[1]/tbody/tr/td[7]/a/div").click()
            driver.find_element(By.XPATH, "/html/body/form/table/tbody/tr[2]/td[2]/span[3]/table[2]/tbody/tr/td/div/div[7]/table[1]/tbody/tr[3]/td[3]/a/div").click()
            time.sleep(0.2)
            # driver.find_element(By.XPATH, '/html/body/form/table/tbody/tr[2]/td[2]/span[3]/table[2]/tbody/tr/td/div/div[7]/table[1]/tbody/tr[3]/td[3]/a/div').send_keys(Keys.RETURN)
            # time.sleep(2)
            alert = driver.switch_to.alert
            alert.accept()
            # WebDriverWait(driver, 8).until(EC.alert_is_present())
            # driver.switch_to.alert.accept()
            # driver.find_element(By.XPATH, '//*[@id="CPC_TextboxDisableTaxpayerDateFa"]').send_keys("1401/07/17")
            # driver.find_element(By.XPATH, "/html/body/form/table/tbody/tr[2]/td[2]/div/table/tbody/tr[2]/td[2]/div/a/span").click()
            # driver.find_element(By.XPATH, '/html/body/form/table/tbody/tr[2]/td[2]/div/table/tbody/tr[2]/td[2]/div/div/div/input').send_keys("سایر موارد")
            # driver.find_element(By.XPATH, '/html/body/form/table/tbody/tr[2]/td[2]/div/table/tbody/tr[2]/td[2]/div/div/div/input').send_keys(Keys.RETURN)
            # driver.find_element(By.XPATH, '/html/body/form/table/tbody/tr[2]/td[2]/div/table/tbody/tr[3]/td[2]/input').send_keys("با توجه به نامه شماره 22213 و با درخواست اداره مربوطه غیرفعال گردید")
            # driver.find_element(By.XPATH, '//*[@id="CPC_CheckBoxDisableTaxpayer"]').click()
            # driver.find_element(By.XPATH, '//*[@id="CPC_ButtonDisableTaxpayer"]').click()
            time.sleep(0.2)
            
            try:
                WebDriverWait(driver, 8).until(EC.presence_of_element_located((By.XPATH, '/html/body/form/table/tbody/tr[2]/td[2]/span[3]/table[2]/tbody/tr/td/div/div[1]/table[1]/tbody/tr[1]/td[2]/table[1]/tbody/tr[1]/td[4]/span')))
                if (driver.find_element(By.XPATH, '/html/body/form/table/tbody/tr[2]/td[2]/span[3]/table[2]/tbody/tr/td/div/div[1]/table[1]/tbody/tr[1]/td[2]/table[1]/tbody/tr[1]/td[4]/span').text == 'پرونده مهم'):
                    log(row['rahgiri'], 'success')
            except:
                log(row['rahgiri'], 'failure')
    except Exception as e:
        log(row["rahgiri"], 'failure')
        print(e)
        time.sleep(0.2)
        continue
        

    time.sleep(0.2)
    
driver.close()    

    

