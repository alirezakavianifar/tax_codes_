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
import shutil
from helpers import maybe_make_dir, retry
import argparse
from watchdog_186 import watch_over, is_downloaded
import threading



first_index, until_date = 0, '14010730'
DOWNLOADED_FILES = first_index
BASE_DATE_1 = '13900101'
BASE_DATE_97_1 = '13970101'
BASE_DATE_97_2 = '13970102'
BASE_DATE_98_1 = '13980101'
BASE_DATE_98_2 = '13980102'
BASE_DATE_99_1 = '13990101'
BASE_DATE_99_2 = '13990102'
BASE_DATE_00_1 = '14000101'
BASE_DATE_00_2 = '14000102'
BASE_DATE_01_1 = '14010101'

@retry
def input_info():
    
    global input_error_counter

    parser = argparse.ArgumentParser()
    parser.add_argument('first_index', help=' insert first index') 
    parser.add_argument('second_index', help=' inser second index')   
    parser.add_argument('until_date', help=' inser until date')             
    args = parser.parse_args()
           
    first_index = int(args.first_index)
    second_index = int(args.second_index)
    until_date = str(args.until_date)
    
    return until_date


    
def save_process(driver):
    global DOWNLOADED_FILES
    
    save = driver.find_element(By.ID, 'StiWebViewer1_SaveLabel')
        
    if (save.is_displayed()):
        actions = ActionChains(driver)
        actions.move_to_element(save).perform()
        hidden_submenu = driver.find_element(By.XPATH, '/html/body/form/div[3]/span/div[1]/div/table/tbody/tr/td[2]/table/tbody/tr/td[3]/div/div[2]/div/table/tbody/tr/td/table[12]/tbody/tr/td[5]')
        actions.move_to_element(hidden_submenu).perform()
        hidden_submenu.click()
            
        element = WebDriverWait(driver, 60).until(EC.presence_of_element_located((By.ID, "StiWebViewer1_StiWebViewer1ExportDataOnly")))
        element.click()
            
        element = WebDriverWait(driver, 60).until(EC.presence_of_element_located((By.ID, "StiWebViewer1_StiWebViewer1ExportObjectFormatting")))
        element.click()
        time.sleep(3)
        element = WebDriverWait(driver, 60).until(EC.presence_of_element_located((By.XPATH, "/html/body/form/div[3]/span/table[1]/tbody/tr/td/table/tbody/tr[2]/td[2]/table/tbody/tr[6]/td/table/tbody/tr/td[1]/table/tbody/tr/td[2]")))
        element.click()
        time.sleep(2)
        

@retry    
def scrape_186():
         
    
    global DOWNLOADED_FILES
    urls=[
        'http://govahi186.tax.gov.ir/StimolSoftReport/StatusOfReqsInEdarat/ShowSelectReqCount.aspx?F={0}&T={1}&S=0&Edare=undefined&reqtl=1&rwndrnd=0.391666473501828'.format(BASE_DATE_1,until_date),
        
        'http://govahi186.tax.gov.ir/StimolSoftReport/StatusOfReqsInEdarat/ShowSelectReqCount.aspx?F={0}&T={1}&S=1&Edare=undefined&reqtl=1&rwndrnd=0.10286883974335082'.format(BASE_DATE_1, BASE_DATE_97_1),
        'http://govahi186.tax.gov.ir/StimolSoftReport/StatusOfReqsInEdarat/ShowSelectReqCount.aspx?F={0}&T={1}&S=1&Edare=undefined&reqtl=1&rwndrnd=0.10286883974335082'.format(BASE_DATE_97_2, BASE_DATE_98_1),
        'http://govahi186.tax.gov.ir/StimolSoftReport/StatusOfReqsInEdarat/ShowSelectReqCount.aspx?F={0}&T={1}&S=1&Edare=undefined&reqtl=1&rwndrnd=0.10286883974335082'.format(BASE_DATE_98_2, BASE_DATE_99_1),
        'http://govahi186.tax.gov.ir/StimolSoftReport/StatusOfReqsInEdarat/ShowSelectReqCount.aspx?F={0}&T={1}&S=1&Edare=undefined&reqtl=1&rwndrnd=0.10286883974335082'.format(BASE_DATE_99_2, BASE_DATE_00_1),
        'http://govahi186.tax.gov.ir/StimolSoftReport/StatusOfReqsInEdarat/ShowSelectReqCount.aspx?F={0}&T={1}&S=1&Edare=undefined&reqtl=1&rwndrnd=0.10286883974335082'.format(BASE_DATE_00_2, BASE_DATE_01_1),
        'http://govahi186.tax.gov.ir/StimolSoftReport/StatusOfReqsInEdarat/ShowSelectReqCount.aspx?F={0}&T={1}&S=1&Edare=undefined&reqtl=1&rwndrnd=0.10286883974335082'.format(BASE_DATE_01_1,until_date),
        
        'http://govahi186.tax.gov.ir/StimolSoftReport/StatusOfReqsInEdarat/ShowSelectReqCount.aspx?F={0}&T={1}&S=2&Edare=undefined&reqtl=1&rwndrnd=0.19619883107398017'.format(BASE_DATE_1,until_date),
        
        'http://govahi186.tax.gov.ir/StimolSoftReport/StatusOfReqsInEdarat/ShowSelectReqCount.aspx?F={0}&T={1}&S=3&Edare=undefined&reqtl=1&rwndrnd=0.9409760878581537'.format(BASE_DATE_1,until_date),
        
        'http://govahi186.tax.gov.ir/StimolSoftReport/StatusOfReqsInEdarat/ShowSelectReqCount.aspx?F={0}&T={1}&S=4&Edare=undefined&reqtl=1&rwndrnd=0.9490208553378973'.format(BASE_DATE_1, BASE_DATE_97_1),
        'http://govahi186.tax.gov.ir/StimolSoftReport/StatusOfReqsInEdarat/ShowSelectReqCount.aspx?F={0}&T={1}&S=4&Edare=undefined&reqtl=1&rwndrnd=0.9490208553378973'.format(BASE_DATE_97_2, BASE_DATE_98_1),
        'http://govahi186.tax.gov.ir/StimolSoftReport/StatusOfReqsInEdarat/ShowSelectReqCount.aspx?F={0}&T={1}&S=4&Edare=undefined&reqtl=1&rwndrnd=0.9490208553378973'.format(BASE_DATE_98_2, BASE_DATE_99_1),
        'http://govahi186.tax.gov.ir/StimolSoftReport/StatusOfReqsInEdarat/ShowSelectReqCount.aspx?F={0}&T={1}&S=4&Edare=undefined&reqtl=1&rwndrnd=0.9490208553378973'.format(BASE_DATE_99_2, BASE_DATE_00_1),
        'http://govahi186.tax.gov.ir/StimolSoftReport/StatusOfReqsInEdarat/ShowSelectReqCount.aspx?F={0}&T={1}&S=4&Edare=undefined&reqtl=1&rwndrnd=0.9490208553378973'.format(BASE_DATE_00_2, BASE_DATE_01_1),
        'http://govahi186.tax.gov.ir/StimolSoftReport/StatusOfReqsInEdarat/ShowSelectReqCount.aspx?F={0}&T={1}&S=4&Edare=undefined&reqtl=1&rwndrnd=0.9490208553378973'.format(BASE_DATE_01_1,until_date),
        
        'http://govahi186.tax.gov.ir/StimolSoftReport/StatusOfReqsInEdarat/ShowSelectReqCount.aspx?F={0}&T={1}&S=5&Edare=undefined&reqtl=1&rwndrnd=0.12348184643733573'.format(BASE_DATE_1,until_date),
        
        'http://govahi186.tax.gov.ir/StimolSoftReport/StatusOfReqsInEdarat/ShowSelectReqCount.aspx?F={0}&T={1}&S=6&Edare=undefined&reqtl=1&rwndrnd=0.9987754619692941'.format(BASE_DATE_1,until_date),
        
        'http://govahi186.tax.gov.ir/StimolSoftReport/StatusOfReqsInEdarat/ShowSelectReqCount.aspx?F={0}&T={1}&S=8&Edare=undefined&reqtl=1&rwndrnd=0.2656927736407235'.format(BASE_DATE_1,until_date),
        #############################################################################################################################################################################################################################
        'http://govahi186.tax.gov.ir/StimolSoftReport/StatusOfReqsInEdarat/ShowSelectReqCount.aspx?F={0}&T={1}&S=0&Edare=undefined&reqtl=3&rwndrnd=0.2053884823939629'.format(BASE_DATE_1,until_date),
        
        'http://govahi186.tax.gov.ir/StimolSoftReport/StatusOfReqsInEdarat/ShowSelectReqCount.aspx?F={0}&T={1}&S=1&Edare=undefined&reqtl=1&rwndrnd=0.5633793857911045'.format(BASE_DATE_1, BASE_DATE_97_1),
        'http://govahi186.tax.gov.ir/StimolSoftReport/StatusOfReqsInEdarat/ShowSelectReqCount.aspx?F={0}&T={1}&S=1&Edare=undefined&reqtl=1&rwndrnd=0.5633793857911045'.format(BASE_DATE_97_2, BASE_DATE_98_1),
        'http://govahi186.tax.gov.ir/StimolSoftReport/StatusOfReqsInEdarat/ShowSelectReqCount.aspx?F={0}&T={1}&S=1&Edare=undefined&reqtl=1&rwndrnd=0.5633793857911045'.format(BASE_DATE_98_2, BASE_DATE_99_1),
        'http://govahi186.tax.gov.ir/StimolSoftReport/StatusOfReqsInEdarat/ShowSelectReqCount.aspx?F={0}&T={1}&S=1&Edare=undefined&reqtl=1&rwndrnd=0.5633793857911045'.format(BASE_DATE_99_2, BASE_DATE_00_1),
        'http://govahi186.tax.gov.ir/StimolSoftReport/StatusOfReqsInEdarat/ShowSelectReqCount.aspx?F={0}&T={1}&S=1&Edare=undefined&reqtl=3&rwndrnd=0.5633793857911045'.format(BASE_DATE_00_2, BASE_DATE_01_1),
        'http://govahi186.tax.gov.ir/StimolSoftReport/StatusOfReqsInEdarat/ShowSelectReqCount.aspx?F={0}&T={1}&S=1&Edare=undefined&reqtl=3&rwndrnd=0.5633793857911045'.format(BASE_DATE_01_1,until_date),
        
        'http://govahi186.tax.gov.ir/StimolSoftReport/StatusOfReqsInEdarat/ShowSelectReqCount.aspx?F={0}&T={1}&S=7&Edare=undefined&reqtl=3&rwndrnd=0.7202288185326258'.format(BASE_DATE_1,until_date),
        #############################################################################################################################################################################################################################
        #############################################################################################################################################################################################################################
        'http://govahi186.tax.gov.ir/StimolSoftReport/MoreThan15DaysDetails/ShowMoreThan15DaysDetails.aspx?F={0}&T={1}&S=1&Edare=undefined&reqtl=1&rwndrnd=0.6731190588613609'.format(BASE_DATE_1,until_date),
        
        'http://govahi186.tax.gov.ir/StimolSoftReport/MoreThan15DaysDetails/ShowMoreThan15DaysDetails.aspx?F={0}&T={1}&S=2&Edare=undefined&reqtl=1&rwndrnd=0.3354817228826432'.format(BASE_DATE_1, BASE_DATE_97_1),
        'http://govahi186.tax.gov.ir/StimolSoftReport/MoreThan15DaysDetails/ShowMoreThan15DaysDetails.aspx?F={0}&T={1}&S=2&Edare=undefined&reqtl=1&rwndrnd=0.3354817228826432'.format(BASE_DATE_97_2, BASE_DATE_98_1),
        'http://govahi186.tax.gov.ir/StimolSoftReport/MoreThan15DaysDetails/ShowMoreThan15DaysDetails.aspx?F={0}&T={1}&S=2&Edare=undefined&reqtl=1&rwndrnd=0.3354817228826432'.format(BASE_DATE_98_2, BASE_DATE_99_1),
        'http://govahi186.tax.gov.ir/StimolSoftReport/MoreThan15DaysDetails/ShowMoreThan15DaysDetails.aspx?F={0}&T={1}&S=2&Edare=undefined&reqtl=1&rwndrnd=0.3354817228826432'.format(BASE_DATE_99_2, BASE_DATE_00_1),
        'http://govahi186.tax.gov.ir/StimolSoftReport/MoreThan15DaysDetails/ShowMoreThan15DaysDetails.aspx?F={0}&T={1}&S=2&Edare=undefined&reqtl=1&rwndrnd=0.3354817228826432'.format(BASE_DATE_00_2, BASE_DATE_01_1),
        'http://govahi186.tax.gov.ir/StimolSoftReport/MoreThan15DaysDetails/ShowMoreThan15DaysDetails.aspx?F={0}&T={1}&S=2&Edare=undefined&reqtl=1&rwndrnd=0.3354817228826432'.format(BASE_DATE_01_1,until_date),
        
        'http://govahi186.tax.gov.ir/StimolSoftReport/MoreThan15DaysDetails/ShowMoreThan15DaysDetails.aspx?F={0}&T={1}&S=3&Edare=undefined&reqtl=1&rwndrnd=0.3154066478901216'.format(BASE_DATE_1,until_date),
        
        'http://govahi186.tax.gov.ir/StimolSoftReport/MoreThan15DaysDetails/ShowMoreThan15DaysDetails.aspx?F={0}&T={1}&S=7&Edare=undefined&reqtl=3&rwndrnd=0.6972614531535117'.format(BASE_DATE_1,until_date),
        
        'http://govahi186.tax.gov.ir/StimolSoftReport/MoreThan15DaysDetails/ShowMoreThan15DaysDetails.aspx?F={0}&T={1}&S=8&Edare=undefined&reqtl=3&rwndrnd=0.8946791182948994'.format(BASE_DATE_1, BASE_DATE_97_1),
        'http://govahi186.tax.gov.ir/StimolSoftReport/MoreThan15DaysDetails/ShowMoreThan15DaysDetails.aspx?F={0}&T={1}&S=8&Edare=undefined&reqtl=3&rwndrnd=0.8946791182948994'.format(BASE_DATE_97_2, BASE_DATE_98_1),
        'http://govahi186.tax.gov.ir/StimolSoftReport/MoreThan15DaysDetails/ShowMoreThan15DaysDetails.aspx?F={0}&T={1}&S=8&Edare=undefined&reqtl=3&rwndrnd=0.8946791182948994'.format(BASE_DATE_98_2, BASE_DATE_99_1),
        'http://govahi186.tax.gov.ir/StimolSoftReport/MoreThan15DaysDetails/ShowMoreThan15DaysDetails.aspx?F={0}&T={1}&S=8&Edare=undefined&reqtl=3&rwndrnd=0.8946791182948994'.format(BASE_DATE_99_2, BASE_DATE_00_1),
        'http://govahi186.tax.gov.ir/StimolSoftReport/MoreThan15DaysDetails/ShowMoreThan15DaysDetails.aspx?F={0}&T={1}&S=8&Edare=undefined&reqtl=3&rwndrnd=0.8946791182948994'.format(BASE_DATE_00_2, BASE_DATE_01_1),
        'http://govahi186.tax.gov.ir/StimolSoftReport/MoreThan15DaysDetails/ShowMoreThan15DaysDetails.aspx?F={0}&T={1}&S=8&Edare=undefined&reqtl=3&rwndrnd=0.8946791182948994'.format(BASE_DATE_01_1,until_date),
        
        'http://govahi186.tax.gov.ir/StimolSoftReport/MoreThan15DaysDetails/ShowMoreThan15DaysDetails.aspx?F={0}&T={1}&S=9&Edare=undefined&reqtl=3&rwndrnd=0.33102715206520283'.format(BASE_DATE_1,until_date)]
        


    
    file_lists=[r'J:\Temp\186',r'J:\Temp\186\bank',r'J:\Temp\186\asnaf',r'J:\Temp\186\asnaf\reply',r'J:\Temp\186\bank\reply']
    maybe_make_dir([str(first_index)])
    maybe_make_dir(file_lists)
    
    # for f in file_lists:
        
    #     filelist = glob.glob(os.path.join(f, "*.xlsx"))

    #     for f in filelist:
    #         os.remove(f)
    # پاک کردن فایل های اکسل پوشه t    # تنظیمات وب درایور فایرفاکس
    fp = webdriver.FirefoxProfile()
    fp.set_preference('browser.download.folderList', 2)
    fp.set_preference('browser.download.manager.showWhenStarting', False)
    fp.set_preference('browser.download.dir', r'J:\Temp\186')
    fp.set_preference('browser.helperApps.neverAsk.openFile',
                  'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet,application/octet-stream')
    fp.set_preference('browser.helperApps.neverAsk.saveToDisk',
                      'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet,application/octet-stream;application/excel')
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
    driver.get("http://govahi186.tax.gov.ir/Login.aspx")
    driver.implicitly_wait(5)
    
    element = WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.ID, "MainContent_txtUserName")))
    element.send_keys("1757400389")
    
    element = WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.ID, "MainContent_txtPassword")))
    element.send_keys("14579Ali.")
    
    # time.sleep(12)
    
    element = driver.find_element(By.ID, "lblUser")
    
    while (element.is_displayed() == False):
        print("waiting for the login to be completed")
    # element = WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.XPATH, "/html/body/form/div[3]/div[2]/div/div[1]/div[2]/div/input")))
    # element.click()
    element = driver.find_element(By.ID, "lblUser")
    
    if (element.is_displayed()):
        driver.get("http://govahi186.tax.gov.ir/StimolSoftReport/StatusOfReqsInEdarat/ReportSelectReqCount.aspx?type=1")
        
        if DOWNLOADED_FILES > first_index:
            index = DOWNLOADED_FILES + 1
        else:
            index = first_index
        for i,url in enumerate(urls[26:]):
            try:
                driver.get(url)
                time.sleep(1)
                t1 = threading.Thread(target=save_process, args=(driver,))
                t2 = threading.Thread(target=watch_over)
                t1.start()
                t2.start()
                t1.join()
                t2.join()
                # save_process(driver)
                DOWNLOADED_FILES += 1
               
            except:
                continue
                
    time.sleep(120)    
    
    
        
    # src = os.path.join(r'D:\Temp\186', "گزارشوضعیتدرخواستها.xlsx")
    # dst = os.path.join(r'D:\Temp\186', "1.xlsx")
    # os.rename(src, dst)
    # for i in range(1,8):
    #      src = os.path.join(r'D:\Temp\186', "گزارشوضعیتدرخواستها({0}).xlsx".format(i))
    #      dst = os.path.join(r'D:\Temp\186', "{0}.xlsx".format(i+1))
    #      os.rename(src, dst)
         
    # for i in range(1,9):
    #     src = os.path.join(r'D:\Temp\186', "{0}.xlsx".format(i))
    #     dst = os.path.join(r'J:\Temp\186\bank', "{0}.xlsx".format(i))
    #     shutil.move(src, dst)
        
        
    # for i in range(8,11):
    #      src = os.path.join(r'D:\Temp\186', "گزارشوضعیتدرخواستها({0}).xlsx".format(i))
    #      dst = os.path.join(r'D:\Temp\186', "{0}.xlsx".format(i+1))
    #      os.rename(src, dst)
         
    # for i in range(1,4):
    #     src = os.path.join(r'D:\Temp\186', "{0}.xlsx".format(i+8))
    #     dst = os.path.join(r'J:\Temp\186\asnaf', "{0}.xlsx".format(i))
    #     shutil.move(src, dst)



        
    # src = os.path.join(r'D:\Temp\186', "Report.xlsx")
    # dst = os.path.join(r'D:\Temp\186', "1.xlsx")
    # os.rename(src, dst)
    # for i in range(1,3):
    #      src = os.path.join(r'D:\Temp\186', "Report({0}).xlsx".format(i))
    #      dst = os.path.join(r'D:\Temp\186', "{0}.xlsx".format(i+1))
    #      os.rename(src, dst)
         
    # for i in range(1,4):
    #     src = os.path.join(r'D:\Temp\186', "{0}.xlsx".format(i))
    #     dst = os.path.join(r'J:\Temp\186\bank\reply', "{0}.xlsx".format(i))
    #     shutil.move(src, dst)
        
        
    # for i in range(3,6):
    #      src = os.path.join(r'D:\Temp\186', "Report({0}).xlsx".format(i))
    #      dst = os.path.join(r'D:\Temp\186', "{0}.xlsx".format(i-2))
    #      os.rename(src, dst)
         
    # for i in range(1,4):
    #     src = os.path.join(r'D:\Temp\186', "{0}.xlsx".format(i))
    #     dst = os.path.join(r'J:\Temp\186\asnaf\reply', "{0}.xlsx".format(i))
    #     shutil.move(src, dst)            
        
    # os.replace(r"path/to/current/file.foo", r"path/to/new/destination/for/file.foo")
    driver.close()
                         


scrape_186()      
        