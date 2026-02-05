from pywinauto.keyboard import send_keys
from pywinauto.application import Application
from pywinauto import application
import pywinauto
import pyautogui
import time
import sys
import warnings
warnings.simplefilter('ignore', UserWarning)
sys.coinit_flags = 2


app = Application(backend="uia").start(u'E:\ITMS\ITCMS.exe', timeout=100)

try:
    app.window().wait('visible')
    time.sleep(1)
    main_app = app["سامانه اشخاص حقوقی"]
    main_app.wrapper_object().children()[0].children()[0].children()[
        11].children()[0].type_keys("14579")
    time.sleep(1)
    main_app['ورود'].click_input()
    time.sleep(6)
    menu_app = main_app['MainStrip1'].wrapper_object()
    menu_app.items()[1].items()[0].select()
    time.sleep(2)
    main_app["لیست برگه ها"].wrapper_object().double_click_input()
    time.sleep(1)
    main_app["عملکرد"].wrapper_object().double_click_input()
    time.sleep(1)
    main_app["تشخیص عملکرد"].wrapper_object().double_click_input()
    time.sleep(1)
    main_app["جستجو بر اساس واحد های زیرمجموعه"].wrapper_object().click_input()
    time.sleep(0.5)
    for item in ['1395', '1396', '1397', '1398', '1399', '1400']:

        main_app["داشبورد مطالبه و وصول(تشخیص عملکرد)"].wrapper_object().children()[
            1].children()[0].children()[0].children()[1].children()[0].double_click_input()
        main_app["داشبورد مطالبه و وصول(تشخیص عملکرد)"].wrapper_object().children()[
            1].children()[0].children()[0].children()[1].children()[0].type_keys(item)

        main_app["میز کار"].wrapper_object().children()[0].children()[0].children()[
            1].children()[9].type_keys('خوزستان')

        main_app["میز کار"].wrapper_object().children()[0].children()[0].children()[
            1].children()[14].click_input()
        time.sleep(69)
        main_app["میز کار"].wrapper_object().children()[0].children()[0].children()[
            1].children()[11].click_input()
        time.sleep(5)

        main_app["Save As"].wrapper_object().children()[0].children()[
            4].children()[0].type_keys(item)

        time.sleep(0.5)

        main_app["Save As"].wrapper_object().children()[2].click()
        time.sleep(15)

        save_excel = Application().connect(title="Microsoft Excel", timeout=20)
        save_excel.top_window().wrapper_object().children()[0].click_input()
        time.sleep(3)
        close_excel = Application().connect(title="%s.xls - Excel" % item, timeout=20)
        close_excel.kill()
        time.sleep(2)

    # app["سامانه اشخاص حقوقی"]['Main_Menu'].menu_select(
    #     'جستجوی مودی -> ثبت و شناسایی')
except Exception as e:
    print(e)


time.sleep(9)


app['WindowsForms10.Window.8.app.0.141b42a_r9_ad1'].close()

dlg = app.top_window()
dlg.print_control_identifiers()
time.sleep(8)
dlg.close()
app['WindowsForms10.Window.8.app.0.141b42a_r9_ad1'].close()

# app['سامانه اشخاص حقوقی']['1756914443WindowsForms10.Window.8.app.0.141b42a_r6_ad1'].type_keys(
#     '1756914443')
# app['سامانه اشخاص حقوقی']['Edit0'].type_keys(
#     '14579')
# time.sleep(1)
# app['سامانه اشخاص حقوقی']['ورود'].click()
time.sleep(3)

print(pyautogui.position())

pyautogui.moveTo(1036, 98)
pyautogui.click()
time.sleep(1)

print(pyautogui.position())
pyautogui.moveTo(708, 341)
pyautogui.click()
pyautogui.write('14579')

print(pyautogui.position())
pyautogui.moveTo(697, 377)
pyautogui.click()

time.sleep(10)

print(pyautogui.position())
pyautogui.moveTo(1226, 41)
pyautogui.click()

print(pyautogui.position())
pyautogui.moveTo(1204, 60)
pyautogui.click()

time.sleep(1)

print(pyautogui.position())
pyautogui.moveTo(1179, 247)
pyautogui.click()
time.sleep(1)

print(pyautogui.position())
pyautogui.moveTo(1160, 264)
pyautogui.click()
time.sleep(1)

print(pyautogui.position())
pyautogui.moveTo(1081, 231)
pyautogui.click()
time.sleep(1)

print(pyautogui.position())
pyautogui.moveTo(1071, 430)
pyautogui.click()
pyautogui.write('1395')
time.sleep(1)

pyautogui.moveTo(1178, 459)
pyautogui.click()
time.sleep(1)

pyautogui.moveTo(943, 490)
pyautogui.click()
time.sleep(1)

pyautogui.moveTo(1111, 502)
pyautogui.click()
time.sleep(1)

pyautogui.moveTo(1047, 605)
pyautogui.click()
time.sleep(40)

pyautogui.moveTo(1029, 661)
pyautogui.click()
time.sleep(2)

pyautogui.moveTo(166, 396)
pyautogui.click()
time.sleep(0.5)
pyautogui.write("تشخیص-95")

pyautogui.moveTo(515, 462)
pyautogui.click()
time.sleep(10)

pyautogui.moveTo(609, 426)
pyautogui.click()
time.sleep(10)

pyautogui.moveTo(1344, 18)
pyautogui.click()
time.sleep(1)

pyautogui.moveTo(1084, 244)
pyautogui.click()
time.sleep(0.5)

pyautogui.moveTo(1070, 599)
pyautogui.click()
time.sleep(0.5)

while True:
    print(pyautogui.position())
    print('f')


app['سامانه اشخاص حقوقی'].print_control_identifiers()
app['سامانه اشخاص حقوقی']['MenuStrip1'].print_control_identifiers()
app['سامانه اشخاص حقوقی']['MenuStrip1'].menu_select('خروج -> خروج')
app['سامانه اشخاص حقوقی'].print_control_identifiers()


app = application.Application()
app.start("Notepad.exe")
# app.UntitledNotepad.draw_outline()
app.UntitledNotepad.menu_select("Help -> About")
app.Replace.print_control_identifiers()
