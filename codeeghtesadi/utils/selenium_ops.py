import os
import time
import glob
import keyboard
import numpy as np
try:
    import tensorflow as tf
except ImportError:
    tf = None

# from tensorflow import keras # Original commented out, using tf.keras
# from tensorflow.keras import layers # Original commented out
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.firefox.options import Options as FirefoxOptions
from selenium.webdriver.firefox.service import Service as FirefoxService
from selenium.webdriver.firefox.firefox_profile import FirefoxProfile
from selenium.webdriver.chrome.options import Options
from contextlib import contextmanager
from pathlib import Path

# Internal imports
from codeeghtesadi.constants import geck_location, set_gecko_prefs
from codeeghtesadi.utils.decorators import wrap_it_with_params, measure_time, log_the_func

@wrap_it_with_params(15, 10, True, False, True, False)
def find_obj_login(driver, info):

    # Use WebDriverWait to wait for up to 3 seconds for the specified element to be clickable.
    WebDriverWait(driver, 3).until(
        EC.element_to_be_clickable(
            (By.ID, 'OBJ')
        )
    )
    # Return the driver and info parameters.
    return driver, info


# Wrapper decorator that wraps another function with specific parameters.
# @wrap_a_wrapper
@wrap_it_with_params(1, 50, True, False, False, False)
def cleanup_driver(driver, info, close_driver=False):

    if isinstance(driver, tuple):
        driver = driver[0]
        driver.switch_to.default_content()
    else:
        driver.switch_to.default_content()

    WebDriverWait(driver, 5).until(
        EC.presence_of_element_located(
            (By.ID, 'logoutLink')))
    driver.find_element(
        By.ID,
        'logoutLink'
    ).click()

    if close_driver:
        driver.close()

    return driver, info


def check_driver_health(driver):

    if driver is None:
        raise Exception
    else:
        return driver

def record_keys(max_num=4, until='esc'):
    # Define a list of accepted letters and digits
    accepted_letters = ['a', 'A', 'b', 'B', 'c', 'C', 'd', 'D', 'e', 'E', 'f', 'F', 'g', 'G', 'h', 'H', 'i', 'I', 'j', 'J', 'k', 'K', 'l', 'L', 'm', 'M',
                        'n', 'N', 'o', 'O', 'p', 'P', 'q', 'Q', 'r', 'R', 's', 'S', 't', 'T', 'u', 'U', 'v', 'V',
                        'w', 'W', 'x', 'X', 'y', 'Y', 'z', 'Z', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9']

    # Initialize variables
    accepted = False
    recorded = keyboard.record(until=until)
    lst = []

    # Iterate over the recorded keys and filter out accepted ones
    for index, item in enumerate(recorded):
        if index % 2 == 0:
            if item.name in accepted_letters:
                lst.append(item.name)

    # Check if the number of recorded keys is equal to max_num
    if len(lst) != max_num:
        return accepted, 'null'

    # Concatenate the recorded keys into a string
    str_result = ''
    accepted = True
    for item in lst:
        str_result += item

    return accepted, str_result

# ==============================
# Captcha helpers
# ==============================

def get_lst_images(data_dir):
    # Get list of all the images
    images = sorted(list(map(str, list(data_dir.glob("*.png")))))
    labels = [img.split(os.path.sep)[-1].split(".png")[0] for img in images]
    characters = set(char for label in labels for char in label)
    characters = sorted(list(characters))

    print("Number of images found: ", len(images))
    print("Number of labels found: ", len(labels))
    print("Number of unique characters: ", len(characters))
    print("Characters present: ", characters)

    return images, labels, characters


def decode_batch_predictions(pred, max_length, num_to_char):
    input_len = np.ones(pred.shape[0]) * pred.shape[1]
    # Use greedy search. For complex tasks, you can use beam search
    results = tf.keras.backend.ctc_decode(pred, input_length=input_len, greedy=True)[0][0][
        :, :max_length
    ]
    # Iterate over the results and get back the text
    output_text = []
    for res in results:
        res = tf.strings.reduce_join(num_to_char(res)).numpy().decode("utf-8")
        output_text.append(res)
    return output_text

def validate_results(training=False, validation_dataset=None, prediction_model=None, max_length=4, num_to_char=4):
    for batch in validation_dataset.take(1):
        batch_images = batch["image"]
        batch_labels = batch["label"]

        preds = prediction_model.predict(batch_images)
        pred_texts = decode_batch_predictions(
            preds, max_length=max_length, num_to_char=num_to_char)

        return pred_texts


def predict_captcha(data_dir_prod, model):

    img_width = 200
    img_height = 50

    x_images, labels, characters = get_lst_images(Path(data_dir_prod))

    max_length = max([len(label) for label in labels])

    # Mapping characters to integers
    char_to_num = tf.keras.layers.StringLookup(
        vocabulary=list(characters), mask_token=None
    )

    # Mapping integers back to original characters
    num_to_char = tf.keras.layers.StringLookup(
        vocabulary=char_to_num.get_vocabulary(), mask_token=None, invert=True
    )

    def encode_single_sample(img_path, label):

        # 1. Read image
        img = tf.io.read_file(img_path)
        # 2. Decode and convert to grayscale
        img = tf.io.decode_png(img, channels=1)
        # 3. Convert to float32 in [0, 1] range
        img = tf.image.convert_image_dtype(img, tf.float32)
        # 4. Resize to the desired size
        img = tf.image.resize(img, [img_height, img_width])
        # 5. Transpose the image because we want the time
        # dimension to correspond to the width of the image.
        img = tf.transpose(img, perm=[1, 0, 2])
        # 6. Map the characters in label to numbers
        label = char_to_num(tf.strings.unicode_split(
            label, input_encoding="UTF-8"))
        # 7. Return a dict as our model is expecting two inputs
        return {"image": img, "label": label}

    # model_save_dir = r'E:\automating_reports_V2\automation\captcha_pred\saved_models\captcha_model'

    prediction_model = tf.keras.models.Model(
        model.get_layer(name="image").input, model.get_layer(
            name="dense2").output
    )

    validation_dataset = tf.data.Dataset.from_tensor_slices(
        (np.array(x_images), np.array(labels)))

    validation_dataset = (
        validation_dataset.map(
            encode_single_sample, num_parallel_calls=tf.data.AUTOTUNE
        )
        .batch(1)
        .prefetch(buffer_size=tf.data.AUTOTUNE)
    )
    # prediction_model.summary()
    pred_texts = validate_results(
        training=False, validation_dataset=validation_dataset, prediction_model=prediction_model,
        max_length=max_length, num_to_char=num_to_char)

    return pred_texts

def handle_pred_captcha(driver, file_path, model):
    pred_results = predict_captcha(file_path, model)

    captcha_input = driver.find_element(By.ID, 'CaptchaCode')
    captcha_input.clear()
    captcha_input.send_keys(pred_results[0])

    time.sleep(0.5)


def handle_captcha_data_gathering(driver, file_path):
    os.makedirs(file_path, exist_ok=True)

    temp_img_path = os.path.join(file_path, 'logo.png')

    with open(temp_img_path, 'wb') as f:
        f.write(
            driver.find_element(By.ID, 'img-captcha')
            .screenshot_as_png
        )

    accepted = False
    while not accepted:
        accepted, file_name = record_keys(until='esc')

    final_path = os.path.join(file_path, f"{file_name}.png")
    os.rename(temp_img_path, final_path)

    driver.refresh()


# ==============================
# Login helpers
# ==============================

def fill_credentials(driver, user_name, password):
    driver.find_element(By.ID, 'username').clear()
    driver.find_element(By.ID, 'username').send_keys(user_name)

    driver.find_element(By.ID, 'Password').clear()
    driver.find_element(By.ID, 'Password').send_keys(password)


def wait_for_4_char_captcha(driver, wait):
    def _has_4_chars(driver):
        value = driver.find_element(
            By.ID, "CaptchaCode").get_attribute("value")
        return len(value) == 4

    wait.until(_has_4_chars)


def submit_login_form(driver, wait):
    captcha_input = wait.until(
        EC.element_to_be_clickable((By.ID, "CaptchaCode"))
    )
    captcha_input.click()

    time.sleep(2)
    wait_for_4_char_captcha(driver, wait)

    submit_label = wait.until(
        EC.element_to_be_clickable(
            (By.CSS_SELECTOR, "label.button[for='submit']")
        )
    )
    submit_label.click()

# ==============================
# Environment configuration
# ==============================
IMG_BASE_DIR = os.getenv(
    "CAPTCHA_IMG_DIR",
    r'E:\automating_reports_V2\saved_dir\codeghtesadi\img_files'
)

MODEL_DIR = os.getenv(
    "CAPTCHA_MODEL_DIR",
    r'E:\automating_reports_V2\automation\captcha_pred\saved_models\captcha_model'
)

# ==============================
# Main login function
# ==============================

def login_codeghtesadi(
    driver,
    data_gathering=False,
    pred_captcha=False,
    info=None,
    user_name=os.getenv("LOGIN_CODEGHTESADI_USER"),
    password=os.getenv("LOGIN_CODEGHTESADI_PASS")
):
    if info is None:
        info = {}

    LOGIN_PAGE = "https://login.tax.gov.ir/Page/Index"
    REDIRECT_PAGE = "https://login.tax.gov.ir/"

    # 🔒 Guard: login only once per browser session
    if info.get("login_performed"):
        print("ℹ️ Login already handled in this session")
        return driver, info

    # ---------- helpers ----------
    def is_logged_in(wait=False):
        if wait:
            WebDriverWait(driver, 300).until(
                EC.url_matches(LOGIN_PAGE)
            )
        return driver.current_url.startswith(LOGIN_PAGE)

    def is_login_required():
        return driver.current_url.startswith(REDIRECT_PAGE)

    # ---------- open ----------
    driver.get(LOGIN_PAGE)
    time.sleep(2)

    # ✅ session still valid
    if is_logged_in():
        print("✅ Already logged in – using stored session")
        info["login_performed"] = True
        return driver, info

    # ❌ session expired
    if not is_login_required():
        print("⚠️ Unknown login state:", driver.current_url)
        return driver, info

    print("🔐 Session expired – logging in once")

    # ---------- captcha setup ----------
    file_path = os.path.join(
        IMG_BASE_DIR,
        'training' if data_gathering else 'production'
    )

    model = None
    if pred_captcha and not data_gathering:
        model = tf.keras.models.load_model(MODEL_DIR)

    # ---------- credentials ----------
    fill_credentials(driver, user_name, password)

    # ---------- captcha loop ----------
    wait = WebDriverWait(driver, 60)

    while True:
        try:
            if data_gathering:
                handle_captcha_data_gathering(driver, file_path)
                continue

            if pred_captcha:
                handle_pred_captcha(driver, file_path, model)

            submit_login_form(driver, wait)

            if is_logged_in(wait=True):
                print("✅ Login successful – session restored")
                info["login_performed"] = True
                break

        except Exception as e:
            print("⚠️ Login attempt failed, retrying:", e)
            time.sleep(1)

    return driver, info

@wrap_it_with_params(20, 60, True, False, False, False)
def login_mostaghelat(driver, info):
    driver.get("http://most.tax.gov.ir/")
    driver.implicitly_wait(20)
    txtUserName = driver.find_element(By.NAME,
                                      'Txt_username').send_keys(os.getenv("LOGIN_MOSTAGHELAT_USER"))
    txtPassword = driver.find_element(By.NAME,
                                      'Txt_Password').send_keys(os.getenv("LOGIN_MOSTAGHELAT_PASS"))
    time.sleep(0.5)
    driver.find_element(By.NAME, 'login_btn').click()

    return driver, info


def login_hoghogh(driver):
    driver.get("http://10.2.12.234:6399/")
    driver.implicitly_wait(20)

    driver.find_element(By.XPATH,
                        '/html/body/div[1]/div/div[1]/div/div/div/div/div/div/div/form/div[1]/div[1]/label/input[3]').click()
    txtUserName = driver.find_element(By.NAME,
                                      'UserName').send_keys(os.getenv("LOGIN_HOGHOGH_USER"))
    txtPassword = driver.find_element(By.NAME,
                                      'Password').send_keys(os.getenv("LOGIN_HOGHOGH_PASS"))
    time.sleep(0.5)
    driver.find_element(By.ID, 'BtnLogin').click()

    return driver


def check_if_login_iris_user_pass_inserted(driver, creds):
    if (creds['username'] == WebDriverWait(driver, 2).until(
        EC.element_to_be_clickable(
            (By.NAME,
                'userName'))).get_attribute('value') and str(creds['pass']) == WebDriverWait(driver, 2).until(
        EC.element_to_be_clickable(
            (By.NAME,
                'password'))).get_attribute('value')):
        return driver
    else:
        raise Exception


# @wrap_a_wrapper
@wrap_it_with_params(20, 60, True, False, False, False)
def insert_login_iris_user_pass(driver, creds, info):
    WebDriverWait(driver, 2).until(
        EC.element_to_be_clickable(
            (By.NAME,
                'userName'))).clear()
    WebDriverWait(driver, 2).until(
        EC.element_to_be_clickable(
            (By.NAME,
                'userName'))).send_keys(creds['username'])

    WebDriverWait(driver, 2).until(
        EC.element_to_be_clickable(
            (By.NAME,
                'password'))).clear()

    WebDriverWait(driver, 2).until(
        EC.element_to_be_clickable(
            (By.NAME,
                'password'))).send_keys(creds['pass'])
    driver = check_if_login_iris_user_pass_inserted(driver, creds)

    return driver, info


# @wrap_a_wrapper
@wrap_it_with_params(20, 60, True, False, False, False)
def login_iris(driver, creds=None, info={'success': True}):
    driver.get("https://its.tax.gov.ir/flexform2/logineris/form2login.jsp")

    time.sleep(4)

    driver, info = insert_login_iris_user_pass(
        driver=driver, creds=creds, info=info)

    time.sleep(2)

    WebDriverWait(driver, 3).until(
        EC.element_to_be_clickable(
            (By.ID,
                'ok_but'))).click()
    time.sleep(0.5)

    try:
        alert_obj = driver.switch_to.alert
        alert_obj.accept()
        error = True
        time.sleep(5)
    except:
        error = False

    if error:
        raise Exception

    driver, info = find_obj_login(driver=driver, info=info)

    return driver, info


# @wrap_a_wrapper
@wrap_it_with_params(20, 60, True, True, False, False)
def login_list_hoghogh(driver, info=None, creds=None):
    if info is None:
        info = {}
    if creds is None:
        creds = {'username': os.getenv("LOGIN_LIST_HOGHOGH_USER"),
                 'password': os.getenv("LOGIN_LIST_HOGHOGH_PASS"),
                 'username_modi': os.getenv("LOGIN_LIST_HOGHOGH_MODI")}
    driver.get("http://salary.tax.gov.ir/Account/LogOnArshad")

    txtUserName = driver.find_element(By.ID,
                                      'UserNameArshad').send_keys(creds['username'])
    txtPassword = driver.find_element(By.ID,
                                      'PasswordArshad').send_keys(creds['password'])
    txtPassword = driver.find_element(By.ID,
                                      'UserNameMoadi').send_keys(creds['username_modi'])

    driver.find_element(
        By.XPATH, '/html/body/div[2]/div[2]/div/fieldset[1]/form/div[7]/p/input').click()

    return driver, info


@wrap_it_with_params(20, 60, True, True, False, False)
def login_nezam_mohandesi(driver, info=None, creds=None):
    if info is None:
        info = {}
    if creds is None:
        creds = {'username': os.getenv("LOGIN_LIST_HOGHOGH_USER"),
                 'password': os.getenv("LOGIN_LIST_HOGHOGH_PASS"),
                 'username_modi': os.getenv("LOGIN_LIST_HOGHOGH_MODI")}
    driver.get("https://reports.khzceo.ir/m5_done.aspx")

    return driver, info


@wrap_it_with_params(15, 60, True, False, False, False)
def login_sanim(driver, info):
    driver.get("https://mgmt.tax.gov.ir/ords/f?p=100:101:16540338045165:::::")

    txtUserName = driver.find_element(By.ID,
                                      'P101_USERNAME').send_keys(os.getenv("LOGIN_SANIM_USER"))
    txtPassword = driver.find_element(By.ID,
                                      'P101_PASSWORD').send_keys(os.getenv("LOGIN_SANIM_PASS"))

    try:
        while (driver.find_element(By.XPATH,
                                   '/html/body/form/div/main/div/div/div/div/div[2]/div[4]/\
                                          div[2]/div/div/center/img[2]').is_displayed()):
            time.sleep(1)

            driver.find_element(By.ID, 'B1700889564218640').click()
    except:
        info['cur_instance'] = driver.current_url.split(':')[-6]

    return driver, info

def login_186(driver):
    driver.get("http://govahi186.tax.gov.ir/Login.aspx")
    driver.implicitly_wait(5)

    element = WebDriverWait(driver, 20).until(
        EC.presence_of_element_located((By.ID, "MainContent_txtUserName")))
    element.send_keys(os.getenv("LOGIN_186_USER"))

    element = WebDriverWait(driver, 20).until(
        EC.presence_of_element_located((By.ID, "MainContent_txtPassword")))
    element.send_keys(os.getenv("LOGIN_186_PASS"))

    element = driver.find_element(By.ID, "lblUser")

    try:
        while (element.is_displayed() == False):
            print("waiting for the login to be completed")

    except Exception as e:
        print(e)
    # element = driver.find_element(By.ID, "lblUser")

        return driver
    return driver


def login_tgju(driver):
    driver.get("https://www.tgju.org/")
    driver.implicitly_wait(20)

    return driver


@log_the_func('none')
def login_arzeshafzoodeh(driver, *args, **kwargs):
    success = False
    driver.get("http://10.2.16.131/frmManagerLogin2.aspx")
    driver.implicitly_wait(20)

    while (success == False):
        txtUserName = driver.find_element(By.NAME,
                                          'txtusername').send_keys(os.getenv("LOGIN_ARZESH_USER"))
        txtPassword = driver.find_element(By.NAME,
                                          'txtpassword').send_keys(os.getenv("LOGIN_ARZESH_PASS"))
        time.sleep(8)
        try:
            driver.find_element(By.NAME, 'btnlogin').click()
            try:
                WebDriverWait(driver, 3).until(
                    EC.presence_of_element_located(
                        (By.ID,
                         'lblmsg')))
                success = False

            except Exception as e:
                # print('Captcha is wrong')
                success = True
        except Exception:
            success = False

    return driver


def login_soratmoamelat(driver):
    driver.get("http://ittms.tax.gov.ir/")
    driver.implicitly_wait(20)
    txtUserName = driver.find_element(By.ID,
                                      'username').send_keys(os.getenv("LOGIN_SORAT_USER"))
    txtPassword = driver.find_element(By.ID,
                                      'Password').send_keys(os.getenv("LOGIN_SORAT_PASS"))
    time.sleep(10)
    driver.find_element(By.CLASS_NAME, 'button').click()
    time.sleep(1)

    return driver


def login_chargoon(driver=None, info=None,
                   user_name=os.getenv("LOGIN_CHARGOON_USER"), password=os.getenv("LOGIN_CHARGOON_PASS")):
    if info is None:
        info = {}
    driver.get(
        "http://chargoon-khoozestan:8090/UserLogin.Dap?logout=1&rnd=kqcuhyleisodrvyvhxkhjnlbdjxtjbdu")
    time.sleep(3)
    WebDriverWait(driver, 5).until(
        EC.presence_of_element_located((By.ID, "txtClientUsername"))).send_keys(user_name)
    WebDriverWait(driver, 5).until(
        EC.presence_of_element_located((By.ID, "txtClientPassword"))).send_keys(password)
    while (True):
        try:
            WebDriverWait(driver, 1).until(
                EC.presence_of_all_elements_located((By.ID, "txtClientPassword")))
            print("Waiting for user to login...")
        except:
            time.sleep(10)
            break

    return driver, info


def login_vosolejra(driver=None, info=None,
                    user_name=os.getenv("LOGIN_VOSOLEJRA_USER"), password=os.getenv("LOGIN_VOSOLEJRA_PASS")):
    if info is None:
        info = {}
    success = False
    driver.get("http://ve.tax.gov.ir/forms/frmVosool.aspx")
    driver.implicitly_wait(5)
    # Create wait object
    wait = WebDriverWait(driver, 10)

    # Input username
    username_field = wait.until(
        EC.presence_of_element_located((By.ID, "txtUser")))
    username_field.send_keys(os.getenv("LOGIN_VOSOLEJRA_USER"))

    # Input password
    password_field = wait.until(
        EC.presence_of_element_located((By.ID, "txtPass")))
    password_field.send_keys(os.getenv("LOGIN_VOSOLEJRA_PASS"))

    # Click login button
    login_button = wait.until(EC.element_to_be_clickable((By.ID, "btnLogin")))
    login_button.click()

    return driver, info


FIREFOX_PROFILE_PATH = (
    r"C:\Users\alkav\AppData\Roaming\Mozilla\Firefox\Profiles\q0uj5wit.taxgov"
)

@contextmanager
def init_driver(
    pathsave,
    driver_type='firefox',
    headless=False,
    prefs={'maximize': False, 'zoom': '1.0'},
    driver=None,
    info=None,
    use_proxy=False,
    disable_popups=False,
    *args,
    **kwargs
):

    if info is None:
        info = {}

    driver = None

    if driver_type == 'chrome':
        # ---------- Chrome (unchanged) ----------
        options = Options()
        options.add_argument("start-maximized")
        options.add_experimental_option(
            "prefs", {"download.default_directory": pathsave}
        )

        driver = webdriver.Chrome(
            executable_path=geck_location(driver_type='chrome'),
            options=options
        )

    else:
        # ---------- Firefox ----------
        if headless:
            # Assuming logging logger is not available here or use print
            raise RuntimeError(
                "Headless Firefox cannot be used with persistent profiles"
            )

        if not os.path.isdir(FIREFOX_PROFILE_PATH):
            raise RuntimeError("Firefox profile path does not exist")

        profile = FirefoxProfile(FIREFOX_PROFILE_PATH)
        options = FirefoxOptions()

        # ---------- Download preferences ----------
        profile.set_preference('browser.download.folderList', 2)
        profile.set_preference('browser.download.dir', pathsave)
        profile.set_preference(
            'browser.download.manager.showWhenStarting', False)

        profile.set_preference(
            'browser.helperApps.neverAsk.saveToDisk',
            'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet,application/octet-stream'
        )
        profile.set_preference('browser.helperApps.alwaysAsk.force', False)

        # ---------- UI / Zoom ----------
        profile.set_preference(
            "layout.css.devPixelsPerPx",
            prefs.get('zoom', '1.0')
        )

        # ---------- Proxy ----------
        if use_proxy:
            proxy_address = "10.52.3.128"
            profile.set_preference("network.proxy.type", 1)
            profile.set_preference("network.proxy.http", proxy_address)
            profile.set_preference("network.proxy.http_port", 8080)
            profile.set_preference("network.proxy.ssl", proxy_address)
            profile.set_preference("network.proxy.ssl_port", 8080)

        # ---------- Popups ----------
        if disable_popups:
            profile.set_preference("dom.popup_maximum", 0)
            profile.set_preference("dom.popup_allowed_events", "")

        driver = webdriver.Firefox(
            service=FirefoxService(geck_location()),
            options=options,
            firefox_profile=profile
        )

        if prefs.get('maximize'):
            driver.maximize_window()

    try:
        yield driver

    finally:
        if driver:
            driver.quit()

class Login:

    def __init__(self, pathsave):
        self.pathsave = pathsave
        fp = set_gecko_prefs(pathsave)
        self.driver = webdriver.Firefox(fp, executable_path=geck_location())
        self.driver.window_handles
        self.driver.switch_to.window(self.driver.window_handles[0])

    def __call__(self):
        return self.driver

    def close(self):
        self.driver.close()

def get_files_sizes(path, postfixes):
    try:
        # Check for the presence of temporary download files
        file_paths = [glob.glob(path + "/*" + '.%s.part' % postfix)
                      for postfix in postfixes]
        return os.path.getsize(file_paths[1][0])
    except Exception: # FileNotFoundError or IndexErr
        return 0

def wait_for_download_to_finish(path, postfixes=['xls', 'html'], sleep_time=6):

    # Sleep to allow for initial file creation
    time.sleep(sleep_time)

    # Variables to track download status
    file_exists = False
    i = 0
    file_size = 0
    while not file_exists:
        time.sleep(sleep_time)
        i += sleep_time

        # Print a message every 30 seconds to indicate ongoing download
        if i % (sleep_time * 2) == 0:

            print(
                f'Waiting for the download to finish, time elapsed is {i} seconds')

        time.sleep(6)
        # Check for the presence of temporary download files
        file_lists = [glob.glob(path + "/*" + '.%s.part' % postfix)
                      for postfix in postfixes]

        file_exists = not any(len(f) != 0 for f in file_lists)
        if not file_exists:
            current_file_size = get_files_sizes(path, postfixes)
            if current_file_size > file_size:
                file_size = current_file_size
                continue
            else:
                return 0
    return 1
