# Refactored helpers.py - Re-exporting from new modules for backward compatibility

import os
try:
    import tensorflow as tf
except ImportError:
    tf = None
import pandas as pd
try:
    import jdatetime
except ImportError:
    jdatetime = None
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# Constants Re-exports (originally imported from constants.py)
from codeeghtesadi.constants import (
    geck_location, set_gecko_prefs, get_remote_sql_con, get_sql_con, get_str_help,
    get_comm_reports, get_heiat, get_lst_reports, get_all_years, get_common_years, 
    get_str_years, get_years, get_common_reports, get_comm_years, get_heiat_reports, 
    get_server_namesV2
)

# Common
from codeeghtesadi.utils.common import (
    leading_zero, add_one_day, maybe_make_dir, make_dir_if_not_exists,
    get_update_date, split_list, get_filename, is_int,
    georgian_to_persian, is_date, rename_duplicate_columns,
    return_start_end, add_days_to_persian_date, calculate_days_difference,
    extract_nums, input_info
)

# Decorators
from codeeghtesadi.utils.decorators import (
    universal_retry, not_none_validator, retry_V1, wrap_it_with_params,
    wrap_it_with_paramsv1, retry, retryV1, measure_time, wrap_func,
    log_the_func, time_it, check_if_up_to_date, check_update, log_it
)

# Database
from codeeghtesadi.utils.database import (
    n_retries, connect_to_sql, drop_into_db, insert_into_tbl,
    get_edare_shahr, find_edares, process_mostaghelat, final_most,
    get_sqltable_colnames
)

# Excel Ops
from codeeghtesadi.utils.excel_ops import (
    read_multiple_excel_sheets, remove_excel_files, merge_multiple_excel_sheets,
    rename_files, merge_multiple_html_files, read_multiple_excel_files,
    merge_multiple_excel_files, save_row, read_excel_withtime,
    save_as_csv_file, cleanupdf, save_excel, read_excel_and_drop_into_db,
    open_and_save_excel_files, is_updated_to_download, is_updated_to_save,
    check_if_col_exists, create_df, highlight_excel_cells,
    df_to_excelsheet, df_to_excelworkbooks
)

# File Ops
from codeeghtesadi.utils.file_ops import (
    list_files, unzip_files, zipdir, get_file_size, move_files, count_num_files,
    generate_readme
)

# Process Ops
from codeeghtesadi.utils.process_ops import (
    get_child_processes, cleanup_processes
)

# Selenium Ops
from codeeghtesadi.utils.selenium_ops import (
    find_obj_login, cleanup_driver, check_driver_health, record_keys,
    get_lst_images, decode_batch_predictions, validate_results, predict_captcha,
    handle_pred_captcha, handle_captcha_data_gathering, fill_credentials,
    wait_for_4_char_captcha, submit_login_form, login_codeghtesadi,
    login_mostaghelat, login_hoghogh, check_if_login_iris_user_pass_inserted,
    insert_login_iris_user_pass, login_iris, login_list_hoghogh,
    login_nezam_mohandesi, login_sanim, login_186, login_tgju,
    login_arzeshafzoodeh, login_soratmoamelat, login_chargoon,
    login_vosolejra, init_driver, Login, get_files_sizes, wait_for_download_to_finish,
    FIREFOX_PROFILE_PATH
)

# Globals
df = pd.DataFrame({'A': [1, 2]})
year = 0
report_type = 0
log_folder_name = 'C:\\ezhar-temp'
log_excel_name = 'excel.xlsx'
log_dir = os.path.join(log_folder_name, log_excel_name)
saved_folder = geck_location(set_save_dir=False)
