import os
import time
import functools
import math
import logging
from functools import wraps
from datetime import datetime
from typing import Callable, Tuple, Any, Optional, Union, Iterable

import pandas as pd

# Define constants
LOG_FOLDER_NAME = r'C:\ezhar-temp'
LOG_EXCEL_NAME = 'excel.xlsx'
DEFAULT_LOG_DIR = os.path.join(LOG_FOLDER_NAME, LOG_EXCEL_NAME)
# Set up logging
logger = logging.getLogger(__name__)

def _format_duration(seconds: float) -> str:
    """Helper to format duration into a readable string."""
    if seconds >= 60:
        return f"{seconds / 60:.2f} minutes"
    return f"{seconds:.2f} seconds"

def universal_retry(
    retries: int = 3,
    delay: float = 1.0,
    backoff: float = 1.0,
    exceptions: Tuple[type, ...] = (Exception,),
    driver_based: bool = False,
    refresh_on_fail: bool = False,
    cleanup_func: Optional[Callable] = None,
    keep_alive: bool = False,
    validate_result: Optional[Callable[[Any], bool]] = None,
    retry_on_tuple: bool = False
) -> Callable:
    """
    A robust, universal retry decorator.
    
    Args:
        retries: Max attempts.
        delay: Initial sleep.
        backoff: Multiplier for delay.
        exceptions: Which errors to catch.
        driver_based: If True, tries to find/quit driver on total failure.
        refresh_on_fail: If True and driver exists, calls driver.refresh().
        cleanup_func: Custom cleanup callable for the driver.
        keep_alive: If True, returns (driver, info) on failure instead of raising.
        validate_result: Optional check on function result.
        retry_on_tuple: Special flag for time_it-style recursion where a tuple result triggers retry/index update.
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            current_delay = delay
            last_exception = None
            
            info = kwargs.get('info', {'success': True})
            if keep_alive:
                kwargs['info'] = info

            for attempt in range(1, retries + 1):
                try:
                    result = func(*args, **kwargs)
                    
                    if retry_on_tuple and isinstance(result, tuple) and len(result) > 0:
                        kwargs['index'] = result[0]
                        raise Exception(f"Recursion trigger: index updated to {result[0]}")

                    if validate_result and not validate_result(result):
                         raise Exception(f"Validation failed for {func.__name__}")
                    
                    return result

                except exceptions as e:
                    last_exception = e
                    logger.warning(f"Attempt {attempt}/{retries} failed for {func.__name__}: {e}")
                    
                    if attempt < retries:
                        # Optional Selenium refresh
                        if refresh_on_fail:
                            driver = kwargs.get('driver')
                            if not driver and args and hasattr(args[0], 'refresh'):
                                driver = args[0]
                            if driver:
                                try:
                                    driver.refresh()
                                except:
                                    pass
                                    
                        time.sleep(current_delay)
                        current_delay *= backoff
                    else:
                        logger.error(f"All {retries} attempts failed for {func.__name__}")

            # Failure cleanup
            driver = kwargs.get('driver')
            if not driver and args:
                for arg in args:
                    if hasattr(arg, 'quit'):
                        driver = arg
                        break
            
            if cleanup_func and driver:
                try:
                    cleanup_func(driver)
                except Exception as ce:
                    logger.error(f"Cleanup error: {ce}")

            if keep_alive:
                info['success'] = False
                return driver, info
            
            raise last_exception
        return wrapper
    return decorator

# Validation helpers
def not_none_validator(result: Any) -> bool:
    return result is not None

# Backward Compatibility & Convenience Wrappers
retry_V1 = universal_retry

def wrap_it_with_params(num_tries=3, time_out=20, driver_based=False, detrimental=True, clean_up=False, keep_alive=False, record_process_details=False):
    # 'time_out' historically wasn't used strictly here, kept for signature parity
    return universal_retry(retries=num_tries, delay=1.0, driver_based=driver_based, keep_alive=keep_alive, validate_result=not_none_validator)

def wrap_it_with_paramsv1(num_tries=3, time_out=10, driver_based=False, detrimental=True, clean_up=False, keep_alive=False):
    return universal_retry(retries=num_tries, delay=2.0, driver_based=driver_based, keep_alive=keep_alive, validate_result=not_none_validator)

def retry(func: Callable) -> Callable:
    return universal_retry(retries=5)(func)

def retryV1(func: Callable) -> Callable:
    return universal_retry(retries=5, driver_based=True)(func)

def measure_time(func: Callable) -> Callable:
    """Decorator to measure and log execution time."""
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        start_dt = datetime.now().strftime("%H:%M:%S")
        
        print(f"*" * 71)
        print(f"{func.__name__} started at {start_dt}\n")

        try:
            result = func(*args, **kwargs)
            return result
        finally:
            end_time = time.time()
            end_dt = datetime.now().strftime("%H:%M:%S")
            duration = end_time - start_time
            print(f"*" * 71)
            print(f"{func.__name__} finished at {end_dt}, time taken: {_format_duration(duration)}")
            
    return wrapper

def log_the_func(log_file_path: Optional[str] = None, **log_kwargs) -> Callable:
    """Decorator for logging function start and completion."""
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            field = kwargs.get('field')
            
            # Start message
            start_msg = f"Starting function {func.__name__}"
            if field and 'soratmoamelat' in log_kwargs:
                msg = f"{start_msg} for '{kwargs.get('selected_option_text', 'unknown')}'\n"
                field(msg)
            elif field:
                field(f"{start_msg}\n")

            if log_file_path:
                with open(log_file_path, 'w') as f:
                    f.write(start_msg + '\n')
                print(start_msg)

            # Execute
            result = func(*args, **kwargs)

            # Completion message
            end_msg = f"Function {func.__name__} completed successfully"
            if field:
                field(f"{end_msg}\n")
            
            if log_file_path:
                print(end_msg)
                with open(log_file_path, 'a') as f:
                    f.write(end_msg + '\n')

            return result
        return wrapper
    return decorator

def _log_to_db(func_name, duration, log, tbl_name, db_info, kwargs):
    """Internal helper to handle DB logging for time_it."""
    if not log or kwargs.get('only_schema', False):
        return
        
    try:
        from codeeghtesadi.utils.common import get_update_date
        from codeeghtesadi.utils.database import drop_into_db
        
        type_of = kwargs.get('type_of', 'nan')
        df_all = pd.DataFrame({
            'table_name': [tbl_name],
            'update_date': [get_update_date()],
            'type_of': [type_of]
        })

        drop_into_db(table_name=db_info['tbl_name'],
                     columns=df_all.columns.tolist(),
                     values=df_all.values.tolist(),
                     append_to_prev=db_info['append_to_prev'],
                     db_name=db_info['db_name'])
    except Exception as e:
        logger.error(f"DB logging failed in time_it: {e}")

def time_it(log: bool = False, tbl_name: str = 'default', num_runs: int = 12,
            db: dict = None) -> Callable:
    """
    Complex decorator that handles timing, retries, and database logging.
    Composed of universal_retry for robust error handling.
    """
    db_info = db or {'db_name': 'testdbV2', 'tbl_name': 'tblLog', 'append_to_prev': False}

    def decorator(func: Callable) -> Callable:
        # Wrap the function with universal_retry first
        retry_wrapped = universal_retry(retries=num_runs, retry_on_tuple=True)(func)

        @wraps(func)
        def wrapper(*args, **kwargs):
            start = time.time()
            
            # Dynamically allow overriding table name
            _tbl_name = kwargs.get('table_name', tbl_name)
            
            try:
                result = retry_wrapped(*args, **kwargs)
                return result
            finally:
                duration = time.time() - start
                
                # Perform DB logging
                _log_to_db(func.__name__, duration, log, _tbl_name, db_info, kwargs)
                
                if 'connect_type' in kwargs:
                    print(kwargs['connect_type'])
                print(f"Function {func.__name__} completed successfully")
                print(f"{func.__name__} took {_format_duration(duration)}")

        return wrapper
    return decorator

def _update_excel_log(log_dir, filename, type_of, current_date):
    """Internal helper to write to the excel log file."""
    df_new = pd.DataFrame([[filename, current_date, type_of]],
                          columns=['file_name', 'date', 'type_of'])
    try:
        os.makedirs(os.path.dirname(log_dir), exist_ok=True)
        if not os.path.exists(log_dir):
            df_new.to_excel(log_dir, index=False)
        else:
            df_old = pd.read_excel(log_dir)
            pd.concat([df_new, df_old], ignore_index=True).to_excel(log_dir, index=False)
    except Exception as e:
        logger.error(f"Excel logging failed: {e}")

def check_if_up_to_date(log_dir: str = DEFAULT_LOG_DIR) -> Callable:
    """Decorator to skip execution if already logged for today."""
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            from codeeghtesadi.utils.common import get_update_date
            
            if kwargs.get('log') is False:
                return func(*args, **kwargs)

            func_name = func.__name__
            type_of = 'save_excel' if 'save_excel' in func_name else ('download_excel' if 'download' in func_name else 'save_excel')
            
            filename = args[0] if args else "unknown"
            current_date = int(get_update_date())

            try:
                if os.path.exists(log_dir):
                    df = pd.read_excel(log_dir)
                    if all(col in df.columns for col in ['file_name', 'type_of', 'date']):
                        check_date = df.loc[(df['file_name'] == filename) & (df['type_of'] == type_of), 'date'].max()
                        if not math.isnan(check_date) and int(check_date) == current_date:
                            if 'save_excel' in func_name:
                                return False
                            print(f"{filename} already logged for today. Skipping.")
                            return func(*args, **kwargs)
                
                if 'save_excel' in func_name:
                    print('Opening excel for update...')
                    
                return func(*args, **kwargs)
            except Exception as e:
                logger.error(f"Update check failed: {e}")
                return func(*args, **kwargs)
        return wrapper
    return decorator

def log_it(log_dir: str = DEFAULT_LOG_DIR) -> Callable:
    """Decorator to log activity to Excel."""
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            from codeeghtesadi.utils.common import get_update_date
            
            if kwargs.get('log') is False:
                return func(*args, **kwargs)

            start_time = time.time()
            filename = args[0] if args else "unknown"
            
            if func.__name__ == 'save_excel':
                print(f"Opening {filename} for saving...")

            result = func(*args, **kwargs)
            
            # Log the result if it's a download (which returns the new filename), else use input filename
            log_entry = result if 'download' in func.__name__ else filename
            _update_excel_log(log_dir, log_entry, func.__name__, get_update_date())

            duration = (time.time() - start_time) / 60
            display_name = kwargs.get('type_of_excel', filename) if 'download' in func.__name__ else filename
            print(f"It took {duration:.2f} minutes for {display_name} to be logged.")
            print("*" * 71)
            
            return result
        return wrapper
    return decorator
