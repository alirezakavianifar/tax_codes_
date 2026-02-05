import time
import functools
from functools import wraps
from datetime import datetime
import pandas as pd
import math

# Weak imports to avoid circular dependencies
# from codeeghtesadi.utils.database import drop_into_db (will import inside functions)
# from codeeghtesadi.constants import ...

def universal_retry(retries=3, delay=1, backoff=1, exceptions=(Exception,), 
                    driver_based=False, cleanup_func=None, keep_alive=False, 
                    validate_result=None):
    """
    A universal retry decorator that can handle various retry scenarios.
    
    Args:
        retries (int): Maximum number of retries.
        delay (int/float): Initial delay between retries in seconds.
        backoff (int/float): Multiplier for delay after each failure (exponential backoff).
        exceptions (tuple): Tuple of exception types to catch.
        driver_based (bool): If True, expects a 'driver' in args/kwargs or return value to handle.
        cleanup_func (callable): Function to call for cleanup on failure (e.g., driver.quit).
        keep_alive (bool): If True, suppress final exception and return (driver, info) with success=False.
        validate_result (callable): Function that accepts result and returns bool. If False, raises Exception to trigger retry.
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            nonlocal retries, delay
            current_delay = delay
            last_exception = None
            
            # Setup info dict if needed for keep_alive/driver_based logic
            if 'info' in kwargs:
                kwargs['info']['success'] = True
            elif keep_alive:
                 kwargs.setdefault('info', {'success': True})

            for attempt in range(retries):
                try:
                    result = func(*args, **kwargs)
                    if validate_result and not validate_result(result):
                         raise Exception(f"Result validation failed for {func.__name__}")
                    return result
                except exceptions as e:
                    last_exception = e
                    print(f"Attempt {attempt + 1}/{retries} failed for {func.__name__}: {e}")
                    
                    if attempt < retries - 1:
                        time.sleep(current_delay)
                        current_delay *= backoff
                    else:
                        print(f"All {retries} attempts failed for {func.__name__}")

            # Failure handling after all retries
            driver = None
            if driver_based:
                if 'driver' in kwargs:
                    driver = kwargs['driver']
                else:
                    for arg in args:
                         if hasattr(arg, 'quit'): # Simple check for driver-like object
                             driver = arg
                             break
            
            if cleanup_func and driver:
                try:
                    cleanup_func(driver)
                except Exception as ce:
                    print(f"Cleanup failed: {ce}")

            if keep_alive:
                 if 'info' in kwargs:
                     kwargs['info']['success'] = False
                 return driver, kwargs.get('info', {})
            
            raise last_exception
        return wrapper
    return decorator

# Validation helpers
def not_none_validator(result):
    return result is not None

# Alias to maintain backward compatibility if needed, or replace usages
retry_V1 = universal_retry

wrap_it_with_params = lambda num_tries=3, time_out=20, driver_based=False, detrimental=True, clean_up=False, keep_alive=False, record_process_details=False: universal_retry(retries=num_tries, delay=1, driver_based=driver_based, keep_alive=keep_alive, validate_result=not_none_validator) 
wrap_it_with_paramsv1 = lambda num_tries=3, time_out=10, driver_based=False, detrimental=True, clean_up=False, keep_alive=False: universal_retry(retries=num_tries, delay=2, driver_based=driver_based, keep_alive=keep_alive, validate_result=not_none_validator)
retry = lambda func: universal_retry(retries=5)(func)
retryV1 = lambda func: universal_retry(retries=5, driver_based=True)(func) # simplified mapping

def measure_time(func):
    # Use the @wraps decorator to preserve the original function's metadata
    @wraps(func)
    def wrapper(*args, **kwargs):
        # Record the starting time using time.process_time()
        time1 = time.process_time()

        # Print a message indicating the start of the function and the current time
        print(
            f'***********************************************************************\n{func.__name__} started at {datetime.now().strftime("%H:%M:%S")}\n')

        # Call the original function and store its result
        res = func(*args, **kwargs)

        # Record the ending time using time.process_time()
        time2 = time.process_time()

        # Calculate the total time taken for the function execution
        all_time = time2 - time1

        # Print a message indicating the end of the function, the current time, and the time taken
        print(
            f'***********************************************************************\n {func.__name__} finished at {datetime.now().strftime("%H:%M:%S")}, time taken is {all_time}')

        # Return the result of the original function
        return res

    # Return the wrapper function
    return wrapper

@measure_time
def wrap_func(func, *args, **kwargs):
    res = func(*args, **kwargs)
    return res

def log_the_func(log_file_path, *log_args, **log_kwargs):
    def wrapper(func):
        @wraps(func)
        def try_it(*args, **kwargs):
            # Log function start with optional custom message
            if ('field' in kwargs and kwargs['field'] is not None):
                if 'soratmoamelat' in log_kwargs:
                    kwargs['field']('Starting the function %s for "%s" \n' % (
                        func.__name__, kwargs['selected_option_text']))
                else:
                    kwargs['field']('Starting the function %s" \n' % (
                        func.__name__))

            # Check if log_file_path is provided for file logging
            if log_file_path is not None:
                # Log function start to the specified log file
                with open(log_file_path, 'w') as f:
                    f.write('Starting the function %s' % func.__name__)
                    f.write('\n')
                print('Starting the function %s' % func.__name__)

                # Execute the wrapped function and capture the result
                result = func(*args, **kwargs)

                # Check if the result is a tuple and return it
                if isinstance(result, tuple):
                    try:
                        raise Exception
                    except:
                        return result

                # Log function completion with optional custom message
                if 'field' in kwargs and kwargs['field'] is not None:
                    kwargs['field'](
                        'The function %s completed successfully \n' % func.__name__)
                print('The function %s completed successfully' % func.__name__)

                # Log function completion to the specified log file
                with open(log_file_path, 'w') as f:
                    f.write('The function %s completed successfully' %
                            func.__name__)
                    f.write('\n')

            return result
        return try_it
    return wrapper

def time_it(log=False, tbl_name='default', num_runs=12,
            db={'db_name': 'testdbV2',
                'tbl_name': 'tblLog',
                'append_to_prev': False}):

    def wrapper(func):
        @wraps(func)
        def inner_func(*args, **kwargs):
            # Late imports to avoid circular dependency
            from codeeghtesadi.utils.common import get_update_date
            from codeeghtesadi.utils.database import drop_into_db
            
            nonlocal tbl_name
            nonlocal log
            func_name = func.__name__
            nonlocal num_runs

            # Check if num_runs is specified in kwargs, if so, override the default value.
            if 'num_runs' in kwargs:
                num_runs = kwargs['num_runs']

            if log:
                # If logging is enabled, check for additional kwargs to customize the logging.
                if 'table_name' in kwargs:
                    tbl_name = kwargs['table_name']
                else:
                    try:
                        tbl_name = args[2] # Risky assumption about args position
                    except IndexError:
                        tbl_name = 'unknown'

                if 'type_of' in kwargs:
                    type_of = kwargs['type_of']
                else:
                    type_of = 'nan'

            start = time.time()

            def tryit():
                nonlocal num_runs
                try:
                    # Attempt to execute the decorated function.
                    result = func(*args, **kwargs)

                    # Check if the result is a tuple, and if so, raise an exception.
                    if isinstance(result, tuple):
                        kwargs['index'] = result[0]
                        raise Exception

                    return result

                except Exception as e:
                    print(e)
                    num_runs -= 1
                    x = num_runs > 0

                    # If there are more runs remaining, recursively retry the function.
                    if x:
                        tryit(*args, **kwargs)
                    else:
                        raise Exception

            # Execute the decorated function and capture the results.
            final_results = tryit()

            # If 'only_schema' is specified and True, do not log the operation.
            if 'only_schema' in kwargs:
                if kwargs['only_schema']:
                    log = False

            # Log the operation into the specified database table.
            if log:
                df_all = pd.DataFrame({
                    'table_name': [tbl_name],
                    'update_date': [get_update_date()],
                    'type_of': [type_of]
                })

                drop_into_db(table_name=db['tbl_name'],
                             columns=df_all.columns.tolist(),
                             values=df_all.values.tolist(),
                             append_to_prev=db['append_to_prev'],
                             db_name=db['db_name'])

            end = time.time()

            try:
                # Print information about the completed function.
                if 'connect_type' in kwargs:
                    print(kwargs['connect_type'])
                print('The function %s completed successfully' % func_name)
                time_taken = str('%.2f' % (end-start))
                print(func.__name__ + ' took ' + time_taken +
                      ' seconds')

                # Return the final results, if any.
                if final_results is not None:
                    return final_results
                return

            except Exception as e:
                print(e)
                return

        return inner_func

    return wrapper


def check_if_up_to_date(func):
    @wraps(func)
    def try_it(*args, **kwargs):
        from codeeghtesadi.utils.common import get_update_date
        # check_if_up_to_date uses connect_to_sql but code seemed to assign df = connect_to_sql then df = pd.read_excel(log_dir)
        # The original code had:
        # df = connect_to_sql
        # df = pd.read_excel(log_dir)
        # This meant connect_to_sql was assigned but immediately overwritten!
        # So it actually uses read_excel from log_dir variable?
        # log_dir variable was global. I need to make sure I have it.
        # Let's assume log_dir is passed or we need to import it.
        # For now, I'll assume log_dir is available or I need to handle it.
        
        # log_dir was defined as 'C:\ezhar-temp\excel.xlsx'
        log_folder_name = 'C:\\ezhar-temp'
        log_excel_name = 'excel.xlsx'
        log_dir = os.path.join(log_folder_name, log_excel_name)

        if 'log' in kwargs:
            if kwargs['log'] == False:
                result = func(*args, **kwargs)
                return result
        current_date = int(get_update_date())
        func_name = func.__name__
        if func_name == 'is_updated_to_save':
            type_of = 'save_excel'
        elif func_name == 'is_updated_to_download':
            type_of = 'download_excel'
        else:
            type_of = 'save_excel'

        try:
            df = pd.read_excel(log_dir)
            check_date = df['date'].where((df['file_name'] == args[0])
                                          & (df['type_of'] == type_of)).max()

            if not math.isnan(check_date):

                if (int(check_date) == int(current_date)
                        and func_name != 'save_excel'):
                    print('The %s have already been logged' % args[0])
                    result = func(*args, **kwargs)
                    return result

                elif (int(check_date) != int(current_date)
                    and func_name == 'save_excel'):
                    print('opening excel')
                    result = func(*args, **kwargs)
                    return result
                else:
                    return False

            elif (math.isnan(check_date) and func_name == 'is_updated_to_save'):
                return False

            elif (math.isnan(check_date)
                and func_name == 'is_updated_to_download'):
                return False

            else:
                result = func(*args, **kwargs)
                return result
        except FileNotFoundError:
             # If log file doesn't exist, just run function?
             return func(*args, **kwargs)

    return try_it

def check_update(func):
    # Decorator to check if a database table has been updated before executing a function

    @wraps(func)
    def inner_wrapper(*args, **kwargs):
        # Function to check and handle updates before executing the decorated function
        from codeeghtesadi.sql_queries import get_tblupdateDate
        from codeeghtesadi.utils.database import connect_to_sql
        from codeeghtesadi.constants import get_sql_con
        from codeeghtesadi.utils.common import get_update_date

        # Get the last update date from the table
        sql_query = get_tblupdateDate(kwargs['table_name'])
        date = connect_to_sql(
            sql_query, sql_con=get_sql_con(database='testdbV2'), read_from_sql=True, return_df=True)

        # Compare the last update date with the current update date
        if date.iloc[0][0] == get_update_date():
            # If the table has already been updated, print a message and return None
            result = None
            print('Table already updated. Skipping function execution.')
        else:
            # If the table has not been updated, proceed with executing the decorated function
            result = func(*args, **kwargs)

        return result

    return inner_wrapper

def log_it(func):

    @wraps(func)
    def try_it(*args, **kwargs):
        from codeeghtesadi.utils.common import get_update_date
        from codeeghtesadi.utils.excel_ops import remove_excel_files # Circular dependency potential
        
        log_folder_name = 'C:\\ezhar-temp'
        log_excel_name = 'excel.xlsx'
        log_dir = os.path.join(log_folder_name, log_excel_name)
        
        if 'log' in kwargs:
            if kwargs['log'] == False:
                result = func(*args, **kwargs)
                return result
        print('log_it initialized')
        d1 = datetime.now()
        type_of = func.__name__

        if (func.__name__ == 'save_excel'):
            print('opening %s for saving' % args[0])

        result = func(*args, **kwargs)

        c_date = get_update_date()

        if type_of == 'download_excel':
            df_1 = pd.DataFrame([[result, c_date, type_of]],
                                columns=['file_name', 'date', 'type_of'])
        else:
            df_1 = pd.DataFrame([[args[0], c_date, type_of]],
                                columns=['file_name', 'date', 'type_of'])

        # create excel file for logging if it does not already exist
        if not os.path.exists(log_dir):
            df_1.to_excel(log_dir)
        else:
            df_2 = pd.read_excel(log_dir, index_col=0)
            df_3 = pd.concat([df_1, df_2])
            
            # Using os.remove instead of importing remove_excel_files to avoid heavy imports
            if os.path.exists(log_dir):
                os.remove(log_dir)

            df_3.to_excel(log_dir)

        d2 = datetime.now()
        d3 = (d2 - d1).total_seconds() / 60

        if type_of == 'download_excel':
             print('it took %s minutes for the %s to be logged' %
                   ("%.2f" % d3, kwargs['type_of_excel']))
        else:
             print('it took %s minutes for the %s to be logged' %
                   ("%.2f" % d3, args[0]))
        print(
            '***********************************************************************\n'
        )
        return result
    return try_it
