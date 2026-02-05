import os
import re
import jdatetime
from datetime import timedelta

def leading_zero(x, sanim_based=False):
    # Check if code eghtesadi is sanim_based
    if sanim_based:
        if len(str(x)) == 12:
            # Pad two zeros at the beginning and return the result
            return f'00{x}'
        elif len(str(x)) == 13:
            # Pad two zeros at the beginning and return the result
            return f'0{x}'

    # Check if the length of the string representation of x is 8
    if len(str(x)) == 8:
        # Pad two zeros at the beginning and return the result
        return f'00{x}'
    # Check if the length of the string representation of x is 9
    elif len(str(x)) == 9:
        # Pad one zero at the beginning and return the result
        return f'0{x}'
    # Check if the length of the string representation of x is 12
    elif len(str(x)) == 12:
        # Return the first 10 characters of the string
        return str(x[:10])
    else:
        # If none of the above conditions are met, return 0 (invalid input)
        return str(x)

def add_one_day(current_date):
    current_date = jdatetime.datetime.strptime(current_date, "%Y%m%d")
    current_date = current_date + jdatetime.timedelta(days=1)
    return current_date.strftime("%Y%m%d")

def maybe_make_dir(directory):
    # Check if a single directory path is provided
    if isinstance(directory, str):
        directory = [directory]  # Convert to a list for uniform processing

    # Iterate through each directory path
    for item in directory:
        # Check if the directory already exists
        if not os.path.exists(item):
            # If not, create the directory and any necessary parent directories
            os.makedirs(item)

def make_dir_if_not_exists(paths):
    # Duplicate logic of maybe_make_dir, kept for compatibility if needed or redirect
    maybe_make_dir(paths)

def get_update_date(date=None, delimiter=None):
    # If no date is provided, use today's date in Jalali calendar
    if date is None:
        x = jdatetime.date.today()

        # Format month with leading zero if it's a single-digit month
        if len(str(x.month)) == 1:
            month = '0%s' % str(x.month)
        else:
            month = str(x.month)

        # Format day with leading zero if it's a single-digit day
        if len(str(x.day)) == 1:
            day = '0%s' % str(x.day)
        else:
            day = str(x.day)

        year = str(x.year)

    # If a Gregorian date is provided, convert it to Jalali calendar
    else:
        gdate = jdatetime.GregorianToJalali(int(date[0]), int(date[1]),
                                            int(date[2]))
        year = str(gdate.jyear)
        month = str(gdate.jmonth)
        day = str(gdate.jday)

        # Format month with leading zero if it's a single-digit month
        if len(str(month)) == 1:
            month = '0%s' % str(month)
        else:
            month = str(month)

        # Format day with leading zero if it's a single-digit day
        if len(str(day)) == 1:
            day = '0%s' % str(day)
        else:
            day = str(day)

    # Check if a delimiter is specified and format the date accordingly
    if delimiter is not None:
        update_date = year + delimiter + month + delimiter + day
    else:
        update_date = year + month + day

    return update_date

def split_list(lst, chunk_size):
    return [lst[i:i + chunk_size] for i in range(0, len(lst), chunk_size)]

def get_filename(item, patterns='[\w-]+?(?=\.)'):
    # Use the re.search function to find the first match of the pattern in the item
    a = re.search(patterns, item)
    # Return the matched portion of the string (file name)
    return a.group()

def is_int(str_val):
    try:
        # Try converting the string to a float
        str_val = float(str_val)

        # Check if the result is a float
        if isinstance(str_val, float):
            # If it's a float, convert it to an integer
            str_val = int(str_val)

        # Return the result (either the integer or the original string)
        return str_val

    except Exception:
        # Return the original string if conversion fails
        return str_val

def georgian_to_persian(row, type_of='g', delimeter='-', complete_date=False):

    none_date = None  # Define the value to return for undefined dates
    # List of placeholders for undefined dates
    lst_none = ['None', 'NaT', 'nan']

    if type_of == 't':
        if row in lst_none:
            return none_date
        else:
            x = int(row[:6])
            return x
    elif type_of == 'g':
        if row in lst_none:
            return none_date
        else:
            lst = row.split(delimeter)
            # Assuming get_update_date can handle list or we need to adapt it. 
            # In helpers.py get_update_date handles list inputs?
            # helpers.py get_update_date signature: def get_update_date(date=None, delimiter=None):
            # It checks if date is None. If not, it assumes date is a list [year, month, day].
            date = get_update_date(lst)

            if complete_date:
                return date

            return int(date[:8])

def is_date(str_val, fuzzy=False):
    from dateutil.parser import parse

    lst_none = ['None', 'NaT', 'nan']

    try:
        # Check if the input string is in the list of known non-date values
        if str_val in lst_none:
            return str_val

        # Extract the first 10 characters (representing the date part) from the input string
        str_val = str_val[:10]

        # Attempt to parse the date using the `parse` function
        parse(str_val, fuzzy=fuzzy)

        # Convert the parsed date to a Persian date using the `georgian_to_persian` function
        str_val = georgian_to_persian(str_val, complete_date=True)

        # Return the Persian date string
        return str_val

    except ValueError:
        # If an error occurs during parsing, return the original input string
        return str_val

def rename_duplicate_columns(df):
    import pandas as pd
    # Create a Series containing the original column names
    cols = pd.Series(df.columns)

    # Iterate through duplicate column names
    for dup in df.columns[df.columns.duplicated(keep=False)]:
        # Update column names with a numerical suffix for duplicates
        cols[df.columns.get_loc(dup)] = ([
            dup + '.' + str(d_idx) if d_idx != 0 else dup
            for d_idx in range(df.columns.get_loc(dup).sum())
        ])

    # Assign the modified column names back to the DataFrame
    df.columns = cols

    return df

def return_start_end(s=0, end=1000000000000000000, inc=10000, alpha_inc=99.1):

    # Initialize an empty list to store tuples
    lst = []

    # Initialize the step with the initial increment
    step = inc

    # Loop until the step exceeds the specified end point
    while step <= end:
        # Append tuple (start, end) to the list
        lst.append((int(s), int(step)))

        # Update starting point and increment values
        s = step + 1
        step += inc
        inc *= alpha_inc

    return lst


def add_days_to_persian_date(date_str, days=1):
    from datetime import datetime, timedelta
    # Convert the string to a Persian date
    persian_date = datetime.strptime(date_str, '%Y/%m/%d')

    # Add one day
    persian_date += timedelta(days=1)

    # Convert the Persian date to a string
    persian_date_str = persian_date.strftime('%Y/%m/%d')
    return persian_date_str

def calculate_days_difference(date1: str, date2: str):
    from persiantools.jdatetime import JalaliDate
    
    if len(date1) < 5:
        return 0

    date1 = date1.replace("/", "-")
    date2 = date2.replace("/", "-")
    
    # helper for isoformat if needed or just use str
    jalali_date1 = JalaliDate.fromisoformat(date1)
    jalali_date2 = JalaliDate.fromisoformat(date2)

    gregorian_date1 = jalali_date1.to_gregorian()
    gregorian_date2 = jalali_date2.to_gregorian()

    day_difference = (gregorian_date2 - gregorian_date1).days
    return abs(day_difference)

def extract_nums(text, expression=r'\d+'):
    import re
    # Extract numbers using regex
    numbers = re.findall(expression, text)
    return numbers


def input_info():
    import argparse
    from codeeghtesadi.constants import (
        get_str_help, get_str_years, get_all_years, get_comm_reports,
        get_comm_years, get_heiat, get_lst_reports, get_years,
        get_common_reports, get_common_years, get_heiat_reports
    )
    from codeeghtesadi.utils.decorators import retry

    # Initialize the argument parser
    parser = argparse.ArgumentParser()

    # Define command-line arguments with their respective help messages
    parser.add_argument('--reportTypes', type=str, help=get_str_help())
    parser.add_argument('--years', type=str, help=get_str_years())
    parser.add_argument('--s',
                        type=str,
                        nargs='?',
                        default='not-s',
                        help='types:\nt = True\nf = False\n')
    parser.add_argument('--d',
                        type=str,
                        nargs='?',
                        default='not-d',
                        help='types:\nt = True\nf = False\n')
    parser.add_argument('--c',
                        type=str,
                        nargs='?',
                        default='not-c',
                        help='types:\nt = True\nf = False\n')

    # Parse the command-line arguments
    args = parser.parse_args()

    # Extract individual arguments from the parsed results
    selected_report_types = args.reportTypes.split(',')  # ['1','2','3','4']
    selected_years = args.years.split(',')

    # Get various data needed for processing
    all_years = get_all_years()
    comm_reports = get_comm_reports()
    comm_years = get_comm_years()
    heiat = get_heiat()

    # Extract arguments related to actions
    dump_to_sql = args.d
    create_reports = args.c
    scrape = args.s

    # Initialize lists to store selected report types and years
    new_reportTypes = []
    new_years = []

    # Retrieve various data lists
    report_types = get_lst_reports()
    years = get_years()
    common_reports = get_common_reports()
    common_years = get_common_years()
    heiat_reports = get_heiat_reports()

    # Process selected report types
    if selected_report_types.count(comm_reports):
        for report_type in common_reports:
            new_reportTypes.append(report_type)

    elif selected_report_types.count(heiat):
        for report_type in heiat_reports:
            new_reportTypes.append(report_type[0])

    else:
        for report_type in report_types:
            if selected_report_types.count(report_type[1]):
                new_reportTypes.append(report_type[0])

    # Process selected years
    if selected_years.count(all_years):
        for year in years:
            new_years.append(year[0])

    elif selected_years.count(comm_years):
        for year in common_years:
            new_years.append(year[0])

    else:
        for year in years:
            if selected_years.count(year[1]):
                new_years.append(year[0])

    # Return the processed data
    return new_reportTypes, new_years, scrape, dump_to_sql, create_reports



