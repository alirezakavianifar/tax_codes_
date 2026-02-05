import pandas as pd
from automation.helpers import connect_to_sql

sql_query = "SELECT * FROM tbldadrasi"

df = connect_to_sql(sql_query, read_from_sql=True, return_df=True)

df.drop('ID', axis=1, inplace=True)

df.duplicated().sum()

df[df.duplicated]['شناسه']

# Specify the Excel file path
excel_file_path = r'C:\ezhar-temp\1392\ghatee_eblagh_shode\Excel.xlsx'

# Specify the chunk size (number of rows to read at a time)
chunk_size = 1000

# Create an ExcelFile object
excel_file = pd.ExcelFile(excel_file_path)

# Get the list of sheet names in the Excel file
sheet_names = excel_file.sheet_names

# Process each sheet in chunks
for sheet_name in sheet_names:
    # Create an iterator for reading each sheet in chunks
    sheet_reader = pd.read_excel(excel_file, sheet_name, iterator=True)

    # Read the first chunk
    chunk = sheet_reader.get_chunk(chunk_size)

    # Process each chunk in the sheet
    while not chunk.empty:
        # Perform your operations on each chunk
        process_chunk(chunk)

        # Read the next chunk
        chunk = sheet_reader.get_chunk(chunk_size)
