from bs4 import BeautifulSoup
import pandas as pd
from automation.helpers import drop_into_db


# df = pd.read_excel(r'E:\automating_reports_V2\saved_dir\test\file.xlsx')
# df = df.astype('str')
# drop_into_db('tbllisthoghogh',
#              df.columns.tolist(),
#              df.values.tolist(),
#              append_to_prev=False)

# Specify the HTML file path
html_file_path = r'C:\ezhar-temp\1392\badvi_darjarian_dadrasi_hamarz\file1.html'

# Function to process each chunk


def process_chunk(chunk):
    # Process the chunk using Pandas
    # Example: Print the shape of the DataFrame
    print(chunk.shape)


# Open the HTML file and create a BeautifulSoup object
with open(html_file_path, 'r', encoding='utf-8') as file:
    # Create a BeautifulSoup object
    soup = BeautifulSoup(file, 'html.parser')

    # Find all tables in the HTML
    tables = soup.find_all('table')

    # Specify the chunk size
    chunk_size = 200

    # Process each chunk
    for i in range(0, len(tables), chunk_size):
        # Extract a chunk of tables
        chunk_tables = tables[i:i+chunk_size]

        # Convert the chunk of tables to a list of DataFrames
        chunk_dfs = pd.read_html(str(chunk_tables), flavor='bs4')

        # Process each DataFrame in the chunk
        for df in chunk_dfs:
            process_chunk(df)
