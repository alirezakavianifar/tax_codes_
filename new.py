import pandas as pd
from automation.helpers import drop_into_db, list_files

files = list_files(r'E:\Ravand\ravand', 'xlsx')

append_to_prev = False
for file in files:

    df = pd.read_excel(file, sheet_name='Sheet2')

    df = df.astype('str')

    drop_into_db('ranavdV2',
                 df.columns.tolist(),
                 df.values.tolist(),
                 append_to_prev=append_to_prev)
