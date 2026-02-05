# import pandas as pd
# from automation.helpers import drop_into_db, list_files

# files = list_files(r'E:\automating_reports_V2\saved_dir\CSV', 'csv')[1:]

# append_to_prev = True
# for file in files:

#     df = pd.read_csv(file)
#     df = df.astype(str)
#     drop_into_db('ranavdV1',
#                  df.columns.tolist(),
#                  df.values.tolist(),
#                  append_to_prev=append_to_prev)

#     append_to_prev = True


import pandas as pd
import multiprocessing as mp
from tqdm import tqdm
from automation.helpers import drop_into_db, list_files


def process_file(file, append_to_prev, chunk_size=100000):
    """Reads a CSV file in chunks and inserts data into the database."""
    for chunk in pd.read_csv(file, chunksize=chunk_size):
        chunk = chunk.astype(str)
        drop_into_db('ranavdV1', chunk.columns.tolist(),
                     chunk.values.tolist(), append_to_prev=append_to_prev)


def process_file_wrapper(args):
    """Wrapper function to unpack arguments and return completion signal."""
    file, append_to_prev, chunk_size = args
    process_file(file, append_to_prev, chunk_size)
    return 1  # Signal completion


if __name__ == "__main__":
    files = list_files(r'E:\automating_reports_V2\saved_dir\CSV', 'csv')[1:]
    append_to_prev = True
    args_list = [(file, append_to_prev, 100000) for file in files]

    with mp.Pool(processes=mp.cpu_count()) as pool:
        # Use imap to process files and track progress with tqdm
        results = pool.imap(process_file_wrapper, args_list)
        for _ in tqdm(results, total=len(args_list)):
            pass
