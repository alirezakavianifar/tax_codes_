import functools as ft
import os
import pandas as pd
from sanim import get_tashkhis_ghatee_sanim, get_badvi_sanim
from mashaghel_sonati import get_tashkhis_ghatee_sonati
from helpers import maybe_make_dir, insert_into_tbl, connect_to_sql, final_most, rename_duplicate_columns, drop_into_db, connect_to_sql, is_int, get_update_date
from scrape import Scrape
from constants import get_sql_con
from sql_queries import get_sql_residegi99, get_sql_noresidegi_arzeshafoodehSanim
import tkinter as tk
from tkinter import filedialog, Frame, ttk, Menu
import threading

FILE_PATH = ''

project_root = os.path.dirname(os.path.dirname(__file__))

path_codeghtesadi = os.path.join(project_root, r'saved_dir\codeghtesadi')
maybe_make_dir([path_codeghtesadi])


def upload_action(tree):
    global FILE_PATH
    filename = filedialog.askopenfilename(title="Open a File", filetype=(
        ('xlsx files', ".*xlsx"), ("All Files", '*.')))
    # print('selected: ', filename.name)
    if filename:
        try:
            filename = r"{}".format(filename)
            df = pd.read_excel(filename)
        except ValueError:
            print('ERROR')
        clear_treeview(tree)

        tree['column'] = list(df.columns)
        tree['show'] = 'headings'

        for col in tree['column']:
            tree.heading(col, text=col)

        df_rows = df.to_numpy().tolist()
        for row in df_rows:
            tree.insert("", "end", values=row)
        tree.pack()

        thread_startadam(filename)
    # FILE_PATH = filename.name


def clear_treeview(tree):
    tree.delete(*tree.get_children())


def thread_startadam(filename):
    print('starting')
    t1 = threading.Thread(target=start_adam, args=(filename))
    t1.start()


def start_adam(filename):
    x = Scrape()
    df = pd.read_excel(
        filename)
    x.scrape_codeghtesadi(path=path_codeghtesadi, return_df=False, codeeghtesadi={
        'getdata': False, 'adam': True, 'df_toadam': df})


def test():
    import time
    time.sleep(6)
    print('finished')


if __name__ == '__main__':

    root = tk.Tk()
    root.geometry("700x350")
    style = ttk.Style()
    style.theme_use('clam')
    # Create a frame
    frame = Frame(root)
    frame.pack(pady=20)
    tree = ttk.Treeview(frame)
    # Add a menu
    m = Menu(root)
    root.config(menu=m)
    # Add a menu dropdown
    file_menu = Menu(m, tearoff=False)
    m.add_cascade(label='Menu', menu=file_menu)
    file_menu.add_command(label='Open Spreadsheet',
                          command=lambda: upload_action(tree))
    # file_menu.add_command(label='Start Scraping',
    #                       command=lambda: thread_startadam())
    # message = tk.Label(root, text="Hello world")
    # message.pack()
    # button = tk.Button(root, text='Open', command=upload_action)
    # btn_exec = tk.Button(root, text='Execute', command=thread_startadam)
    # button.pack()
    # btn_exec.pack()
    root.mainloop()
