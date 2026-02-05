import tkinter as tk
from threading import Thread
from arzeshafzoodehsonati import run_it as arzesh
from mashaghelsonati import get_tashkhis_ghatee_sonati as mash
from oracle import run_it as sanim
from soratmoamelat import run_it as soratmoamelat


def btn_soratmoamelat_handler():
    th1 = Thread(target=soratmoamelat)
    th1.start()


def btn_arzesh_handler():
    th1 = Thread(target=arzesh)
    th1.start()


def btn_mash_handler():
    th1 = Thread(target=mash, kwargs={'eblagh': False, 'return_df': False})
    th1.start()


def btn_sanim_handler():
    th1 = Thread(target=sanim)
    th1.start()


window = tk.Tk()
window.geometry("300x400")

lbl = tk.Label(text="Hello, Tkinter")
lbl.pack()

btn_arzesh = tk.Button(
    text="Arzesh Afzoodeh Sonati!",
    width=25,
    height=5,
    bg="blue",
    fg="yellow",
    command=btn_arzesh_handler
)
btn_arzesh.pack()

btn_soratmoamelat = tk.Button(
    text="soratmoamelat!",
    width=25,
    height=5,
    bg="blue",
    fg="yellow",
    command=btn_soratmoamelat_handler
)
btn_soratmoamelat.pack()

btn_mashghel = tk.Button(
    text="Mashaghel Sonati!",
    width=25,
    height=5,
    bg="blue",
    fg="yellow",
    command=btn_mash_handler
)
btn_mashghel.pack()

btn_mashghel = tk.Button(
    text="Sanim!",
    width=25,
    height=5,
    bg="blue",
    fg="yellow",
    command=btn_sanim_handler
)
btn_mashghel.pack()


window.mainloop()
