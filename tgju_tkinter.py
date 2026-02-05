from soratmoamelat import run_it as soratmoamelat
from oracle import run_it as sanim
from mashaghelsonati import get_tashkhis_ghatee_sonati as mash
from arzeshafzoodehsonati import run_it as arzesh
from threading import Thread
import customtkinter
import tkinter.messagebox
import tkinter
import multiprocess
import sys
import os
import psutil

IND = 0
ALL_PROCESSES = {}
btn_tgju_text_init = 'دریافت اطلاعات سکه'


def running_tgju():
    global IND
    global ALL_PROCESSES
    global btn_tgju_text_init

    def thread_tgju_start():
        global ALL_PROCESSES
        p1 = multiprocess.Process(
            target=run_tgju, args=(False, False), daemon=True)
        p1.start()
        ALL_PROCESSES['tgju_start'] = p1

    def kill_proc_tree(pid, including_parent=False):
        parent = psutil.Process(pid)
        children = parent.children(recursive=True)
        for child in children:
            child.kill()
        gone, still_alive = psutil.wait_procs(children, timeout=5)
        if including_parent:
            parent.kill()
            parent.wait(5)

    def thread_tgju_kill():

        if 'tgju_start' in ALL_PROCESSES.keys():
            pid = os.getpid()
            kill_proc_tree(pid, False)
            # ALL_PROCESSES['tgju_start'].terminate()
            ALL_PROCESSES.pop('tgju_start', None)

    if IND == 0:
        IND = 1
        thread_tgju_start()
    else:
        thread_tgju_kill()
        IND = 0


# def print_selection():
#     if (var1.get() == 1) & (var2.get() == 0):
#         l.config(text='I love Python ')
#     elif (var1.get() == 0) & (var2.get() == 1):
#         l.config(text='I love C++')
#     elif (var1.get() == 0) & (var2.get() == 0):
#         l.config(text='I do not anything')
#     else:
#         l.config(text='I love both')
# var1 = tkinter.IntVar()
# var2 = tkinter.IntVar()
# Modes: "System" (standard), "Dark", "Light"
customtkinter.set_appearance_mode("System")
# Themes: "blue" (standard), "green", "dark-blue"
customtkinter.set_default_color_theme("blue")


class App(customtkinter.CTk):

    WIDTH = 780
    HEIGHT = 520

    def __init__(self):
        super().__init__()

        self.var1 = tkinter.IntVar()
        self.var2 = tkinter.IntVar()
        self.var3 = tkinter.IntVar()
        self.var4 = tkinter.IntVar()

        # Set btn_tgju text
        btn_tgju_text = customtkinter.StringVar()
        btn_tgju_text.set(btn_tgju_text_init)

        self.title("CustomTkinter complex_example.py")
        self.geometry(f"{App.WIDTH}x{App.HEIGHT}")
        # call .on_closing() when app gets closed
        self.protocol("WM_DELETE_WINDOW", self.on_closing)

        # ============ create two frames ============

        # configure grid layout (2x1)
        self.grid_columnconfigure(1, weight=1)
        self.grid_rowconfigure(0, weight=1)

        self.frame_left = customtkinter.CTkFrame(master=self, width=180)

        self.frame_left.grid(row=0, column=0, sticky="nswe", padx=20, pady=20)

        self.frame_right = customtkinter.CTkFrame(master=self)
        self.frame_right.grid(row=0, column=1, sticky="nswe", padx=20, pady=20)

        # self.frame_middle = customtkinter.CTkFrame(master=self)
        # self.frame_middle.grid(row=0, column=2, sticky="nswe", padx=20, pady=20)

        # ============ frame_left ============

        # configure grid layout (1x11)
        # empty row with minsize as spacing
        self.frame_left.grid_rowconfigure(0, minsize=10)
        self.frame_left.grid_rowconfigure(5, weight=1)  # empty row as spacing
        # empty row with minsize as spacing
        self.frame_left.grid_rowconfigure(8, minsize=20)
        # empty row with minsize as spacing
        self.frame_left.grid_rowconfigure(11, minsize=10)

        self.label_1 = customtkinter.CTkLabel(master=self.frame_left,
                                              text="دریافت اطلاعات",
                                              text_font=('tahoma', 10)
                                              )  # font name and size in px
        self.label_1.grid(row=1, column=0, pady=10, padx=10)

        self.btn_tgju = customtkinter.CTkButton(master=self.frame_left, textvariable=btn_tgju_text,
                                                command=lambda: running_tgju(), text_font=('Tahoma', 10))
        self.btn_tgju.grid(row=2, column=0, pady=10, padx=20)

        self.button_2 = customtkinter.CTkButton(master=self.frame_left,
                                                text="CTkButton",
                                                command=self.button_event)
        self.button_2.grid(row=3, column=0, pady=10, padx=20)

        self.button_3 = customtkinter.CTkButton(master=self.frame_left,
                                                text="CTkButton",
                                                command=self.button_event)
        self.button_3.grid(row=4, column=0, pady=10, padx=20)

        # self.label_mode = customtkinter.CTkLabel(
        #     master=self.frame_left, text="Appearance Mode:")
        # self.label_mode.grid(row=9, column=0, pady=0, padx=20, sticky="w")

        # self.optionmenu_1 = customtkinter.CTkOptionMenu(master=self.frame_left,
        #                                                 values=[
        #                                                     "Light", "Dark", "System"],
        #                                                 command=self.change_appearance_mode)
        # self.optionmenu_1.grid(row=10, column=0, pady=10, padx=20, sticky="w")

        # ============ frame_right ============

        # configure grid layout (3x7)
        self.frame_right.rowconfigure((0, 1, 2, 3), weight=1)
        self.frame_right.rowconfigure(7, weight=10)
        self.frame_right.columnconfigure((0, 1), weight=1)
        self.frame_right.columnconfigure(2, weight=0)

        # self.frame_info = customtkinter.CTkFrame(master=self.frame_right)
        # self.frame_info.grid(row=0, column=0, columnspan=2,
        #                      rowspan=4, pady=20, padx=20, sticky="nsew")

        # ============ frame_info ============

        # configure grid layout (1x1)
        # self.frame_info.rowconfigure(0, weight=1)
        # self.frame_info.columnconfigure(0, weight=1)

        # self.label_info_1 = customtkinter.CTkLabel(master=self.frame_info,
        #                                            text="CTkLabel: Lorem ipsum dolor sit,\n" +
        #                                                 "amet consetetur sadipscing elitr,\n" +
        #                                                 "sed diam nonumy eirmod tempor",
        #                                            height=100,
        #                                            corner_radius=6,  # <- custom corner radius
        #                                            # <- custom tuple-color
        #                                            fg_color=(
        #                                                "white", "gray38"),
        #                                            justify=tkinter.LEFT)
        # self.label_info_1.grid(column=0, row=0, sticky="nwe", padx=15, pady=15)

        # self.progressbar = customtkinter.CTkProgressBar(master=self.frame_info)
        # self.progressbar.grid(row=1, column=0, sticky="ew", padx=15, pady=15)

        # ============ frame_right ============

        # self.radio_var = tkinter.IntVar(value=0)

        # self.label_radio_group = customtkinter.CTkLabel(master=self.frame_right,
        #                                                 text="CTkRadioButton Group:")
        # self.label_radio_group.grid(
        #     row=0, column=2, columnspan=1, pady=20, padx=10, sticky="")

        # self.radio_button_1 = customtkinter.CTkRadioButton(master=self.frame_right,
        #                                                    variable=self.radio_var,
        #                                                    value=0)
        # self.radio_button_1.grid(row=1, column=2, pady=10, padx=20, sticky="n")

        # self.radio_button_2 = customtkinter.CTkRadioButton(master=self.frame_right,
        #                                                    variable=self.radio_var,
        #                                                    value=1)
        # self.radio_button_2.grid(row=2, column=2, pady=10, padx=20, sticky="n")

        # self.radio_button_3 = customtkinter.CTkRadioButton(master=self.frame_right,
        #                                                    variable=self.radio_var,
        #                                                    value=2)
        # self.radio_button_3.grid(row=3, column=2, pady=10, padx=20, sticky="n")

        # self.slider_1 = customtkinter.CTkSlider(master=self.frame_right,
        #                                         from_=0,
        #                                         to=1,
        #                                         number_of_steps=3,
        #                                         command=self.progressbar.set)
        # self.slider_1.grid(row=4, column=0, columnspan=2,
        #                    pady=10, padx=20, sticky="we")

        # self.slider_2 = customtkinter.CTkSlider(master=self.frame_right,
        #                                         command=self.progressbar.set)
        # self.slider_2.grid(row=5, column=0, columnspan=2,
        #                    pady=10, padx=20, sticky="we")

        # self.switch_1 = customtkinter.CTkSwitch(master=self.frame_right,
        #                                         text="CTkSwitch")
        # self.switch_1.grid(row=4, column=2, columnspan=1,
        #                    pady=10, padx=20, sticky="we")

        # self.switch_2 = customtkinter.CTkSwitch(master=self.frame_right,
        #                                         text="CTkSwitch")
        # self.switch_2.grid(row=5, column=2, columnspan=1,
        #                    pady=10, padx=20, sticky="we")

        # self.combobox_1 = customtkinter.CTkComboBox(master=self.frame_right,
        #                                             values=["Value 1", "Value 2"])
        # self.combobox_1.grid(row=6, column=2, columnspan=1,
        #                      pady=10, padx=20, sticky="we")

        self.check_box_1 = customtkinter.CTkCheckBox(master=self.frame_right, variable=self.var1,
                                                     text="CTkCheckBox")
        self.check_box_1.grid(row=1, column=0, pady=10, padx=20, sticky="w")

        self.check_box_2 = customtkinter.CTkCheckBox(master=self.frame_right, variable=self.var2,
                                                     text="CTkCheckBox")
        self.check_box_2.grid(row=2, column=0, pady=10, padx=20, sticky="w")

        self.check_box_3 = customtkinter.CTkCheckBox(master=self.frame_right, variable=self.var3,
                                                     text="CTkCheckBox")
        self.check_box_3.grid(row=3, column=0, pady=10, padx=20, sticky="w")

        self.check_box_4 = customtkinter.CTkCheckBox(master=self.frame_right, variable=self.var4,
                                                     text="CTkCheckBox")
        self.check_box_4.grid(row=4, column=0, pady=10, padx=20, sticky="w")

        # self.entry = customtkinter.CTkEntry(master=self.frame_right,
        #                                     width=120,
        #                                     placeholder_text="CTkEntry")
        # self.entry.grid(row=8, column=0, columnspan=2,
        #                 pady=20, padx=20, sticky="we")

        self.button_5 = customtkinter.CTkButton(master=self.frame_right,
                                                text="CTkButton",
                                                border_width=2,  # <- custom border_width
                                                fg_color=None,  # <- no fg_color
                                                command=self.button_event)
        self.button_5.grid(row=5, column=0,
                           pady=10, padx=20, sticky="we")

        # set default values
        # self.optionmenu_1.set("Dark")
        # self.button_3.configure(state="disabled", text="Disabled CTkButton")
        # self.combobox_1.set("CTkCombobox")
        # self.radio_button_1.select()
        # self.slider_1.set(0.2)
        # self.slider_2.set(0.7)
        # self.progressbar.set(0.5)
        # self.switch_2.select()
        # self.radio_button_3.configure(state=tkinter.DISABLED)
        # self.check_box_1.configure(
        #     state=tkinter.DISABLED, text="CheckBox disabled")
        # self.check_box_2.select()

    def button_event(self):
        if self.var1.get() == 1:
            print('operation 1 executed')
        if self.var2.get() == 1:
            print('operation 2 executed')
        if self.var3.get() == 1:
            print('operation 3 executed')
        if self.var4.get() == 1:
            print('operation 4 executed')

    def change_appearance_mode(self, new_appearance_mode):
        customtkinter.set_appearance_mode(new_appearance_mode)

    def on_closing(self, event=0):
        self.destroy()


if __name__ == "__main__":
    app = App()
    app.mainloop()
