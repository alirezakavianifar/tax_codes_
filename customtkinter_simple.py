import customtkinter
from tkinter_helpers import btn_handler
import tkinter
import zope.interface
from automation.design_patterns.observer.observer_pattern import IObserver
from oracle import run_it as sanim
from mashaghelsonati import get_tashkhis_ghatee_sonati as mash
from arzeshafzoodehsonati import run_it as arzesh
from soratmoamelat import run_it as soratmoamelat
from automation.codeeghtesadi import run_it as codeghtesadi
import atexit
import psutil
import os
import time


@zope.interface.implementer(IObserver)
class App(customtkinter.CTk):
    def __init__(self, data_source=None):
        super().__init__()
        self.data_source = data_source
        self.geometry("800x600")
        self.title("CTk example")
        self.kill = True
        self.pid = os.getpid()

        # add widgets to app
        self.button_1 = customtkinter.CTkButton(
            self, command=lambda: btn_th(self.update_soratmoamelat),
            text='صورت معاملات',
            font=customtkinter.CTkFont('Tahoma', size=15, weight="bold"))
        self.button_1.grid(row=0, column=0, padx=20, pady=10)

        self.button_2 = customtkinter.CTkButton(
            self, command=lambda: btn_th(self.update_btn2),
            text='ارزش افزوده',
            font=customtkinter.CTkFont('Tahoma', size=15, weight="bold"))
        self.button_2.grid(row=1, column=0, padx=20, pady=10)

        self.button_3 = customtkinter.CTkButton(
            self,  command=lambda: btn_th(self.update_mashaghel),
            text='مشاغل',
            font=customtkinter.CTkFont('Tahoma', size=15, weight="bold"))
        self.button_3.grid(row=2, column=0, padx=20, pady=10)

        self.button_4 = customtkinter.CTkButton(
            self, command=lambda: btn_th(self.update_btn4),
            text='سنیم',
            font=customtkinter.CTkFont('Tahoma', size=15, weight="bold"))
        self.button_4.grid(row=3, column=0, padx=20, pady=10)

        self.button_5 = customtkinter.CTkButton(
            self, command=lambda: btn_th(
                lambda: self.update_dadrasi(btn=self.button_5)),
            text='دادرسی',
            font=customtkinter.CTkFont('Tahoma', size=15, weight="bold"))
        self.button_5.grid(row=4, column=0, padx=20, pady=10)

        self.textbox_1 = customtkinter.CTkTextbox(self, width=500, height=100)
        self.textbox_1.grid(row=0, column=1, padx=(
            20, 0), pady=(20, 0), sticky="nsew")
        self.textbox_2 = customtkinter.CTkTextbox(self, width=500, height=100)
        self.textbox_2.grid(row=1, column=1, padx=(
            20, 0), pady=(20, 0), sticky="nsew")
        self.textbox_3 = customtkinter.CTkTextbox(self, width=500, height=100)
        self.textbox_3.grid(row=2, column=1, padx=(
            20, 0), pady=(20, 0), sticky="nsew")
        self.textbox_4 = customtkinter.CTkTextbox(self, width=500, height=100)
        self.textbox_4.grid(row=3, column=1, padx=(
            20, 0), pady=(20, 0), sticky="nsew")
        self.textbox_5 = customtkinter.CTkTextbox(self, width=500, height=100)
        self.textbox_5.grid(row=4, column=1, padx=(
            20, 0), pady=(20, 0), sticky="nsew")

        # Set default values
        self.textbox_1.insert("0.0", self.data_source.get_txt1_val())
        self.textbox_2.insert("0.0", self.data_source.get_txt2_val())
        self.textbox_3.insert("0.0", self.data_source.get_txt3_val())
        self.textbox_4.insert("0.0", self.data_source.get_txt4_val())
        self.textbox_5.insert("0.0", self.data_source.get_txt5_val())

        # Register the cleanup function to be called on exit
        if self.kill:
            atexit.register(self.cleanup)
            # Bind the function to the window close event
            self.protocol("WM_DELETE_WINDOW", self.on_close)

    def cleanup(self):
        # Terminate all subprocesses when the application is closed
        # Note: This is a simple example; you may need to adapt it based on your specific subprocess handling
        print('cleaning up')
        procs = self.get_child_processes(self.pid)
        print(procs)
        for proc in psutil.process_iter(['pid', 'cmdline']):
            if proc.pid in procs:
                proc.terminate()

    def on_close(self):
        # Display a warning pop-up window
        result = tkinter.messagebox.askokcancel(
            "Warning", "Do you really want to close the application?\nAll child processes will be terminated.")
        if result:
            self.cleanup()
            self.destroy()

    def get_child_processes(self, parent_pid):
        child_processes = []

        try:
            process = psutil.Process(parent_pid)
            for child in process.children(recursive=True):
                child_processes.append(child.pid)
        except psutil.NoSuchProcess:
            pass

        return child_processes

    def get_subprocesses(self):
        # Return a list of subprocesses started by your application
        # Note: This is a simple example; you may need to adapt it based on how you track subprocesses
        processes = []
        for proc in psutil.process_iter(['pid', 'cmdline']):
            print(proc.pid)
            if 'your_command_here' in proc.info['cmdline']:
                processes.append(proc)
        return processes

    def update(self, field, fieldname):
        # self.value = self.data_source.get_value()
        if fieldname == '1':
            field.insert("end", self.data_source.get_txt1_val())

        if fieldname == '2':
            field.insert("end", self.data_source.get_txt2_val())

        if fieldname == '3':
            field.insert("end", self.data_source.get_txt3_val())

        if fieldname == '4':
            field.insert("end", self.data_source.get_txt4_val())

        if fieldname == '5':
            field.insert("end", self.data_source.get_txt5_val())

    # add methods to app
    def update_soratmoamelat(self):
        self.textbox_1.delete('1.0', 'end')
        # btn_handler(
        #     soratmoamelat, 'soratmoamelat', field=self.data_source.set_txt1_val)
        soratmoamelat(field=self.data_source.set_txt1_val)

    def update_btn2(self):
        self.textbox_2.delete('1.0', 'end')
        arzesh(field=self.data_source.set_txt2_val)

    def update_mashaghel(self):
        self.textbox_3.delete('1.0', 'end')
        mash(field=self.data_source.set_txt3_val)

    def update_btn4(self):
        self.textbox_4.delete('1.0', 'end')
        sanim(field=self.data_source.set_txt4_val)

    def update_dadrasi(self, btn, *args, **kwargs):
        btn.configure(text="...در حال دریافت", state='disabled')
        self.textbox_5.delete('1.0', 'end')
        codeghtesadi(field=self.data_source.set_txt5_val, get_dadrasi=True)
        btn.configure(text="دادرسی", state='normal')


def btn_th(task):
    import threading
    t1 = threading.Thread(target=lambda: task())
    t1.start()
    x = 12


if __name__ == "__main__":
    from automation.design_patterns.observer.observer_pattern import DataSource
    data_source = DataSource()
    app = App(data_source)
    data_source.add_observer(app)

    app.mainloop()
