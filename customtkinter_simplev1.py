import customtkinter
from tkinter_helpers import btn_handler
import tkinter
import zope.interface
from automation.design_patterns.observer.observer_pattern import IObserver
from oracle import run_it as sanim
from mashaghelsonati import get_tashkhis_ghatee_sonati as mash
from arzeshafzoodehsonati import run_it as arzesh
from soratmoamelat import run_it as soratmoamelat
from automation.codeeghtesadi import code_eghtesadi as codeghtesadi
from automation.helpers import get_child_processes
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

        self.btndadrasi = customtkinter.CTkButton(
            self, command=lambda: btn_th(
                lambda: self.update_dadrasi(btn=self.btndadrasi)),
            text='دادرسی',
            font=customtkinter.CTkFont('Tahoma', size=15, weight="bold"))
        self.btndadrasi.grid(row=4, column=0, padx=20, pady=10)

        self.btnarzesh = customtkinter.CTkButton(
            self, command=lambda: btn_th(
                lambda: self.get_sabtenamarzshafzoode(btn=self.btnarzesh, get_sabtenamCodeEghtesadiData=True,
                                                      down_url="https://reports.tax.gov.ir/Reports/RegistrationReports/AllTaxpayerDataWithVatInfo")),
            text='ثبت نام ارزش افزوده',
            font=customtkinter.CTkFont('Tahoma', size=12, weight="bold"))
        self.btnarzesh.grid(row=5, column=0, padx=20, pady=10)

        self.sabtenamcodeeghtesadi = customtkinter.CTkButton(
            self, command=lambda: btn_th(
                lambda: self.get_sabtenamcodeeghtesadi(btn=self.sabtenamcodeeghtesadi, get_sabtenamCodeEghtesadiData=True,
                                                       down_url="https://reports.tax.gov.ir/Reports/RegistrationReports/FullTaxpayerInformationInExcel")),
            text='ثبت نام کد اقتصادی',
            font=customtkinter.CTkFont('Tahoma', size=12, weight="bold"))
        self.sabtenamcodeeghtesadi.grid(row=6, column=0, padx=20, pady=10)

        # Register the cleanup function to be called on exit
        if self.kill:
            atexit.register(lambda: cleanup(pid))
            # Bind the function to the window close event
            self.protocol("WM_DELETE_WINDOW", self.on_close)

    def on_close(self):
        # Display a warning pop-up window
        result = tkinter.messagebox.askokcancel(
            "Warning", "Do you really want to close the application?\nAll child processes will be terminated.")
        if result:
            cleanup(self.pid)
            self.destroy()

    def get_subprocesses(self):
        # Return a list of subprocesses started by your application
        # Note: This is a simple example; you may need to adapt it based on how you track subprocesses
        processes = []
        for proc in psutil.process_iter(['pid', 'cmdline']):
            print(proc.pid)
            if 'your_command_here' in proc.info['cmdline']:
                processes.append(proc)
        return processes

    # add methods to app
    def update_soratmoamelat(self):
        soratmoamelat()

    def update_btn2(self):
        arzesh()

    def update_mashaghel(self):
        mash()

    def update_btn4(self):
        sanim()

    def get_sabtenamarzshafzoode(self, btn, get_sabtenamCodeEghtesadiData, down_url):
        btn.configure(text="...در حال دریافت ارزش افزوده", state='disabled')
        codeghtesadi(get_sabtenamCodeEghtesadiData=get_sabtenamCodeEghtesadiData,
                     down_url=down_url)

    def get_sabtenamcodeeghtesadi(self, btn, get_sabtenamCodeEghtesadiData, down_url):
        btn.configure(text="...در حال دریافت کد اقتصادی", state='disabled')
        codeghtesadi(get_sabtenamCodeEghtesadiData=get_sabtenamCodeEghtesadiData,
                     down_url=down_url)

    def update_dadrasi(self, btn, *args, **kwargs):
        btn.configure(text="...در حال دریافت دادرسی", state='disabled')
        codeghtesadi(get_dadrasi=True)
        btn.configure(text="دادرسی", state='normal')

    # def get_sabtenamarzeshafzoodeh(self, btn, *args, **kwargs):
    #     btn.configure(text="...در حال دریافت", state='disabled')
    #     codeghtesadi(get_sabtenamCodeEghtesadiData=True,
    #                  down_url="https://reports.tax.gov.ir/Reports/RegistrationReports/AllTaxpayerDataWithVatInfo")
    #     btn.configure(text="ثبت نام های ارزش افزوده", state='normal')


def btn_th(task):
    import threading
    t1 = threading.Thread(target=lambda: task())
    t1.start()


if __name__ == "__main__":
    from automation.design_patterns.observer.observer_pattern import DataSource
    data_source = DataSource()
    app = App(data_source)
    data_source.add_observer(app)

    app.mainloop()
