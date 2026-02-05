import threading
import ctypes
import time
import sys
import trace


class CustomThread(threading.Thread):
    def __init__(self, group=None, target=None, name=None,
                 args=(), kwargs={}, Verbose=None):
        threading.Thread.__init__(self, group, target, name, args, kwargs)
        self.name = name
        self._return = None
        self.killed = False

    def start(self):
        self.__run_backup = self.run
        self.run = self.__run
        threading.Thread.start(self)

    def run(self):

        # target function of the thread class
        try:
            self._return = self._target(*self._args, **self._kwargs)
            print('running ' + self.name)
        finally:
            print('ended')

    def __run(self):
        # sys.settrace(self.globaltrace)
        self.__run_backup()
        self.run = self.__run_backup

    def globaltrace(self, frame, event, arg):
        if event == 'call':
            return self.localtrace
        else:
            return None

    def localtrace(self, frame, event, arg):
        if self.killed:
            if event == 'line':
                raise SystemExit()
        return self.localtrace

    def kill(self):
        self.killed = True

    def join(self, *args):
        threading.Thread.join(self, *args)
        return self._return


# def hello():
#     try:
#         time.sleep(1)
#         return 'hello'
#     except Exception as e:
#         print('f')


# t1 = CustomThread(target=hello)
# t1.start()
# time.sleep(2)
# t1.kill()
# res = t1.join()
# if not t1.is_alive():
#     print('thread killed')
