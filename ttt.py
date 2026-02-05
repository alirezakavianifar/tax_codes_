# Python program using
# traces to kill threads

import sys
import trace
import threading
import time


class thread_with_trace(threading.Thread):
    def __init__(self, *args, **keywords):
        threading.Thread.__init__(self, *args, **keywords)
        self.killed = False

    def start(self):
        self.__run_backup = self.run
        self.run = self.__run
        threading.Thread.start(self)

    def __run(self):
        sys.settrace(self.globaltrace)
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


def func():
    while True:
        try:
            print('thread running')
        except:
            print('continue')


t1 = thread_with_trace(target=func)
t1.start()
t1.join(1)
t1.kill()
t1.join()
if not t1.is_alive():

    print('thread killed')


def hello():
    # time.sleep(1)
    try:
        print('hello')
        time.sleep(3)
    except:
        time.sleep(4)
        print('finished')


# t1 = CustomThread(target=hello)
# t1.start()
# t1.join(1)
# t1.raise_exception()

# t1.join(1)
# t1.raise_exception()

# print('done')
