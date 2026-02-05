import sys
import time
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from automation.helpers import log_the_func


is_downloaded = False


def dec_one_sec(timeout):
    start = time.process_time()
    end = time.process_time()
    while (end - start) < 1:
        end = time.process_time()
    start = end
    timeout -= 1
    return timeout


class Handler(FileSystemEventHandler):

    def on_created(self, event):
        global is_downloaded
        is_downloaded = True
        print('file was created')


@log_the_func('none')
def watch_over(path=r'E:\automating_reports_V2\saved_dir\arzeshafzoodeh_sonati', timeout=120, showlog_every=120, *args, **kwargs):
    global is_downloaded

    event_handler = Handler()
    observer = Observer()
    observer.schedule(event_handler, path, recursive=True)
    observer.start()
    try:
        while True:
            if is_downloaded == True:
                raise Exception

            # time.sleep(1)

            while (timeout > 0 and is_downloaded == False):
                if (('field' in kwargs and kwargs['field'] is not None) and (timeout % showlog_every == 0)):
                    kwargs['field'](
                        'waiting...\nremaining time before time out = %s\n' % str(timeout))
                print('waiting')
                timeout = dec_one_sec(timeout)

            if timeout == 0:
                return True

            is_downloaded = True
            raise Exception

    except Exception as e:
        print(e)
        is_downloaded = False

    finally:
        observer.stop()
        observer.join()
        return False
